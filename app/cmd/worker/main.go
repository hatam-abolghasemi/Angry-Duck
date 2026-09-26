// Command angryduck-worker runs on every node (as a DaemonSet pod). It
//
//   - reports disk utilization, local images and its layer inventory to
//     the controller;
//   - pulls images from the registry when the controller makes it a seed;
//   - ships images to and from peer workers (rescue and propagation),
//     blobs first, snapshots only as a last resort;
//   - runs a pull-only registry mirror for its own containerd, answering
//     from this node or a peer before the registry;
//   - cleans up images nothing has run here for a while, when the disk is
//     getting full.
//
// It carries no container CLIs of its own: crictl/ctr/docker are the
// node's binaries, run chrooted into HOST_ROOT (see worker.HostExec).
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"angryduck/internal/config"
	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
	"angryduck/internal/registryauth"
	"angryduck/internal/sharedtoken"
	"angryduck/internal/worker"
)

func main() {
	envFile := config.String("ENV_FILE", ".env")
	if err := config.Load(envFile); err != nil {
		log.Printf("angryduck-worker: warning: failed to load %s: %v", envFile, err)
	}
	logging.SetLevel(logging.ParseLevel(config.String("LOG_LEVEL", "info")))
	memlimit.Apply("worker")

	nodeID := config.String("NODE_ID", "")
	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("angryduck-worker: NODE_ID not set and hostname lookup failed: %v", err)
		}
		nodeID = hostname
	}
	selfAddress := config.String("SELF_ADDRESS", "")
	if selfAddress == "" {
		log.Fatalf("angryduck-worker: SELF_ADDRESS must be set to a controller-reachable host or host:port (e.g. pod IP via downward API)")
	}
	listenAddr := config.String("WORKER_LISTEN_ADDR", ":18081")
	if !strings.Contains(selfAddress, ":") {
		selfAddress = selfAddress + ":" + listenPort(listenAddr)
	}
	metricsURL := config.String("NODE_EXPORTER_URL", "http://localhost:9100/metrics")
	controllerURL := config.String("CONTROLLER_URL", "http://angryduck-controller:8080")
	reportInterval := config.Duration("REPORT_INTERVAL_S", 15)
	labelRegistry := config.Bool("METRICS_LABEL_REGISTRY", false)
	preheatAttributionInterval := config.Duration("PREHEAT_ATTRIBUTION_INTERVAL_S", 300)
	preheatAttributionRetention := config.Duration("PREHEAT_ATTRIBUTION_RETENTION_S", 360)
	// crictl lists every running container's image in one call; ctr
	// needs one subprocess per container.
	runtimeKind := config.String("CONTAINER_RUNTIME", "crictl")
	runtimeEndpoint := config.String("CONTAINER_RUNTIME_ENDPOINT", "unix:///run/containerd/containerd.sock")
	credsPath := config.String("REGISTRY_CREDENTIALS_PATH", "")
	creds, err := registryauth.Load(credsPath)
	if err != nil {
		log.Fatalf("angryduck-worker: failed to load registry credentials from %s: %v", credsPath, err)
	}
	if credsPath != "" {
		log.Printf("angryduck-worker: loaded credentials for %d registr(y/ies) from %s", creds.Count(), credsPath)
	}
	// The node's filesystem as seen from here: /proc/1/root with hostPID.
	hostRoot := config.String("HOST_ROOT", "/proc/1/root")
	stateDir := config.String("RESCUE_STATE_DIR", "/var/lib/angryduck")
	namespace := config.String("RESCUE_CONTAINERD_NAMESPACE", "k8s.io")

	log.Printf("angryduck-worker[%s]: starting: listen=%s self=%s metrics=%s controller=%s report_interval=%s runtime=%s runtime_endpoint=%s host_root=%s",
		nodeID, listenAddr, selfAddress, metricsURL, controllerURL, reportInterval, runtimeKind, runtimeEndpoint, hostRoot)

	hx, err := worker.NewHostExec(hostRoot)
	if err != nil {
		log.Fatalf("angryduck-worker[%s]: %v", nodeID, err)
	}
	bin := worker.RuntimeBinary(runtimeKind)
	binPath, err := hx.Resolve(bin)
	if err != nil {
		log.Fatalf("angryduck-worker[%s]: CONTAINER_RUNTIME=%s needs %s on the node: %v", nodeID, runtimeKind, bin, err)
	}
	log.Printf("angryduck-worker[%s]: using node binary %s", nodeID, binPath)

	rt := worker.NewRuntime(runtimeKind, creds, runtimeEndpoint, hx)
	inv := worker.NewInventory(rt)
	puller := worker.NewPuller(rt, nodeID, labelRegistry)
	reporter := worker.NewReporter(nodeID, selfAddress, metricsURL, controllerURL, reportInterval, inv)
	puller.OnSuccess(reporter.Kick)

	// One containerd store shared by everything below, so its short
	// listing cache is shared too.
	var store *worker.CtrStore
	if strings.EqualFold(runtimeKind, "docker") {
		log.Printf("angryduck-worker[%s]: CONTAINER_RUNTIME=docker: layer inventory, rescue, mirror and image cleanup need containerd and are off", nodeID)
	} else if _, err := hx.Resolve("ctr"); err != nil {
		log.Printf("angryduck-worker[%s]: no ctr on the node, so layer inventory, rescue, mirror and image cleanup are off: %v", nodeID, err)
	} else {
		store = worker.NewCtrStore(hx, namespace, runtimeEndpoint)
		if dir := store.UseBlobDir(config.String("CONTAINERD_ROOT", "")); dir != "" {
			log.Printf("angryduck-worker[%s]: reading blobs directly from %s", nodeID, dir)
		} else {
			log.Printf("angryduck-worker[%s]: containerd's content store not found (set CONTAINERD_ROOT if it isn't under /var/lib/containerd): serving blobs through ctr", nodeID)
		}
	}
	token, tokenErr := sharedtoken.Load(config.String("RESCUE_TOKEN_PATH", "/etc/angryduck/rescue-token/token"))
	if tokenErr != nil {
		log.Printf("angryduck-worker[%s]: WARNING: no usable token (%v): rescue, propagation and the mirror's peer lookups are off", nodeID, tokenErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if store != nil && config.Bool("LAYER_INVENTORY_ENABLED", true) {
		interval := config.Duration("LAYER_SCAN_INTERVAL_S", 60)
		log.Printf("angryduck-worker[%s]: layer inventory enabled: scan_interval=%s", nodeID, interval)
		reporter.SetLayerTracker(worker.NewLayerTracker(store, nodeID, interval))
	}
	go reporter.Run(ctx)
	if preheatAttributionInterval > 0 {
		go worker.NewPreheatMonitor(rt, puller, preheatAttributionInterval, preheatAttributionRetention, nodeID).Run(ctx)
	}

	var rescue *worker.Rescue
	if store != nil && tokenErr == nil && config.Bool("RESCUE_ENABLED", true) {
		rescue = newRescue(ctx, nodeID, hostRoot, stateDir, store, token)
		rescue.OnSuccess(reporter.Kick)
	}
	var extra []func(*http.ServeMux)

	if store != nil && config.Bool("GC_ENABLED", true) {
		gc := worker.NewImageGC(store,
			rt.ListRunningImages,
			func() (float64, error) {
				r, err := worker.FetchRootUtilization(metricsURL, 5*time.Second)
				return r.Utilization, err
			},
			func(names []string) bool {
				for _, n := range names {
					if puller.Pulling(n) {
						return true
					}
				}
				return rescue != nil && rescue.Busy(names)
			},
			nodeID,
			worker.GCConfig{
				Interval:          config.Duration("GC_INTERVAL_S", 60),
				High:              config.Float("GC_HIGH_UTILIZATION", 0.70),
				Low:               config.Float("GC_LOW_UTILIZATION", 0.60),
				UnusedFor:         config.Duration("GC_UNUSED_FOR_S", 21600),
				RollbackKeep:      config.Int("GC_ROLLBACK_KEEP", 3),
				Batch:             config.Int("GC_BATCH", 5),
				Settle:            config.Duration("GC_SETTLE_S", 10),
				ProtectSubstrings: config.StringSlice("GC_PROTECT_SUBSTRINGS", []string{"pause"}),
				StatePath:         filepath.Join(hostRoot, stateDir, "image-usage.json"),
			})
		gc.OnDone(reporter.Kick)
		go gc.Run(ctx)
	} else if store != nil {
		log.Printf("angryduck-worker[%s]: image cleanup disabled (GC_ENABLED=false): kubelet's own image GC is all there is", nodeID)
	}

	if store != nil {
		mirrorOn := config.Bool("MIRROR_ENABLED", false) && tokenErr == nil
		hosts := &worker.HostsConfig{
			HostRoot:  hostRoot,
			ConfigDir: config.String("MIRROR_CONTAINERD_CONFIG_DIR", "/etc/containerd/certs.d"),
			Mirror:    config.String("MIRROR_LISTEN_ADDR", "127.0.0.1:18082"),
			Extra:     config.StringSlice("MIRROR_REGISTRIES", nil),
			NodeID:    nodeID,
		}
		if mirrorOn {
			m := worker.NewMirror(store, nodeID, token, controllerURL)
			extra = append(extra, m.RegisterPeer)
			go func() {
				srv := &http.Server{Addr: hosts.Mirror, Handler: m.Handler(), ReadHeaderTimeout: 10 * time.Second}
				log.Printf("angryduck-worker[%s]: mirror listening on %s", nodeID, hosts.Mirror)
				if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Printf("angryduck-worker[%s]: WARNING: mirror stopped: %v", nodeID, err)
				}
			}()
		}
		// Keep containerd's hosts.toml in step with the registries this
		// node pulls from (or remove ours when the mirror is off).
		go func() {
			for {
				refs, _ := inv.Get(5 * time.Minute)
				hosts.Sync(refs, mirrorOn)
				if !mirrorOn {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Minute):
				}
			}
		}()
	}

	httpServer := &http.Server{Addr: listenAddr, Handler: worker.NewServer(puller, rescue, extra...)}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("angryduck-worker[%s]: http server failed: %v", nodeID, err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("angryduck-worker[%s]: shutting down", nodeID)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	cancel()
}

// newRescue sets up the peer-to-peer image transfer used by rescue and
// propagation. It is mounted only with a valid token: its endpoints hand
// out image content.
func newRescue(ctx context.Context, nodeID, hostRoot, stateDir string, store *worker.CtrStore, token string) *worker.Rescue {
	// The worker binary is built for the node's architecture.
	platform := config.String("RESCUE_PLATFORM", "linux/"+runtime.GOARCH)
	maxConcurrent := config.Int("RESCUE_NODE_MAX_CONCURRENT", 2)
	pinTTL := config.Duration("RESCUE_PIN_TTL_S", 3600)
	pinFile := filepath.Join(hostRoot, stateDir, "rescue-pins.json")
	log.Printf("angryduck-worker[%s]: rescue enabled: platform=%s max_concurrent=%d pin_ttl=%s pin_file=%s", nodeID, platform, maxConcurrent, pinTTL, pinFile)
	pins := worker.NewPins(store, nodeID, pinTTL, pinFile)
	cleanupCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	worker.CleanupLeftovers(cleanupCtx, store, pins, nodeID)
	cancel()
	go pins.Run(ctx, 5*time.Minute)
	return worker.NewRescue(store, pins, token, nodeID, platform, maxConcurrent)
}

// listenPort extracts the port from a listen address like ":18081".
func listenPort(listenAddr string) string {
	idx := strings.LastIndex(listenAddr, ":")
	if idx < 0 || idx == len(listenAddr)-1 {
		return "18081"
	}
	port := listenAddr[idx+1:]
	if _, err := strconv.Atoi(port); err != nil {
		return "18081"
	}
	return port
}
