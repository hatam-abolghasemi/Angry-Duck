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
// It talks to containerd over its socket only (containerd's own API plus
// CRI): no node binaries, no chroot, no host PID namespace.
package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd"

	"angryduck/internal/config"
	"angryduck/internal/kube"
	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
	"angryduck/internal/metrics"
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
	runtimeKind := strings.ToLower(config.String("CONTAINER_RUNTIME", "containerd"))
	switch runtimeKind {
	case "containerd", "crictl", "ctr":
		// crictl and ctr were the CLI backends before 1.8.6; both meant
		// containerd, which is what the worker now talks to directly.
	default:
		log.Fatalf("angryduck-worker: CONTAINER_RUNTIME=%s is not supported: Angry Duck needs containerd", runtimeKind)
	}
	runtimeEndpoint := config.String("CONTAINER_RUNTIME_ENDPOINT", "unix:///run/containerd/containerd.sock")
	credsPath := config.String("REGISTRY_CREDENTIALS_PATH", "")
	creds, err := registryauth.Load(credsPath)
	if err != nil {
		log.Fatalf("angryduck-worker: failed to load registry credentials from %s: %v", credsPath, err)
	}
	if credsPath != "" {
		log.Printf("angryduck-worker: loaded credentials for %d registr(y/ies) from %s", creds.Count(), credsPath)
	}
	// Prefix for the node paths this pod mounts (the state dir, containerd's
	// config and certs.d). The manifests mount them at the same paths, so
	// it is empty; it is not a chroot.
	hostRoot := config.String("HOST_ROOT", "")
	stateDir := config.String("RESCUE_STATE_DIR", "/var/lib/angryduck")
	namespace := config.String("RESCUE_CONTAINERD_NAMESPACE", "k8s.io")
	snapshotter := config.String("CONTAINERD_SNAPSHOTTER", "overlayfs")

	log.Printf("angryduck-worker[%s]: starting: listen=%s self=%s metrics=%s controller=%s report_interval=%s containerd=%s namespace=%s snapshotter=%s",
		nodeID, listenAddr, selfAddress, metricsURL, controllerURL, reportInterval, runtimeEndpoint, namespace, snapshotter)

	client, err := containerd.New(strings.TrimPrefix(runtimeEndpoint, "unix://"),
		containerd.WithDefaultNamespace(namespace), containerd.WithTimeout(10*time.Second),
		containerd.WithDialOpts(worker.ContainerdDialOptions(nodeID)))
	if err != nil {
		log.Fatalf("angryduck-worker[%s]: connecting to containerd at %s: %v", nodeID, runtimeEndpoint, err)
	}
	defer client.Close()
	if v, err := client.Version(context.Background()); err == nil {
		log.Printf("angryduck-worker[%s]: connected to containerd %s", nodeID, v.Version)
	}

	rt := worker.NewRuntime(client.Conn(), creds)
	inv := worker.NewInventory(rt)
	puller := worker.NewPuller(rt, nodeID, labelRegistry)
	reporter := worker.NewReporter(nodeID, selfAddress, metricsURL, controllerURL, reportInterval, inv)
	puller.OnSuccess(reporter.Kick)

	// One containerd store shared by everything below, so its short
	// listing cache is shared too.
	store := worker.NewContainerdStore(client, namespace, snapshotter)
	if dir := store.UseBlobDir(config.String("CONTAINERD_ROOT", ""), filepath.Join(hostRoot, "/etc/containerd/config.toml")); dir != "" {
		log.Printf("angryduck-worker[%s]: reading blobs directly from %s", nodeID, dir)
	} else {
		log.Printf("angryduck-worker[%s]: containerd's content store isn't mounted (set CONTAINERD_ROOT to where it is): serving blobs through containerd's content API", nodeID)
	}
	token, tokenErr := sharedtoken.Load(config.String("RESCUE_TOKEN_PATH", "/etc/angryduck/rescue-token/token"))
	if tokenErr != nil {
		log.Printf("angryduck-worker[%s]: WARNING: no usable token (%v): rescue, propagation and the mirror's peer lookups are off, and /pull is unauthenticated", nodeID, tokenErr)
		token = ""
	}
	authMode, err := sharedtoken.ParseMode(config.String("AUTH_MODE", ""))
	if err != nil {
		log.Fatalf("angryduck-worker[%s]: %v", nodeID, err)
	}
	auth := &sharedtoken.Auth{Token: token, Mode: authMode}
	reporter.SetToken(token)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if store != nil && config.Bool("LAYER_INVENTORY_ENABLED", true) {
		interval := config.Duration("LAYER_SCAN_INTERVAL_S", 60)
		log.Printf("angryduck-worker[%s]: layer inventory enabled: scan_interval=%s", nodeID, interval)
		reporter.SetLayerTracker(worker.NewLayerTracker(store, nodeID, interval))
	}
	go metrics.TrackProcessMemory(ctx, "worker")
	go memlimit.ReleaseWhenIdle(ctx, time.Minute, 1<<20)
	go reporter.Run(ctx)
	if preheatAttributionInterval > 0 {
		go worker.NewPreheatMonitor(rt, puller, preheatAttributionInterval, preheatAttributionRetention, nodeID).Run(ctx)
	}

	var rescue *worker.Rescue
	var pins *worker.Pins
	if store != nil && tokenErr == nil && config.Bool("RESCUE_ENABLED", true) {
		rescue, pins = newRescue(ctx, nodeID, hostRoot, stateDir, store, token)
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

	var hosts *worker.HostsConfig
	var mirrorSrv *http.Server
	if store != nil {
		mirrorOn := config.Bool("MIRROR_ENABLED", false) && tokenErr == nil
		// The mirror listens on the pod network; containerd, on the host
		// network, reaches it at the pod IP and sends a per-pod random
		// token from hosts.toml, so no other pod can use it.
		mirrorListen := mirrorListenAddr(config.String("MIRROR_LISTEN_ADDR", ":18082"), nodeID)
		mirrorAddr := config.String("MIRROR_ADVERTISE_ADDR", "")
		if podIP := config.String("POD_IP", ""); mirrorAddr == "" && podIP != "" {
			_, port, _ := net.SplitHostPort(mirrorListen)
			mirrorAddr = net.JoinHostPort(podIP, port)
		}
		if mirrorOn && mirrorAddr == "" {
			log.Printf("angryduck-worker[%s]: WARNING: mirror off: set POD_IP (downward API) or MIRROR_ADVERTISE_ADDR", nodeID)
			mirrorOn = false
		}
		hosts = &worker.HostsConfig{
			HostRoot:  hostRoot,
			ConfigDir: config.String("MIRROR_CONTAINERD_CONFIG_DIR", "/etc/containerd/certs.d"),
			Mirror:    mirrorAddr,
			Token:     randomToken(),
			Extra:     config.StringSlice("MIRROR_REGISTRIES", nil),
			NodeID:    nodeID,
		}
		if mirrorOn {
			m := worker.NewMirror(store, nodeID, token, controllerURL)
			m.RequireClientToken(hosts.Token)
			extra = append(extra, m.RegisterPeer)
			mirrorSrv = &http.Server{Addr: mirrorListen, Handler: m.Handler(), ReadHeaderTimeout: 10 * time.Second}
			go func() {
				log.Printf("angryduck-worker[%s]: mirror listening on %s, containerd reaches it at %s", nodeID, mirrorListen, mirrorAddr)
				if err := mirrorSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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

	httpServer := &http.Server{Addr: listenAddr, Handler: worker.NewServer(auth, puller, rescue, extra...)}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("angryduck-worker[%s]: http server failed: %v", nodeID, err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("angryduck-worker[%s]: shutting down", nodeID)
	shutdown(nodeID, cancel, hosts, mirrorSrv, httpServer, store, pins, filepath.Join(hostRoot, stateDir))
}

// shutdown runs on SIGTERM: a rollout, an eviction, or the DaemonSet being
// deleted. It fits in the pod's 30-second grace period.
//
// Every time: containerd stops being pointed at this pod (it goes straight
// to the registry until the next worker writes hosts.toml again), the
// servers stop, and rescue pins are released.
//
// When the DaemonSet itself is gone or being deleted, nothing will start
// on this node again, so it also removes what the next worker would have
// cleaned up at startup (temporary snapshots and base images) and its
// state files. If it can't tell, it keeps them: a later install picks up
// from there.
func shutdown(nodeID string, cancel context.CancelFunc, hosts *worker.HostsConfig, mirrorSrv, httpServer *http.Server,
	store *worker.ContainerdStore, pins *worker.Pins, stateDir string) {
	if hosts != nil {
		if n := hosts.RemoveOwn(); n > 0 {
			log.Printf("angryduck-worker[%s]: removed %d hosts.toml file(s): containerd pulls from registries directly", nodeID, n)
		}
	}
	cancel() // stop the loops
	ctx, done := context.WithTimeout(context.Background(), 20*time.Second)
	defer done()
	if mirrorSrv != nil {
		_ = mirrorSrv.Shutdown(ctx)
	}
	_ = httpServer.Shutdown(ctx)
	if pins != nil {
		pins.ReleaseAll(ctx)
	}
	if !uninstalling(ctx, nodeID) {
		return
	}
	log.Printf("angryduck-worker[%s]: DaemonSet deleted: removing this node's Angry Duck state", nodeID)
	if store != nil && pins != nil {
		worker.CleanupLeftovers(ctx, store, pins, nodeID)
	}
	for _, f := range []string{"rescue-pins.json", "image-usage.json"} {
		if err := os.Remove(filepath.Join(stateDir, f)); err != nil && !os.IsNotExist(err) {
			log.Printf("angryduck-worker[%s]: removing %s: %v", nodeID, f, err)
		}
	}
	// The directory is a mount point inside the pod; empty it is all that
	// can be done from here. kubelet created it, and an empty one is harmless.
}

// uninstalling asks the API server whether this pod's DaemonSet is
// deleted or being deleted.
func uninstalling(ctx context.Context, nodeID string) bool {
	name := config.String("DAEMONSET_NAME", "angryduck-worker")
	ns := config.String("POD_NAMESPACE", kube.Namespace())
	c, err := kube.InCluster()
	if err != nil || ns == "" {
		log.Printf("angryduck-worker[%s]: can't check whether DaemonSet %s is being deleted (%v); keeping node state", nodeID, name, err)
		return false
	}
	gone, err := c.DaemonSetGone(ctx, ns, name)
	if err != nil {
		log.Printf("angryduck-worker[%s]: checking DaemonSet %s/%s: %v; keeping node state", nodeID, ns, name, err)
		return false
	}
	return gone
}

// mirrorListenAddr turns a loopback address from a pre-1.8.6 config into a
// pod-network one: the worker no longer shares the node's network, so
// containerd can't reach its loopback.
func mirrorListenAddr(addr, nodeID string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ":18082"
	}
	if host == "127.0.0.1" || host == "localhost" || host == "::1" {
		log.Printf("angryduck-worker[%s]: MIRROR_LISTEN_ADDR=%s is loopback, which containerd can't reach from the host network; listening on :%s", nodeID, addr, port)
		return ":" + port
	}
	return addr
}

func randomToken() string {
	b := make([]byte, 32)
	if _, err := crand.Read(b); err != nil {
		log.Fatalf("angryduck-worker: random token: %v", err)
	}
	return hex.EncodeToString(b)
}

// newRescue sets up the peer-to-peer image transfer used by rescue and
// propagation. It is mounted only with a valid token: its endpoints hand
// out image content.
func newRescue(ctx context.Context, nodeID, hostRoot, stateDir string, store *worker.ContainerdStore, token string) (*worker.Rescue, *worker.Pins) {
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
	return worker.NewRescue(store, pins, token, nodeID, platform, maxConcurrent), pins
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
