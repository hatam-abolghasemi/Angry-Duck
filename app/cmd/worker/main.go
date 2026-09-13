// Command angryduck-worker runs on every node (typically as a DaemonSet
// pod). It reports root-filesystem utilization to the controller, executes
// pull orders the controller sends it, and garbage-collects images that
// have fallen out of use — while protecting freshly-ordered images with a
// short grace period so they don't get GC'd before Argo ever schedules a
// pod that needs them.
//
// It carries no container CLIs of its own: crictl/ctr/docker are the
// node's binaries, run chrooted into HOST_ROOT (see worker.HostExec).
//
// Peer-to-peer image distribution for a running fleet is Spegel's job,
// not this one — Angry Duck only ever does the initial preheat pull onto
// a few seed nodes, GC, and (with RESCUE_ENABLED) one narrow exception: a
// single, one-shot image handoff to unstick a pod already failing in
// ImagePullBackOff, never a standing mirror in front of containerd.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"angryduck/internal/config"
	"angryduck/internal/logging"
	"angryduck/internal/registryauth"
	"angryduck/internal/worker"
)

func main() {
	envFile := config.String("ENV_FILE", ".env")
	if err := config.Load(envFile); err != nil {
		log.Printf("angryduck-worker: warning: failed to load %s: %v", envFile, err)
	}
	logging.SetLevel(logging.ParseLevel(config.String("LOG_LEVEL", "info")))

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

	// SELF_ADDRESS is commonly injected via the k8s downward API as a bare
	// IP (status.podIP), which has no port. If it's missing one, append the
	// port this worker's HTTP server actually listens on so the controller
	// can dial it back correctly.
	if !strings.Contains(selfAddress, ":") {
		selfAddress = selfAddress + ":" + listenPort(listenAddr)
	}
	metricsURL := config.String("NODE_EXPORTER_URL", "http://localhost:9100/metrics")
	controllerURL := config.String("CONTROLLER_URL", "http://angryduck-controller:8080")
	reportInterval := config.Duration("REPORT_INTERVAL_S", 15)
	gcInterval := config.Duration("GC_CHECK_INTERVAL_S", 60)
	gcMissThreshold := config.Int("GC_MISS_THRESHOLD", 5)
	gracePeriod := config.Duration("GC_GRACE_PERIOD_S", 60)
	gcExcludeSubstrings := config.StringSlice("GC_EXCLUDE_IMAGE_SUBSTRINGS", nil)
	// Off by default: registry host is an unbounded-ish label (unlike
	// node), so it's opt-in — flip on for troubleshooting which
	// registries' images are filling disk, flip back off if it makes the
	// worker's /metrics too large for your Prometheus setup. Shared with
	// the controller's angryduck_controller_pull_orders_total, since both
	// read the same ConfigMap.
	labelRegistry := config.Bool("METRICS_LABEL_REGISTRY", false)
	// How often PreheatMonitor samples running containers against
	// recently-preheated repos, and how long a repo counts as
	// "recently preheated" once a pull for it succeeds here. Deliberately
	// much coarser than GC_CHECK_INTERVAL_S — this is a dashboard sample,
	// not a decision that needs to react quickly. On the crictl backend
	// this is already close to free (the monitor skips its runtime call
	// entirely on any node with nothing preheated recently — see
	// PreheatMonitor.tick), but on CONTAINER_RUNTIME=containerd it's a
	// second N+1-subprocess pass on top of GC's own; set this to 0 to
	// disable the monitor entirely if that ever matters more than the
	// visibility it buys.
	preheatAttributionInterval := config.Duration("PREHEAT_ATTRIBUTION_INTERVAL_S", 300)
	preheatAttributionRetention := config.Duration("PREHEAT_ATTRIBUTION_RETENTION_S", 3600)
	// Empty (the default) disables Spegel-presence detection entirely —
	// no extra cost paid unless set. When non-empty, every preheat pull
	// does ONE extra ListRunningImages() call (not a periodic poll: pulls
	// are already a low-frequency event, bounded by RANK_TOP_N per push,
	// so this stays cheap even on the ctr backend) to check whether a
	// container whose image contains this substring is running on this
	// node, and labels angryduck_worker_pull_duration_seconds with the
	// result — answering "was this pull's timing measured with or
	// without Spegel in place," using only the local container runtime,
	// no Kubernetes API access.
	spegelImageSubstring := config.String("SPEGEL_IMAGE_SUBSTRING", "")
	// crictl is the default: unlike ctr, it lists every running
	// container's image in one call instead of one subprocess per
	// container — see the comment on NewRuntime for why this matters.
	runtimeKind := config.String("CONTAINER_RUNTIME", "crictl")
	// Only consulted by the crictl backend. Defaults to the socket every
	// deploy manifest already mounts, so this normally doesn't need to be
	// set explicitly.
	runtimeEndpoint := config.String("CONTAINER_RUNTIME_ENDPOINT", "unix:///run/containerd/containerd.sock")
	// Defaults to true deliberately: after any change to the runtime
	// matching logic, watch what GC decides via logs before letting it
	// actually delete anything. Set GC_DRY_RUN=false to enable real removal.
	gcDryRun := config.Bool("GC_DRY_RUN", true)

	// The worker pulls images by shelling out directly to the container
	// runtime CLI, bypassing kubelet's CRI plumbing entirely — so
	// kubelet's own imagePullSecrets never apply here. Point this at a
	// mounted dockerconfigjson secret (the same format imagePullSecrets
	// use) to give preheat pulls credentials for private registries. Not
	// setting this is fine for public images; private ones will fail with
	// a 401/403 until it's configured.
	credsPath := config.String("REGISTRY_CREDENTIALS_PATH", "")
	creds, err := registryauth.Load(credsPath)
	if err != nil {
		log.Fatalf("angryduck-worker: failed to load registry credentials from %s: %v", credsPath, err)
	}
	if credsPath != "" {
		log.Printf("angryduck-worker: loaded credentials for %d registr(y/ies) from %s", creds.Count(), credsPath)
	}

	// Where the node's filesystem is visible from inside this container.
	// /proc/1/root is the node's real root (with hostPID: true), live
	// mounts included, and needs no hostPath volume. Set to "/" for local
	// runs outside Kubernetes.
	hostRoot := config.String("HOST_ROOT", "/proc/1/root")

	// Rescue: a pod stuck in ImagePullBackOff for a reason nothing else
	// here can see (kubelet gives up before containerd's own pull ever
	// starts, e.g. origin unreachable for that specific registry/node).
	// Deliberately not P2P: at most ONE attempt per stuck image per
	// RESCUE_RETRY_INTERVAL_S, from at most ONE source the controller
	// knows about, then silence until the interval passes. See rescue.go.
	rescueEnabled := config.Bool("RESCUE_ENABLED", true)
	rescueInterval := config.Duration("RESCUE_POLL_INTERVAL_S", 30)
	rescueRetryInterval := config.Duration("RESCUE_RETRY_INTERVAL_S", 600)
	rescueExcludeNamespaces := config.StringSlice("RESCUE_EXCLUDE_NAMESPACE_SUBSTRINGS", nil)
	rescueExcludeImages := config.StringSlice("RESCUE_EXCLUDE_IMAGE_SUBSTRINGS", nil)
	rescueToken := config.String("RESCUE_PEER_TOKEN", "")
	rescueMaxConcurrentExports := config.Int("RESCUE_MAX_CONCURRENT_EXPORTS", 1)

	log.Printf("angryduck-worker[%s]: starting: listen=%s self=%s metrics=%s controller=%s report_interval=%s gc_interval=%s gc_miss_threshold=%d grace_period=%s runtime=%s runtime_endpoint=%s gc_dry_run=%v gc_exclude_image_substrings=%v host_root=%s rescue_feature_enabled=%v metrics_label_registry=%v preheat_attribution_interval=%s preheat_attribution_retention=%s spegel_image_substring=%q",
		nodeID, listenAddr, selfAddress, metricsURL, controllerURL, reportInterval, gcInterval, gcMissThreshold, gracePeriod, runtimeKind, runtimeEndpoint, gcDryRun, gcExcludeSubstrings, hostRoot, rescueEnabled, labelRegistry, preheatAttributionInterval, preheatAttributionRetention, spegelImageSubstring)

	hx, err := worker.NewHostExec(hostRoot)
	if err != nil {
		log.Fatalf("angryduck-worker[%s]: %v", nodeID, err)
	}
	// Fail fast: a worker that can't find its runtime CLI on the node can
	// neither GC nor preheat, and a crash loop is louder than a log line.
	bin := worker.RuntimeBinary(runtimeKind)
	binPath, err := hx.Resolve(bin)
	if err != nil {
		log.Fatalf("angryduck-worker[%s]: CONTAINER_RUNTIME=%s needs %s on the node: %v", nodeID, runtimeKind, bin, err)
	}
	log.Printf("angryduck-worker[%s]: using node binary %s", nodeID, binPath)

	rt := worker.NewRuntime(runtimeKind, creds, runtimeEndpoint, hx)
	inv := worker.NewInventory(rt)
	puller := worker.NewPuller(rt, gracePeriod, nodeID, labelRegistry, spegelImageSubstring)
	gc := worker.NewGC(rt, inv, reportInterval, puller, gcInterval, gcMissThreshold, gcDryRun, gcExcludeSubstrings, nodeID, labelRegistry)
	reporter := worker.NewReporter(nodeID, selfAddress, metricsURL, controllerURL, reportInterval, inv)
	puller.OnSuccess(reporter.Kick)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go reporter.Run(ctx)
	go gc.Run(ctx)
	if preheatAttributionInterval > 0 {
		preheatMonitor := worker.NewPreheatMonitor(rt, puller, preheatAttributionInterval, preheatAttributionRetention, nodeID)
		go preheatMonitor.Run(ctx)
	} else {
		log.Printf("angryduck-worker[%s]: preheat-attribution monitor disabled (PREHEAT_ATTRIBUTION_INTERVAL_S=0)", nodeID)
	}

	// Rescue setup: soft-fails (logs and runs in count-only mode) rather
	// than crashing, since it's a secondary safety net, not core to what
	// this worker does — unlike the old mirror, a misconfigured rescue
	// doesn't make every pull silently go to origin; it just means a pod
	// stuck for reasons rescue could have fixed stays stuck, exactly as
	// it would if Angry Duck weren't installed at all.
	var exporter *worker.RescueExporter
	var rescuer *worker.Rescuer
	if rescueEnabled {
		containerdAddr := strings.TrimPrefix(runtimeEndpoint, "unix://")
		if _, err := hx.Resolve("ctr"); err != nil {
			log.Printf("angryduck-worker[%s]: rescue running in count-only mode: ctr is not on the node, so stuck pulls will be logged but not fixed", nodeID)
		} else if len(rescueToken) < 16 {
			log.Printf("angryduck-worker[%s]: rescue running in count-only mode: RESCUE_PEER_TOKEN is unset or under 16 chars, so stuck pulls will be logged but not fixed", nodeID)
		} else {
			exporter = worker.NewRescueExporter(nodeID, rescueToken, hx, containerdAddr, "k8s.io", rescueMaxConcurrentExports)
			rescuer = worker.NewRescuer(nodeID, controllerURL, rescueToken, hx, containerdAddr, "k8s.io")
			log.Printf("angryduck-worker[%s]: rescue running in fix mode: poll_interval=%s retry_interval=%s max_concurrent_exports=%d",
				nodeID, rescueInterval, rescueRetryInterval, rescueMaxConcurrentExports)
		}
		if k8sClient, err := worker.NewInClusterK8sClient(); err != nil {
			log.Printf("angryduck-worker[%s]: rescue watch not started: %v", nodeID, err)
		} else {
			var attempt func(context.Context, string) error
			if rescuer != nil {
				attempt = rescuer.Attempt
			}
			rw := worker.NewRescueWatch(nodeID, k8sClient, rescueInterval, rescueRetryInterval, rescueExcludeNamespaces, rescueExcludeImages, attempt)
			go rw.Run(ctx)
		}
	}

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: worker.NewServer(puller, exporter),
	}

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

// listenPort extracts the port from a listen address like ":18081" or
// "0.0.0.0:18081". Falls back to "18081" if it can't parse one.
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
