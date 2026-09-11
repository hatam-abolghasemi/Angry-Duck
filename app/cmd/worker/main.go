// Command angryduck-worker runs on every node (typically as a DaemonSet
// pod). It reports root-filesystem utilization to the controller, executes
// pull orders the controller sends it, and garbage-collects images that
// have fallen out of use — while protecting freshly-ordered images with a
// short grace period so they don't get GC'd before Argo ever schedules a
// pod that needs them. With MIRROR_ENABLED it also sits in front of the
// origin registry for containerd and satisfies pulls from peer nodes.
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

	mirrorEnabled := config.Bool("MIRROR_ENABLED", false)
	// "*" = every registry, via containerd's certs.d/_default.
	mirrorRegistries := config.StringSlice("MIRROR_REGISTRIES", []string{"*"})
	mirrorToken := config.String("MIRROR_PEER_TOKEN", "")

	// Defaults to true: a pod stuck in ImagePullBackOff because origin
	// itself can't resolve the tag is invisible to everything else Angry
	// Duck does (GC and the mirror both only ever see containerd/CRI
	// state, which a pod this stuck never reaches). See podwatch.go.
	podWatchEnabled := config.Bool("POD_WATCH_ENABLED", true)
	podWatchInterval := config.Duration("POD_WATCH_INTERVAL_S", 30)
	podWatchBackoff := config.Duration("POD_WATCH_RETRY_BACKOFF_S", 300)
	podWatchExcludeNamespaces := config.StringSlice("POD_WATCH_EXCLUDE_NAMESPACE_SUBSTRINGS", nil)
	podWatchExcludeImages := config.StringSlice("POD_WATCH_EXCLUDE_IMAGE_SUBSTRINGS", nil)

	log.Printf("angryduck-worker[%s]: starting: listen=%s self=%s metrics=%s controller=%s report_interval=%s gc_interval=%s gc_miss_threshold=%d grace_period=%s runtime=%s runtime_endpoint=%s gc_dry_run=%v gc_exclude_image_substrings=%v host_root=%s mirror=%v pod_watch=%v",
		nodeID, listenAddr, selfAddress, metricsURL, controllerURL, reportInterval, gcInterval, gcMissThreshold, gracePeriod, runtimeKind, runtimeEndpoint, gcDryRun, gcExcludeSubstrings, hostRoot, mirrorEnabled, podWatchEnabled)

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
	puller := worker.NewPuller(rt, gracePeriod, nodeID)
	gc := worker.NewGC(rt, inv, reportInterval, puller, gcInterval, gcMissThreshold, gcDryRun, gcExcludeSubstrings, nodeID)
	reporter := worker.NewReporter(nodeID, selfAddress, metricsURL, controllerURL, reportInterval, inv)
	puller.OnSuccess(reporter.Kick)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mirror *worker.Mirror
	var hosts *worker.HostsTOML
	if mirrorEnabled {
		mirror, hosts = setupMirror(nodeID, hx, inv, controllerURL, runtimeKind, runtimeEndpoint, listenAddr, mirrorRegistries, mirrorToken)
	}

	go reporter.Run(ctx)
	go gc.Run(ctx)

	// Pod-watch: fixes a pod stuck in ImagePullBackOff by importing its
	// image from a peer under its exact tag, for the case the mirror
	// above can never help with — tag resolution against origin itself
	// failing, so containerd's pull never even reaches this worker. See
	// internal/worker/podwatch.go. Runs by default; the fix mechanism
	// itself (Mirror.FallbackImport) needs the mirror, so with
	// MIRROR_ENABLED=false pod-watch still runs and still counts failed
	// pulls for visibility, it just can't repair anything.
	if podWatchEnabled {
		if k8sClient, err := worker.NewInClusterK8sClient(); err != nil {
			log.Printf("angryduck-worker[%s]: POD_WATCH_ENABLED=true but not usable: %v — pod-watch disabled", nodeID, err)
		} else {
			var fix func(context.Context, string) error
			if mirror != nil {
				fix = mirror.FallbackImport
			} else {
				log.Printf("angryduck-worker[%s]: pod-watch running without a fix mechanism (MIRROR_ENABLED=false) — it will only count and log stuck pulls", nodeID)
			}
			pw := worker.NewPodWatch(nodeID, k8sClient, podWatchInterval, podWatchBackoff, podWatchExcludeNamespaces, podWatchExcludeImages, fix)
			go pw.Run(ctx)
		}
	}

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: worker.NewServer(puller, mirror),
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("angryduck-worker[%s]: http server failed: %v", nodeID, err)
		}
	}()
	// Register with containerd only once something is listening.
	if hosts != nil {
		go hosts.Run(ctx, 30*time.Second)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("angryduck-worker[%s]: shutting down", nodeID)

	// Unregister from containerd first, so no new pull is routed at a
	// worker that is about to stop listening.
	if hosts != nil {
		hosts.Remove()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	cancel()
}

// setupMirror validates mirror config and builds its pieces. With
// MIRROR_ENABLED=true any problem is fatal: a worker that quietly runs
// without the mirror looks healthy while every pull goes to origin (that
// is exactly how the first stg test went — pods started before the token
// Secret existed). Crashing makes it visible, and the restart re-reads
// the Secret, so creating it fixes the pods without a manual rollout.
func setupMirror(nodeID string, hx *worker.HostExec, inv *worker.Inventory, controllerURL, runtimeKind, runtimeEndpoint, listenAddr string, registries []string, token string) (*worker.Mirror, *worker.HostsTOML) {
	fail := func(format string, args ...interface{}) (*worker.Mirror, *worker.HostsTOML) {
		log.Fatalf("angryduck-worker[%s]: MIRROR_ENABLED=true but cannot start the mirror: "+format, append([]interface{}{nodeID}, args...)...)
		return nil, nil
	}
	if strings.EqualFold(runtimeKind, "docker") {
		return fail("peer transfer needs containerd; CONTAINER_RUNTIME=docker")
	}
	if len(registries) == 0 {
		return fail("MIRROR_REGISTRIES is empty")
	}
	// /export streams any image on this node to whoever asks, and the
	// worker is hostNetwork — without a shared secret that is every
	// private image readable by anything that can route to the node.
	if len(token) < 16 {
		return fail("MIRROR_PEER_TOKEN must be set (16+ chars) — see the angryduck-mirror Secret")
	}
	if _, err := hx.Resolve("ctr"); err != nil {
		return fail("%v", err)
	}
	addr := strings.TrimPrefix(runtimeEndpoint, "unix://")
	cfg := worker.MirrorConfig{
		NodeID:            nodeID,
		ControllerURL:     controllerURL,
		Token:             token,
		ContainerdAddress: addr,
		Namespace:         "k8s.io",
		HoldTimeout:       config.Duration("MIRROR_HOLD_TIMEOUT_S", 60),
		QueueWait:         config.Duration("MIRROR_QUEUE_WAIT_S", 3),
		MaxExports:        config.Int("MIRROR_MAX_EXPORTS", 2),
		MaxImports:        config.Int("MIRROR_MAX_IMPORTS", 2),
	}
	endpoint := "http://127.0.0.1:" + listenPort(listenAddr)
	var hosts *worker.HostsTOML
	if config.Bool("MIRROR_MANAGE_HOSTS_TOML", true) {
		hosts = worker.NewHostsTOML(hx, config.String("MIRROR_HOSTS_DIR", "/etc/containerd/certs.d"), registries, endpoint)
		hosts.Check()
	}
	log.Printf("angryduck-worker[%s]: mirror enabled: registries=%v endpoint=%s containerd=%s hold=%s queue_wait=%s max_exports=%d max_imports=%d",
		nodeID, registries, endpoint, addr, cfg.HoldTimeout, cfg.QueueWait, cfg.MaxExports, cfg.MaxImports)
	return worker.NewMirror(cfg, hx, inv), hosts
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
