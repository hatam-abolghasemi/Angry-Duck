// Command angryduck-worker runs on every node (typically as a DaemonSet
// pod). It reports root-filesystem utilization to the controller, executes
// pull orders the controller sends it, and garbage-collects images that
// have fallen out of use — while protecting freshly-ordered images with a
// short grace period so they don't get GC'd before Argo ever schedules a
// pod that needs them.
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

	nodeIP := config.String("NODE_IP", "")
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
	// Kubespray installs its node-managed binaries under /usr/local/bin by default.
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
	ctrPath := resolveToolPath(config.String("CTR_PATH", "/host/usr/local/bin/ctr"), "/usr/local/bin/ctr")
	crictlPath := resolveToolPath(config.String("CRICTL_PATH", "/host/usr/local/bin/crictl"), "/usr/local/bin/crictl")
	registryEnabled := config.Bool("REGISTRY_MIRROR_ENABLED", true)
	registryListenAddr := config.String("REGISTRY_LISTEN_ADDR", ":5000")
	registryMaxStreams := config.Int("REGISTRY_MAX_CONCURRENT_STREAMS", 4)
	registryCandidateLimit := config.Int("P2P_SOURCE_CANDIDATES", 3)
	registryCandidateCacheTTL := config.Duration("REGISTRY_CANDIDATE_CACHE_S", 1)
	inventoryCacheTTL := config.Duration("IMAGE_INVENTORY_CACHE_S", 10)
	containerdCertsDir := config.String("CONTAINERD_CERTS_DIR", "/host/etc/containerd/certs.d")
	containerdPath := config.String("CONTAINERD_PATH", "/host/usr/local/bin/containerd")
	containerdConfigPath := config.String("CONTAINERD_CONFIG_PATH", "/host/etc/containerd/config.toml")

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

	log.Printf("angryduck-worker[%s]: starting: listen=%s registry_listen=%s self=%s metrics=%s controller=%s report_interval=%s gc_interval=%s gc_miss_threshold=%d grace_period=%s runtime=%s runtime_endpoint=%s gc_dry_run=%v gc_exclude_image_substrings=%v ctr_path=%s crictl_path=%s containerd_path=%s containerd_config=%s registry_enabled=%v registry_max_streams=%d registry_candidate_limit=%d registry_candidate_ttl=%s inventory_cache_ttl=%s",
		nodeID, listenAddr, registryListenAddr, selfAddress, metricsURL, controllerURL, reportInterval, gcInterval, gcMissThreshold, gracePeriod, runtimeKind, runtimeEndpoint, gcDryRun, gcExcludeSubstrings, ctrPath, crictlPath, containerdPath, containerdConfigPath, registryEnabled, registryMaxStreams, registryCandidateLimit, registryCandidateCacheTTL, inventoryCacheTTL)

	if registryEnabled {
		if err := worker.CheckContainerdMirrorCompatibility(containerdPath, containerdConfigPath); err != nil {
			log.Printf("angryduck-worker[%s]: WARNING: %v", nodeID, err)
		}
		if err := worker.EnsureContainerdMirrorConfig(containerdCertsDir, nodeIP, registryListenAddr, true); err != nil {
			log.Printf("angryduck-worker[%s]: warning: containerd mirror config was not installed: %v", nodeID, err)
		} else {
			log.Printf("angryduck-worker[%s]: containerd _default mirror configured at %s for node=%s", nodeID, containerdCertsDir, nodeIP)
		}
	}

	rt := worker.NewRuntime(runtimeKind, creds, runtimeEndpoint, ctrPath, crictlPath)
	rt = worker.NewCachedRuntime(rt, inventoryCacheTTL)
	registry := worker.NewRegistryMirror(ctrPath, controllerURL, nodeID, registryEnabled, registryCandidateLimit, registryMaxStreams, inventoryCacheTTL, registryCandidateCacheTTL)
	puller := worker.NewPuller(rt, gracePeriod, nodeID)
	gc := worker.NewGC(rt, puller, gcInterval, gcMissThreshold, gcDryRun, gcExcludeSubstrings, nodeID)
	reporter := worker.NewReporter(nodeID, selfAddress, metricsURL, controllerURL, reportInterval, rt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reporter.Run(ctx)
	go gc.Run(ctx)

	httpServer := &http.Server{Addr: listenAddr, Handler: worker.NewServer(puller, nil)}
	registryServer := &http.Server{Addr: registryListenAddr, Handler: worker.NewRegistryServer(registry)}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("angryduck-worker[%s]: control http server failed: %v", nodeID, err)
		}
	}()
	go func() {
		if err := registryServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("angryduck-worker[%s]: registry http server failed: %v", nodeID, err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("angryduck-worker[%s]: shutting down", nodeID)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = registryServer.Shutdown(shutdownCtx)
	cancel()
}

// resolveToolPath prefers the node-mounted runtime binary, but keeps
// compatibility with legacy worker images that bundled ctr/crictl under
// /usr/local/bin.
func resolveToolPath(configured, fallback string) string {
	if configured != "" {
		if _, err := os.Stat(configured); err == nil {
			return configured
		}
	}
	if _, err := os.Stat(fallback); err == nil {
		logging.Warnf("angryduck-worker: runtime tool %q is unavailable; falling back to %q", configured, fallback)
		return fallback
	}
	return configured
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
