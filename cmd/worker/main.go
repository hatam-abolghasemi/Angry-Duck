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
	"angryduck/internal/worker"
)

func main() {
	envFile := config.String("ENV_FILE", ".env")
	if err := config.Load(envFile); err != nil {
		log.Printf("angryduck-worker: warning: failed to load %s: %v", envFile, err)
	}

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
	runtimeKind := config.String("CONTAINER_RUNTIME", "containerd")

	log.Printf("angryduck-worker[%s]: starting: listen=%s self=%s metrics=%s controller=%s report_interval=%s gc_interval=%s gc_miss_threshold=%d grace_period=%s runtime=%s",
		nodeID, listenAddr, selfAddress, metricsURL, controllerURL, reportInterval, gcInterval, gcMissThreshold, gracePeriod, runtimeKind)

	rt := worker.NewRuntime(runtimeKind)
	puller := worker.NewPuller(rt, gracePeriod)
	gc := worker.NewGC(rt, puller, gcInterval, gcMissThreshold)
	reporter := worker.NewReporter(nodeID, selfAddress, metricsURL, controllerURL, reportInterval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reporter.Run(ctx)
	go gc.Run(ctx)

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: worker.NewServer(puller),
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
