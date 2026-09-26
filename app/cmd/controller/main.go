// Command angryduck-controller runs the Angry Duck control plane: it
// receives the post-`docker push` webhook, tracks worker disk-utilization
// and image-inventory reports, and orders nodes to pre-pull the new image —
// preferring nodes that already have some tag of the same repo locally,
// then falling back to the least-utilized nodes.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"angryduck/internal/config"
	"angryduck/internal/controller"
	"angryduck/internal/kube"
	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
	"angryduck/internal/sharedtoken"
)

func main() {
	envFile := config.String("ENV_FILE", ".env")
	if err := config.Load(envFile); err != nil {
		log.Printf("angryduck-controller: warning: failed to load %s: %v", envFile, err)
	}
	logging.SetLevel(logging.ParseLevel(config.String("LOG_LEVEL", "info")))
	memlimit.Apply("controller")

	listenAddr := config.String("CONTROLLER_LISTEN_ADDR", ":8080")
	staleAfter := config.Duration("WORKER_STALE_AFTER_S", 30)
	targetTTL := config.Duration("TARGET_TTL_S", 120)
	rankInterval := config.Duration("RANK_INTERVAL_S", 10)
	topN := config.Int("RANK_TOP_N", 2)
	excludeNodeSubstrings := config.StringSlice("RANK_EXCLUDE_NODE_SUBSTRINGS", nil)
	// Defaults to true: prefer pre-pulling onto a node that already has
	// some tag of the target image's repo before falling back to
	// utilization-only ranking. Set to false to restore the old
	// utilization-only behavior if this ever needs a quick rollback.
	preferImageLocality := config.Bool("RANK_PREFER_IMAGE_LOCALITY", true)
	// Same flag, same default, as the worker's METRICS_LABEL_REGISTRY —
	// both binaries read it from the same ConfigMap so
	// angryduck_controller_pull_orders_total and the worker-side pull
	// metrics turn their registry-host label on or off together.
	labelRegistry := config.Bool("METRICS_LABEL_REGISTRY", false)

	log.Printf("angryduck-controller: starting: listen=%s stale_after=%s target_ttl=%s rank_interval=%s top_n=%d rank_exclude_node_substrings=%v rank_prefer_image_locality=%v metrics_label_registry=%v",
		listenAddr, staleAfter, targetTTL, rankInterval, topN, excludeNodeSubstrings, preferImageLocality, labelRegistry)

	registry := controller.NewRegistry(staleAfter, targetTTL)
	ranker := controller.NewRanker(registry, topN, rankInterval, excludeNodeSubstrings, preferImageLocality, labelRegistry)
	server := controller.NewServer(registry, ranker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ranker.Run(ctx)
	// Rescue and propagation both order workers to copy images from each
	// other, and the workers only accept that with the shared token.
	token, tokenErr := sharedtoken.Load(config.String("RESCUE_TOKEN_PATH", "/etc/angryduck/rescue-token/token"))
	if tokenErr != nil {
		log.Printf("angryduck-controller: WARNING: no usable rescue token, so rescue and propagation are off: %v", tokenErr)
	} else {
		if rescuer := newRescuer(registry, token); rescuer != nil {
			server.SetRescuer(rescuer)
			go rescuer.Run(ctx)
		}
		if propagator := newPropagator(registry, token, excludeNodeSubstrings); propagator != nil {
			server.SetPropagator(propagator)
			go propagator.Run(ctx)
		}
	}

	httpServer := &http.Server{
		Addr:    listenAddr,
		Handler: server.Handler(),
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("angryduck-controller: http server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("angryduck-controller: shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	cancel()
}

// newRescuer sets up ImagePullBackOff rescue, or returns nil (feature off)
// with a log line saying why. Rescue is on by default but needs two things
// the base install didn't: a service account allowed to list pods, and the
// shared token the workers check.
func newRescuer(registry *controller.Registry, token string) *controller.Rescuer {
	if !config.Bool("RESCUE_ENABLED", true) {
		log.Println("angryduck-controller: rescue disabled (RESCUE_ENABLED=false)")
		return nil
	}
	pods, err := kube.InCluster()
	if err != nil {
		log.Printf("angryduck-controller: WARNING: rescue disabled, no Kubernetes API access: %v", err)
		return nil
	}
	cfg := controller.RescuerConfig{
		Interval:      config.Duration("RESCUE_INTERVAL_S", 15),
		RetryAfter:    config.Duration("RESCUE_RETRY_AFTER_S", 120),
		BackoffMax:    config.Duration("RESCUE_BACKOFF_MAX_S", 3600),
		Timeout:       config.Duration("RESCUE_TIMEOUT_S", 600),
		MaxConcurrent: config.Int("RESCUE_MAX_CONCURRENT", 2),
		MaxSources:    config.Int("RESCUE_MAX_SOURCES", 3),
	}
	return controller.NewRescuer(registry, pods, token, cfg)
}

// newPropagator sets up spreading each pushed image to every eligible node
// from peers, or returns nil when it's turned off.
func newPropagator(registry *controller.Registry, token string, rankExclude []string) *controller.Propagator {
	if !config.Bool("PROPAGATE_ENABLED", true) {
		log.Println("angryduck-controller: propagation disabled (PROPAGATE_ENABLED=false)")
		return nil
	}
	cfg := controller.PropagatorConfig{
		Interval:       config.Duration("PROPAGATE_INTERVAL_S", 10),
		Window:         config.Duration("PROPAGATE_WINDOW_S", 3600),
		MaxConcurrent:  config.Int("PROPAGATE_MAX_CONCURRENT", 4),
		PerSource:      config.Int("PROPAGATE_PER_SOURCE", 2),
		MaxUtilization: config.Float("PROPAGATE_MAX_UTILIZATION", 0.70),
		// Same nodes preheat leaves alone (masters, typically), unless
		// set separately.
		ExcludeNodeSubstrings: config.StringSlice("PROPAGATE_EXCLUDE_NODE_SUBSTRINGS", rankExclude),
		RetryAfter:            config.Duration("RESCUE_RETRY_AFTER_S", 120),
		BackoffMax:            config.Duration("RESCUE_BACKOFF_MAX_S", 3600),
		Timeout:               config.Duration("RESCUE_TIMEOUT_S", 600),
	}
	return controller.NewPropagator(registry, token, cfg)
}
