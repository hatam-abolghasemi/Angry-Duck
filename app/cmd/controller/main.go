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
	"angryduck/internal/logging"
	"angryduck/internal/memlimit"
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
	// angryduck_controller_pull_orders_total and the worker-side pull/GC
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
