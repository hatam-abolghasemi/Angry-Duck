// Command angryduck-controller runs the Angry Duck control plane: it
// receives the post-`docker push` webhook, tracks worker reports (disk,
// images, layer inventory), picks as seeds the nodes that already hold the
// most of the new image, spreads it from them to every node, rescues pods
// stuck on pulls, and tells workers' mirrors which peer holds a blob.
package main

import (
	"context"
	"errors"
	"io/fs"
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
	"angryduck/internal/metrics"
	"angryduck/internal/registryauth"
	"angryduck/internal/registryclient"
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
	topN := config.Int("RANK_TOP_N", 5)
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
	authMode, err := sharedtoken.ParseMode(config.String("AUTH_MODE", ""))
	if err != nil {
		log.Fatalf("angryduck-controller: %v", err)
	}
	server.SetAuthMode(authMode)
	webhookTokenPath := config.String("WEBHOOK_TOKEN_PATH", "/etc/angryduck/webhook-token/token")
	switch wt, err := sharedtoken.Load(webhookTokenPath); {
	case err == nil:
		server.SetWebhookToken(wt)
		log.Printf("angryduck-controller: /webhook/preheat requires the webhook token from %s", webhookTokenPath)
	case webhookTokenPath == "" || errors.Is(err, fs.ErrNotExist):
		log.Printf("angryduck-controller: WARNING: no webhook token at %q: /webhook/preheat is unauthenticated", webhookTokenPath)
	default:
		// A token that exists but is unusable is a mistake, not a choice
		// to run open.
		log.Fatalf("angryduck-controller: webhook token: %v", err)
	}

	// Layer ranking reads each pushed image's manifest and config from its
	// registry (a few KB), with the same pull secret the workers use.
	platform := config.String("RANK_PLATFORM", "linux/amd64")
	var resolver controller.LayerResolver
	var regClient *registryclient.Client
	if config.Bool("RANK_BY_LAYERS", true) {
		credsPath := config.String("REGISTRY_CREDENTIALS_PATH", "")
		creds, err := registryauth.Load(credsPath)
		if err != nil {
			log.Printf("angryduck-controller: WARNING: registry credentials from %s unusable, resolving anonymously: %v", credsPath, err)
			creds = registryauth.Empty()
		}
		regClient = registryclient.New(creds, config.Duration("RANK_RESOLVE_TIMEOUT_S", 5), config.Duration("RANK_RESOLVE_CACHE_S", 600))
		resolver = regClient
		ranker.SetLayerResolver(resolver, platform, config.Duration("RANK_RESOLVE_TIMEOUT_S", 5))
		log.Printf("angryduck-controller: layer ranking on: platform=%s credentials for %d registr(y/ies)", platform, creds.Count())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Rescue and propagation both order workers to copy images from each
	// other, and the workers only accept that with the shared token.
	token, tokenErr := sharedtoken.Load(config.String("RESCUE_TOKEN_PATH", "/etc/angryduck/rescue-token/token"))
	if tokenErr != nil {
		log.Printf("angryduck-controller: WARNING: no usable rescue token, so rescue and propagation are off: %v", tokenErr)
	} else {
		server.SetToken(token)
		ranker.SetToken(token)
		log.Printf("angryduck-controller: /report requires the shared token (AUTH_MODE=%s)", authMode)
		rescuer, pods := newRescuer(registry, token)
		if rescuer != nil {
			if resolver != nil {
				rescuer.SetLayerResolver(resolver, platform)
			}
			server.SetRescuer(rescuer)
			go pods.Run(ctx)
			go rescuer.Run(ctx)
		}
		if propagator := newPropagator(registry, token, excludeNodeSubstrings, platform); propagator != nil {
			propagator.SetSeeds(ranker.Seeds)
			if rescuer != nil {
				propagator.SetWaiting(rescuer.Waiting)
			}
			if resolver != nil {
				propagator.SetLayerResolver(resolver)
			}
			server.SetPropagator(propagator)
			go propagator.Run(ctx)
		}
		if spreader := newSpreader(registry, regClient, token, excludeNodeSubstrings, platform, topN); spreader != nil {
			if rescuer != nil {
				spreader.SetWaiting(rescuer.Waiting)
			}
			server.SetSpreader(spreader)
			go spreader.Run(ctx)
		}
	}

	go metrics.TrackProcessMemory(ctx, "controller")
	go memlimit.ReleaseWhenIdle(ctx, time.Minute, 1<<20)
	// Started after SetToken so pull orders never race the token.
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

// newRescuer sets up ImagePullBackOff rescue, or returns nil (feature off)
// with a log line saying why. Rescue is on by default but needs two things
// the base install didn't: a service account allowed to list pods, and the
// shared token the workers check.
func newRescuer(registry *controller.Registry, token string) (*controller.Rescuer, *kube.PodWatch) {
	if !config.Bool("RESCUE_ENABLED", true) {
		log.Println("angryduck-controller: rescue disabled (RESCUE_ENABLED=false)")
		return nil, nil
	}
	api, err := kube.InCluster()
	if err != nil {
		log.Printf("angryduck-controller: WARNING: rescue disabled, no Kubernetes API access: %v", err)
		return nil, nil
	}
	cfg := controller.RescuerConfig{
		Interval:      config.Duration("RESCUE_INTERVAL_S", 15),
		RetryAfter:    config.Duration("RESCUE_RETRY_AFTER_S", 120),
		BackoffMax:    config.Duration("RESCUE_BACKOFF_MAX_S", 3600),
		Timeout:       config.Duration("RESCUE_TIMEOUT_S", 600),
		MaxConcurrent: config.Int("RESCUE_MAX_CONCURRENT", 2),
		MaxSources:    config.Int("RESCUE_MAX_SOURCES", 3),
	}
	// Pending pods are watched, so a pod stuck pulling is seen as kubelet
	// reports it; the tick only re-checks backoffs in memory.
	var rescuer *controller.Rescuer
	pods := kube.NewPodWatch(api, "status.phase=Pending", config.Duration("RESCUE_RESYNC_INTERVAL_S", 600), cfg.Interval,
		func() {
			if rescuer != nil {
				rescuer.Wake()
			}
		})
	rescuer = controller.NewRescuer(registry, pods, token, cfg)
	return rescuer, pods
}

// newPropagator sets up spreading each pushed image to every eligible node
// from peers, or returns nil when it's turned off.
func newPropagator(registry *controller.Registry, token string, rankExclude []string, platform string) *controller.Propagator {
	if !config.Bool("PROPAGATE_ENABLED", true) {
		log.Println("angryduck-controller: propagation disabled (PROPAGATE_ENABLED=false)")
		return nil
	}
	cfg := controller.PropagatorConfig{
		Interval: config.Duration("PROPAGATE_INTERVAL_S", 10),
		// 0: until every eligible node has it, or a newer tag replaces it.
		Window:         config.Duration("PROPAGATE_WINDOW_S", 0),
		MaxConcurrent:  config.Int("PROPAGATE_MAX_CONCURRENT", 8),
		PerSource:      config.Int("PROPAGATE_PER_SOURCE", 1),
		MaxUtilization: config.Float("PROPAGATE_MAX_UTILIZATION", 0.70),
		// Same nodes preheat leaves alone (masters, typically), unless
		// set separately.
		ExcludeNodeSubstrings: config.StringSlice("PROPAGATE_EXCLUDE_NODE_SUBSTRINGS", rankExclude),
		RetryAfter:            config.Duration("RESCUE_RETRY_AFTER_S", 120),
		BackoffMax:            config.Duration("RESCUE_BACKOFF_MAX_S", 3600),
		Timeout:               config.Duration("RESCUE_TIMEOUT_S", 600),
		SeedTimeoutMin:        config.Duration("PROPAGATE_SEED_TIMEOUT_MIN_S", 120),
		SeedTimeoutMax:        config.Duration("PROPAGATE_SEED_TIMEOUT_MAX_S", 600),
		SeedTimeoutFactor:     config.Float("PROPAGATE_SEED_TIMEOUT_FACTOR", 3),
		Platform:              platform,
	}
	return controller.NewPropagator(registry, token, cfg)
}

// newSpreader sets up spreading pushed images blob by blob, or returns nil
// when it's off or can't work: it reads each image's layers from its
// registry, so it needs RANK_BY_LAYERS. Without it, preheats seed whole
// images and propagation spreads them.
func newSpreader(registry *controller.Registry, rc *registryclient.Client, token string, rankExclude []string, platform string, topN int) *controller.Spreader {
	if !config.Bool("SPREAD_ENABLED", true) {
		log.Println("angryduck-controller: blob-by-blob spreading disabled (SPREAD_ENABLED=false): seeding and propagating whole images")
		return nil
	}
	if rc == nil {
		log.Println("angryduck-controller: blob-by-blob spreading needs RANK_BY_LAYERS=true: seeding and propagating whole images")
		return nil
	}
	cfg := controller.SpreaderConfig{
		Interval:       config.Duration("PROPAGATE_INTERVAL_S", 10),
		MaxRegistry:    config.Int("SPREAD_MAX_REGISTRY_PULLS", topN),
		RacePerBlob:    config.Int("SPREAD_RACE_PER_BLOB", 2),
		PerSource:      config.Int("PROPAGATE_PER_SOURCE", 1),
		MaxUtilization: config.Float("PROPAGATE_MAX_UTILIZATION", 0.70),
		Exclude:        config.StringSlice("PROPAGATE_EXCLUDE_NODE_SUBSTRINGS", rankExclude),
		RetryAfter:     config.Duration("SPREAD_RETRY_AFTER_S", 15),
		BackoffMax:     config.Duration("SPREAD_BACKOFF_MAX_S", 600),
		// A bit longer than the worker's own limit, so the worker's report
		// normally arrives first.
		TransferTimeout: config.Duration("SPREAD_TRANSFER_TIMEOUT_S", 1800) + time.Minute,
		ResolveTimeout:  config.Duration("RANK_RESOLVE_TIMEOUT_S", 5),
		Window:          config.Duration("PROPAGATE_WINDOW_S", 0),
		Platform:        platform,
	}
	return controller.NewSpreader(registry, rc, token, cfg)
}
