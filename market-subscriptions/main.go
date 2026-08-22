// market-subscriptions is an operator console for turning market data feeds on and off.
//
// It reads which markets exist from postgres (exchange_markets joined to exchanges) and
// asks NiFi, over its two control-plane endpoints, to subscribe or unsubscribe them.
// This service only ever writes the PENDING status; NiFi settles the row to
// subscribe/unsubscribe once the feed is really on or off.
//
// One binary: the UI is embedded, so there is no separate frontend to deploy.
package main

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"market-subscriptions/internal/config"
	"market-subscriptions/internal/domain"
	"market-subscriptions/internal/httpapi"
	"market-subscriptions/internal/nifi"
	"market-subscriptions/internal/postgres"
)

//go:embed public
var publicFS embed.FS

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := config.Load(".env")
	client := nifi.New(nifi.Options{
		BaseURL:       cfg.NiFiBaseURL,
		SubscribePath: cfg.NiFiSubscribePath,
		UnsubPath:     cfg.NiFiUnsubPath,
		Timeout:       cfg.NiFiTimeout,
		MaxRetries:    cfg.NiFiMaxRetries,
		RetryDelay:    cfg.NiFiRetryDelay,
	})
	// Logged in full because a wrong base URL is otherwise invisible until an action
	// fails, and the failure looks like NiFi being down rather than a config typo.
	log.Info("configured",
		"port", cfg.Port,
		"subscribe_url", client.URL(domain.ActionSubscribe),
		"unsubscribe_url", client.URL(domain.ActionUnsubscribe),
		"nifi_timeout", cfg.NiFiTimeout,
		"nifi_max_retries", cfg.NiFiMaxRetries,
		"ui_refresh_seconds", cfg.UIRefreshSeconds)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo, err := postgres.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("cannot reach postgres", "err", err)
		os.Exit(1)
	}
	defer repo.Close()
	log.Info("connected to postgres")

	ui, err := fs.Sub(publicFS, "public")
	if err != nil {
		log.Error("cannot open embedded ui", "err", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           httpapi.New(repo, client, ui, cfg.UIRefreshSeconds, log).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server stopped", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown", "err", err)
	}
}
