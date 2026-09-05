package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"orderbook-e2e/config"
	"orderbook-e2e/scenario"
	"orderbook-e2e/server"
	"orderbook-e2e/stack"
)

// @title			Orderbook E2E Harness API
// @version		1.0
// @description	Runs one end-to-end scenario against the normalizer pipeline: the stack is provisioned once when the server starts, then every request warms the pipeline up for its own exchange/pair, feeds the raw topic and checks what came back out.
// @description
// @description	The same `Scenario` the compiled-in cases use, posted as JSON. Regenerate this spec with `swag init -g main.go -o docs` after changing the handler or the scenario struct.
// @host			localhost:9595
// @BasePath		/
func main() {
	serve := flag.Bool("serve", false, "serve scenarios over HTTP instead of running the built-in list")
	addr := flag.String("addr", ":9595", "address the -serve listener binds to")
	provisionStack := flag.Bool("provision-stack", true, "provision stack using `docker compose up -d` command")
	logLevel := flag.String("log-level", "info", "log verbosity: debug, info, warn or error")

	flag.Parse()

	if err := setupLogger(*logLevel); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	cfg, err := config.Load(".env")
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()

	if provisionStack != nil && *provisionStack {
		if err := stack.Provision(ctx, cfg.ComposeFile); err != nil {
			slog.Error("provision stack", "err", err)
			os.Exit(1)
		}
	}

	if *serve {
		runServer(cfg, *addr)
		return
	}

	// One failure does not stop the run: a suite this slow is only worth waiting
	// on if it reports every case it can, so failures are collected and listed
	// at the end.
	var failed []string
	for i, sc := range scenario.Scenarios {
		slog.Info("scenario start", "n", i+1, "of", len(scenario.Scenarios), "name", sc.Name)
		start := time.Now()
		if err := scenario.Run(ctx, cfg, sc.S); err != nil {
			slog.Error("scenario FAIL", "name", sc.Name, "took", took(start), "err", err)
			failed = append(failed, sc.Name)
		} else {
			slog.Info("scenario PASS", "name", sc.Name, "took", took(start))
		}
	}

	if len(failed) > 0 {
		slog.Error("run finished with failures", "failed", len(failed), "of", len(scenario.Scenarios), "names", failed)
		os.Exit(1)
	}
	slog.Info("run finished", "passed", len(scenario.Scenarios))
}

// setupLogger installs the process logger. The harness is a console tool, so
// this is text on stderr; the timestamp is clock-only because a run takes
// minutes and what matters between two lines is the elapsed time, not the date.
func setupLogger(level string) error {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return fmt.Errorf("invalid -log-level %q: %w", level, err)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format(time.TimeOnly))
			}
			return a
		},
	})))
	return nil
}

func took(start time.Time) time.Duration {
	return time.Since(start).Round(time.Second)
}

// runServer blocks serving scenarios over HTTP. There is no write timeout: a
// run warms the pipeline up and then waits a minute on the snapshot topic, so
// the response is minutes away and the server must not cut it off. Only the
// header read is bounded.
func runServer(cfg config.Config, addr string) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.New(cfg),
		ReadHeaderTimeout: 10 * time.Second,
	}
	slog.Info("listening", "addr", addr, "endpoint", "POST /scenarios/run")
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
