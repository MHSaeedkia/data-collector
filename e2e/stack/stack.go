// Package stack recreates the docker compose stack the harness runs against.
package stack

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
)

// Provision tears the stack down together with its volumes and brings it back
// up, so a run starts from an empty broker, an empty schema registry and a
// database freshly seeded from postgres/. `up --wait` returns only once every
// service that has a healthcheck is healthy, so nothing downstream talks to a
// half-started broker. Images are not rebuilt: a missing one is built by `up`,
// and the job jars come from the harness's own `mvn` build, not the image.
func Provision(ctx context.Context, composeFile string) error {
	slog.Info("recreating the docker compose stack")

	if err := compose(ctx, composeFile, "down", "-v"); err != nil {
		return err
	}
	return compose(ctx, composeFile, "up", "-d", "--wait")
}

// compose runs one docker compose command. Its output is only worth reading
// when it fails, so it is held back and replayed with the error — unless debug
// is on, where watching a slow `up --wait` progress live is the point.
func compose(ctx context.Context, composeFile string, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", composeFile}, args...)...)

	var out bytes.Buffer
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	} else {
		cmd.Stdout, cmd.Stderr = &out, &out
	}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %s: %w\n%s", args[0], err, out.String())
	}
	return nil
}
