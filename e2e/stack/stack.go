// Package stack recreates the docker compose stack the harness runs against.
package stack

import (
	"context"
	"fmt"
	"log"
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
	log.Print("recreating the docker compose stack...")

	if err := compose(ctx, composeFile, "down", "-v"); err != nil {
		return err
	}
	return compose(ctx, composeFile, "up", "-d", "--wait")
}

func compose(ctx context.Context, composeFile string, args ...string) error {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", composeFile}, args...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %s: %w", args[0], err)
	}
	return nil
}
