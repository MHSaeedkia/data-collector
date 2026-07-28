// Package jobs builds the six normalizer job jars in a container and hands
// them back as bytes. Adapter for ports.JarSource.
//
// Nothing here touches the host toolchain: CI has no JDK and no Maven, so the
// build happens inside flink/normalizer/Dockerfile.jobs and the jars are copied
// out of a throwaway container.
package jobs

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"orderbook-e2e/internal/domain"
)

const (
	imageRepo = "tibobit-e2e-jobs"
	imageTag  = "latest"
	jarDir    = "/jars"
)

// Builder builds the jars once and serves the same bytes afterwards. The
// stack is recreated per scenario but the jars are not: a cold Maven build
// pulls the whole Flink dependency closure, and paying that per scenario would
// dominate the run.
type Builder struct {
	contextDir string

	mu   sync.Mutex
	jars []domain.Jar
}

// NewBuilder builds from <repoRoot>/flink/normalizer.
func NewBuilder(repoRoot string) *Builder {
	return &Builder{contextDir: filepath.Join(repoRoot, "flink", "normalizer")}
}

// Jars returns the built job jars, in domain.JobModules order.
func (b *Builder) Jars(ctx context.Context) ([]domain.Jar, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.jars != nil {
		return b.jars, nil
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			FromDockerfile: testcontainers.FromDockerfile{
				Context:    b.contextDir,
				Dockerfile: "Dockerfile.jobs",
				Repo:       imageRepo,
				Tag:        imageTag,
				// Keep the image so the Docker layer cache makes the next run
				// (and the next scenario) cheap.
				KeepImage: true,
			},
			WaitingFor: wait.ForLog("jars-ready"),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("build jars image: %w", err)
	}
	defer func() { _ = container.Terminate(context.WithoutCancel(ctx)) }()

	jars := make([]domain.Jar, 0, len(domain.JobModules))
	for _, module := range domain.JobModules {
		path := jarDir + "/" + module + ".jar"

		reader, err := container.CopyFileFromContainer(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("copy %s: %w", path, err)
		}
		body, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}

		jars = append(jars, domain.Jar{Name: module, Bytes: body})
	}

	b.jars = jars
	return b.jars, nil
}
