// Package schemas reads the Avro schemas from the repo's schemas/ directory.
// Adapter for ports.SubjectSource.
package schemas

import (
	"fmt"
	"os"
	"path/filepath"

	"orderbook-e2e/internal/domain"
)

// Dir reads schemas from a directory of .avsc files.
type Dir struct {
	Path string
}

// New points at <repoRoot>/schemas.
func New(repoRoot string) *Dir {
	return &Dir{Path: filepath.Join(repoRoot, "schemas")}
}

// Subjects returns one Subject per entry in domain.SchemaFiles.
func (d *Dir) Subjects() ([]domain.Subject, error) {
	subjects := make([]domain.Subject, 0, len(domain.SchemaFiles))
	for _, s := range domain.SchemaFiles {
		body, err := os.ReadFile(filepath.Join(d.Path, s.File))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", s.File, err)
		}
		subjects = append(subjects, domain.Subject{Name: s.Subject, Schema: string(body)})
	}
	return subjects, nil
}
