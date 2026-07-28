// Package repo locates the repository root from inside a test binary.
//
// The harness needs files that live outside its own Go module — the Avro
// schemas under schemas/, the Postgres init scripts under postgres/, and the
// two Dockerfiles under flink/normalizer/ — so paths cannot be resolved
// relative to the working directory, which is whichever package `go test`
// happens to be running.
package repo

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// Root walks up from this source file until it finds the repository root,
// identified by the normalizer's pom.xml.
func Root() (string, error) {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("cannot determine caller source path")
	}

	dir := filepath.Dir(self)
	for {
		if _, err := os.Stat(filepath.Join(dir, "flink", "normalizer", "pom.xml")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("repository root not found above %s", filepath.Dir(self))
		}
		dir = parent
	}
}
