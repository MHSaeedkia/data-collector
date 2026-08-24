package main

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The go:embed + fs.Sub wiring is the one seam the package tests cannot reach: a UI file
// renamed or moved out of public/ would still compile and only fail as a blank page.
func TestEmbeddedUIIsServable(t *testing.T) {
	ui, err := fs.Sub(publicFS, "public")
	require.NoError(t, err)

	page, err := fs.ReadFile(ui, "index.html")
	require.NoError(t, err, "public/index.html must be embedded and served at /")

	html := string(page)
	assert.Contains(t, html, "<title>Market Subscriptions</title>")
	// The endpoints the page calls must exist in Routes(); this catches a rename on
	// either side.
	for _, endpoint := range []string{"/api/subscriptions", "/api/exchanges", "/api/actions", "/api/config"} {
		assert.True(t, strings.Contains(html, endpoint), "UI should call %s", endpoint)
	}
}
