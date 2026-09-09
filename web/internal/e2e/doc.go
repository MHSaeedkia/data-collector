// Package e2e holds black-box tests that drive the real web image against a
// real Kafka, in a throwaway docker compose stack of their own.
//
// Everything here is behind the `e2e` build tag, so `go test ./...` stays fast
// and needs no docker. Run these with:
//
//	make e2e          # from web/
//	go test -tags e2e -count=1 -v -timeout 20m ./internal/e2e
//
// This file carries no build tag on purpose: without one non-test file the
// directory has no buildable package and `go build ./...` fails.
package e2e
