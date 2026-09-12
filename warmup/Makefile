# Test, build and run commands for warmup. Run from warmup/.
#
# `run` is what `make warmup` in the repo root calls. It runs from THIS
# directory, which is what makes .env and SCHEMA_DIR=../schemas resolve.

.DEFAULT_GOAL := help
.PHONY: help test vet fmt check build run

help: ## show this help
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

test: ## unit tests — fast, offline, no docker
	go test ./...

vet: ## go vet
	go vet ./...

fmt: ## fail if anything outside vendor/ needs gofmt
	@out=$$(gofmt -l . | grep -v '^vendor/' || true); \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	@echo "gofmt clean"

check: fmt vet test ## everything that must pass before a commit

build: ## build the binary
	go build -o warmup .

# A file target with no prerequisites: make runs it only when .env is absent, so
# an existing .env is never overwritten by a newer .env.example.
.env: ## create .env from .env.example if it does not exist
	@cp .env.example .env
	@echo "created .env from .env.example — edit it to change retention"

run: .env ## register the schemas and reconcile the topics (see .env)
	go run .
