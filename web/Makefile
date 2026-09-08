# Test and build commands for the web service. Run from web/.
#
# The e2e targets stand apart from everything else: they need docker, they take
# minutes, and they build the real image. Everything above them is fast, offline
# and safe to run on every save.

COMPOSE_E2E := docker compose -p $(or $(WEB_E2E_PROJECT),web-e2e) -f internal/e2e/docker-compose.e2e.yml

.DEFAULT_GOAL := help
.PHONY: help test race vet fmt check e2e e2e-clean run build

help: ## show this help
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

test: ## unit tests — fast, offline, no docker
	go test ./...

race: ## unit tests under the race detector, no caching
	go test -race -count=1 ./...

vet: ## go vet, including the e2e-tagged files
	go vet ./...
	go vet -tags e2e ./internal/e2e/

fmt: ## fail if anything outside vendor/ needs gofmt
	@out=$$(gofmt -l . | grep -v '^vendor/' || true); \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	@echo "gofmt clean"

check: fmt vet race ## everything that must pass before a commit

e2e: ## black-box tests against a THROWAWAY kafka+web stack (needs docker, ~5 min)
	go test -tags e2e -count=1 -v -timeout 20m ./internal/e2e/

e2e-clean: ## remove a stack left behind by an interrupted e2e run
	-$(COMPOSE_E2E) down -v --remove-orphans --timeout 10

build: ## build the binary
	go build -o orderbook-web .

run: ## run against whatever the env points at (see .env.example)
	go run .
