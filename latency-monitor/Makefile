# Test, build and run commands for latency-monitor. Run from latency-monitor/.
#
# Running from THIS directory is what makes .env and the tests' ../schemas path
# resolve.

.DEFAULT_GOAL := help
.PHONY: help test vet fmt check build run

TOPIC ?= ex1-p1-orderbook-snapshot-flink

help: ## show this help
	@grep -hE '^[a-z0-9-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

test: ## unit tests — fast, offline, no docker
	go test ./...

vet: ## go vet
	go vet ./...

fmt: ## fail if anything outside vendor/ needs gofmt
	@out=$$(gofmt -l . | grep -v '^vendor/' || true); \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

check: fmt vet test ## everything CI would run

build: ## build the binary into ./latency-monitor
	go build -o latency-monitor .

# A file target with no prerequisites: make runs it only when .env is absent, so
# an existing .env is never overwritten by a newer .env.example.
.env: ## create .env from .env.example if it does not exist
	@cp .env.example .env
	@echo "created .env from .env.example — edit it to change the endpoints"

run: .env ## tail one topic: make run TOPIC=p1-asks
	go run . $(TOPIC)
