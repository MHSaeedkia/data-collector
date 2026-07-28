---
name: e2e-harness
description: e2e/ is a standalone Go app (`go run .`); it currently registers the Avro schemas in the schema registry.
metadata:
    type: project
---

# `e2e/` — schema registry warmup

`e2e/` is a standalone Go application (`go run .` from inside `e2e/`), not a `go test` package.
Module path is `orderbook-e2e`, so internal imports are `orderbook-e2e/<pkg>`.

## Layout

- `e2e/config/` — `config.Load(".env")` returns `Config{SchemaRegistryURL, SchemasDir}`.
- `e2e/schemaregistry/` — `schemaregistry.RegisterDir(registryURL, dir)` globs `*.avsc` from `dir`
  and POSTs each to `/subjects/{subject}/versions` as `{"schemaType":"AVRO","schema":...}`.
- `e2e/main.go` — wiring only: load config, call `RegisterDir`.

Verified 2026-07-28 against a live registry on `localhost:8082`: 4 subjects registered, ids 1–4.

## Decisions

- **Each concern is its own package, `main.go` stays wiring only.** Config and registry logic are
  packages so the remaining warmup steps (topics, payloads, assertions) can land as sibling packages
  without growing `main`.
- **Subjects are derived, not hardcoded.** The subject is the file name with `_`→`-`
  (`aggregated_order_book_event.avsc` → `aggregated-order-book-event`). A new `.avsc` needs no
  code change — but a file whose subject is not the dashed file name would register under the
  wrong subject.
- **`.env` is parsed by hand (~20 lines), no `godotenv`.** Real env vars win over the file;
  a missing `.env` is not an error.
- **Defaults**: `SCHEMA_REGISTRY_URL=http://localhost:8082`, `SCHEMAS_DIR=../schemas`
  (relative to `e2e/`). `e2e/.env` is not in the repo — only `.env.example`.
