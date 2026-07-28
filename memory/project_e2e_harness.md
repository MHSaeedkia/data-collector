---
name: e2e-harness
description: e2e/ is a standalone Go app (`go run .`); it currently registers the Avro schemas in the schema registry.
metadata:
    type: project
---

# `e2e/` — schema registry warmup

`e2e/` is a standalone Go application (`go run .` from inside `e2e/`), not a `go test` package.

## What it does

- `e2e/config.go` — `Config{SchemaRegistryURL, SchemasDir}` via `loadConfig(".env")`.
- `e2e/main.go` — `registerSchemas` globs `*.avsc` from `SchemasDir` and POSTs each to
  `/subjects/{subject}/versions` as `{"schemaType":"AVRO","schema":...}`.

Verified 2026-07-28 against a live registry on `localhost:8082`: 4 subjects registered, ids 1–4.

## Decisions

- **Subjects are derived, not hardcoded.** The subject is the file name with `_`→`-`
  (`aggregated_order_book_event.avsc` → `aggregated-order-book-event`). A new `.avsc` needs no
  code change — but a file whose subject is not the dashed file name would register under the
  wrong subject.
- **`.env` is parsed by hand (~20 lines), no `godotenv`.** Real env vars win over the file;
  a missing `.env` is not an error.
- **Defaults**: `SCHEMA_REGISTRY_URL=http://localhost:8082`, `SCHEMAS_DIR=../schemas`
  (relative to `e2e/`). `e2e/.env` is not in the repo — only `.env.example`.
