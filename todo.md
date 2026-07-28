# TODO

## e2e

- [x] Schema registry warmup — load `schemas/*.avsc`, register each in the registry (2026-07-28, verified live: 4 subjects, ids 1–4)
- [x] Split into packages — `e2e/config/` and `e2e/schemaregistry/`, `main.go` is wiring only (2026-07-28, verified live)
