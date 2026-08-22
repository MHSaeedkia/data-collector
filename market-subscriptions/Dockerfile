# syntax=docker/dockerfile:1

# --- test stage ---
# Dependencies are vendored, so the image builds fully offline — no module proxy needed.
FROM golang:1.26-alpine AS test
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go test -mod=vendor ./...

# --- build stage ---
# Chained from test (not a fresh golang image) so BuildKit is forced to run the test
# stage — an unreferenced stage would otherwise be skipped. The UI (public/) is baked in
# via go:embed, so the result is a single self-contained static executable.
FROM test AS build
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -mod=vendor -o /market-subscriptions .

# --- runtime stage ---
FROM alpine:3.22
WORKDIR /app
COPY --from=build /market-subscriptions /app/market-subscriptions

RUN adduser -D -u 10001 app
USER app

EXPOSE 8090
ENTRYPOINT ["/app/market-subscriptions"]
