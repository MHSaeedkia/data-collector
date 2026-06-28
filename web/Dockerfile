# syntax=docker/dockerfile:1

# --- build stage ---
FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies are vendored (web/vendor), so the image builds fully offline — no
# module proxy needed. The UI (public/) is baked in via go:embed, so the result is a
# single self-contained static executable.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -mod=vendor -o /orderbook-web .

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /orderbook-web /app/orderbook-web

ENV PORT=3000
EXPOSE 3000

ENTRYPOINT ["/app/orderbook-web"]
