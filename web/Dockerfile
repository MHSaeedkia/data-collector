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
FROM alpine:3.22
WORKDIR /app
COPY --from=build /orderbook-web /app/orderbook-web

# Keep the non-root posture we had with distroless.
RUN adduser -D -u 10001 app
USER app

ENV PORT=3000
EXPOSE 3000

ENTRYPOINT ["/app/orderbook-web"]
