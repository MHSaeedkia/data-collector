---
name: project-nifi-https
description: NiFi runs HTTPS single-user on 8443; the apache/nifi 2.x SNI host-check and the NIFI_WEB_PROXY_HOST fix
metadata:
  type: project
---

NiFi runs **HTTPS single-user secured** at `https://<host>:8443/nifi` using the stock `apache/nifi:2.9.0` image. `nifi/Dockerfile` only adds the postgres JDBC driver — there is **no custom entrypoint** (a prior plain-HTTP `start-http.sh` approach was reverted; do not assume it exists). The nifi service (defined in both `docker-compose-orderbook-job.yml` and `docker-compose-orderbook-consolidator.yml`) maps `8443:8443` and `8081:8081`, healthcheck hits `https://$(hostname):8443/nifi-api/system-diagnostics`.

**SNI gotcha (non-obvious):** apache/nifi 2.x Jetty enforces a strict SNI / host-header allowlist via `SecureRequestCustomizer`. By default only `localhost`/`127.0.0.1`/the container hostname are accepted. Hitting NiFi by raw server IP (e.g. `https://192.168.150.104:8443/nifi`) returns `HTTP 400 Invalid SNI` — `localhost` works, the IP does not, which is the tell. It is a host-allowlist rejection, not a broken TLS cert.

**Fix:** add every host that appears in the browser address bar (with port) to `nifi.web.proxy.host`. With the apache/nifi image this is the `NIFI_WEB_PROXY_HOST` env var (image's `start.sh` does `prop_replace 'nifi.web.proxy.host'`). Set in docker-compose nifi `environment:` e.g. `NIFI_WEB_PROXY_HOST: "192.168.150.104:8443,localhost:8443"`, then `docker compose up -d nifi`. Add any DNS name / reverse-proxy host to the same comma list. Setting it makes it the allowlist, so keep `localhost:8443` in the list.
