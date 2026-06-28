---
name: project-nifi-http
description: NiFi runs unsecured over plain HTTP; how the apache/nifi image is overridden to do it
metadata:
  type: project
---

NiFi runs **unsecured over plain HTTP** at `http://localhost:8081/nifi` (host 8081 → container 8080), anonymous access with full read/write.

**Why this is non-obvious:** the `apache/nifi:2.9.0` image's bundled `start.sh` only ever configures HTTPS — it hard-codes `nifi.web.https.port=8443`, runs single-user secured mode, and the default `nifi.properties` ships `nifi.security.keystore=./conf/keystore.p12`. There is **no env var** to switch it to HTTP. Just setting `nifi.web.http.port` is not enough: NiFi 2.x still builds an SSL context whenever a keystore path is set, and fails with `FileNotFoundException: ./conf/keystore.p12`.

**How it's done:** `nifi/start-http.sh` (a custom ENTRYPOINT added in `nifi/Dockerfile`) `sed`-patches `conf/nifi.properties` on every boot, then runs nifi. It sets `nifi.web.http.host=0.0.0.0`, `nifi.web.http.port=8080`, and **clears** `nifi.web.https.host/port`, `nifi.security.keystore*`, `nifi.security.truststore*`, `nifi.security.user.login.identity.provider`, `nifi.web.proxy.host`, and `nifi.remote.input.secure`. Patching the live conf (not the image) means it works even on a reused `data-collector-nifi-conf` volume that still holds old secured config.

`docker-compose.yml` nifi service: no `SINGLE_USER_CREDENTIALS_*` env, port `8081:8080`, single conf volume (was a duplicate-mount bug — two volumes on `/conf`), healthcheck hits `http://localhost:8080/nifi/`.

Verified via `docker compose up -d --build nifi`: UI 200, `/nifi-api/flow/current-user` returns `anonymous:true`, healthcheck healthy.
