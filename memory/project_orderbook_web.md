---
name: orderbook-web
description: Standalone Node.js web app that consumes consolidated Kafka topics and renders a live order book in the browser
metadata:
  type: project
---

## Live order book web UI (`web/`)

Standalone Node.js app that consumes the Flink job's consolidated output topics
(`{pair}-{side}`, e.g. `BTC-USDT-asks`/`BTC-USDT-bids`; see [[orderbook-aggregation]]) and
shows a live order book in the browser. NOT dockerized — runs on the host against the
exposed broker `localhost:9092`.

## Stack & shape (decisions)

- **Node.js + Express + kafkajs + ws** (chosen over Python/Java for a simple app).
- **Multiple pairs selectable** (dropdown), both sides each — full asks/bids book per pair.
- Browsers can't read Kafka directly, so: kafkajs consumer → keep latest book per topic in
  memory → push to browser over **WebSocket**. Express serves one static `public/index.html`.

## Files

- `web/server.js` — Express static server + `ws` WebSocketServer + kafkajs consumer.
  HTTP server starts FIRST (UI loads even if Kafka is down); consumer runs separately and
  logs (does not exit) on Kafka error.
- `web/public/index.html` — single-page UI (vanilla JS), pair dropdown, asks/bids tables,
  spread, live WS updates with auto-reconnect.
- `web/package.json`, `web/.gitignore`, `web/README.md`.

## Key implementation notes

- Subscribes via regex `^.+-(asks|bids)$` → matches only the consolidated OUTPUT topics;
  input topics `{pair}-{side}-{exchange}` end in an exchange name so they don't match.
- Fresh consumer group each start (`orderbook-web-${Date.now()}`) + `fromBeginning: true`,
  so the current book renders on load. Replays history each restart — fine for dev only.
- New pairs added after server start are only picked up on restart (kafkajs matches the
  regex against topics existing at subscribe time).
- Config via env: `PORT` (default 3000), `KAFKA_BROKER` (default `localhost:9092`).

## Run

`cd web && npm install && npm start` → http://localhost:3000
