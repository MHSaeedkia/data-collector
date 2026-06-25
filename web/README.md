# Order Book Web UI

Live viewer for the consolidated order book topics produced by the Flink job
(`{pair}-{side}`, e.g. `BTC-USDT-asks`, `BTC-USDT-bids`).

A small Node server consumes those topics, keeps the latest book per topic, and pushes
updates to the browser over WebSocket. The page has a pair dropdown and renders asks/bids
live.

## Run

```bash
cd web
npm install
npm start
```

Then open http://localhost:3000.

## Config (env vars)

- `PORT` — HTTP port (default `3000`)
- `KAFKA_BROKER` — broker address (default `localhost:9092`, the host-exposed listener)

## Notes

- Subscribes via regex `^.+-(asks|bids)$`, so it reads only the consolidated **output**
  topics — input topics (`{pair}-{side}-{exchange}`) end in an exchange name and don't match.
- Uses a fresh consumer group each start and reads `fromBeginning`, so the current book shows
  on load. Fine for dev; for high-volume topics this replays history on every restart.
- New pairs created after the server starts are picked up on restart (kafkajs matches the
  regex against topics that exist at subscribe time).
