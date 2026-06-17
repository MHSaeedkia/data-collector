# Market Sync

A simple CLI tool to bulk subscribe or unsubscribe markets across exchanges via HTTP requests.

---

## File Structure

```
markets/
├── market-sync.sh   # Main script
├── markets.csv      # Market definitions
└── README.md
```

---

## markets.csv Format

```csv
exchange,market,action
nobitex,BTCUSDT,subscribe
nobitex,ETHUSDT,subscribe
nobitex,XRPUSDT,unsubscribe
binance,BTCUSDT,subscribe
```

| Column | Description |
|---|---|
| `exchange` | Exchange name (e.g. `nobitex`, `binance`) |
| `market` | Market pair (e.g. `BTCUSDT`) |
| `action` | `subscribe` or `unsubscribe` |

---

## Usage

```bash
chmod +x market-sync.sh

# Normal mode — use actions as defined in markets.csv
./market-sync.sh

# Reverse mode — flip every action (subscribe → unsubscribe and vice versa)
./market-sync.sh --reverse
./market-sync.sh -r
```

---

## Configuration

Edit the variables at the top of `market-sync.sh`:

| Variable | Default | Description |
|---|---|---|
| `BASE_URL` | `http://localhost:8081/control-plane` | API base URL |
| `MAX_RETRIES` | `3` | Number of retry attempts on failure |
| `RETRY_DELAY` | `2` | Seconds to wait between retries |

---

## Output

```
[INFO]   Starting market sync from: markets.csv
────────────────────────────────────────────
[OK]     [subscribe] nobitex / BTCUSDT  (HTTP 200)
[OK]     [subscribe] nobitex / ETHUSDT  (HTTP 200)
[WARN]   [unsubscribe] nobitex / XRPUSDT  → HTTP 503 — retrying (1/3)...
[OK]     [unsubscribe] nobitex / XRPUSDT  (HTTP 200)
[FAIL]   [subscribe] binance / BTCUSDT  → failed after 3 attempts (last HTTP: 500)
────────────────────────────────────────────
  Total:   4
  Success: 3
  Failed:  1
────────────────────────────────────────────
```

---

## Notes

- The script processes markets sequentially in the order they appear in `markets.csv`.
- Empty lines and the header row are automatically skipped.
- A request is considered successful on any `2xx` HTTP response.
