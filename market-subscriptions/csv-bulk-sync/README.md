# csv-bulk-sync

The original CSV-driven bulk subscribe/unsubscribe CLI, kept for scripted runs.

**For day-to-day work use the console instead** — see [../README.md](../README.md). The
console reads markets from postgres, which is the source of truth; the CSV here is a
separate list that can and does drift from the database.

---

## Relationship to the console

|  | `csv-bulk-sync` | `market-subscriptions` console |
|---|---|---|
| Market list from | `markets.csv` | postgres `exchange_markets` |
| Writes `status` | no | yes (`pending-*` only) |
| Shows current state | no | yes |
| Good for | one big scripted sweep | day-to-day, and seeing what is on |

Both call the same two NiFi endpoints with the same `{"exchange": ..., "market": ...}`
payload, so they are interchangeable from NiFi's point of view. The difference is that the
console records the request in the database and the script does not.

⚠ `markets.csv` currently holds 364 rows, **all marked `disable`**, so a plain run sends
nothing at all.

---

## markets.csv format

```csv
exchange,market,action
nobitex,BTCUSDT,subscribe
nobitex,XRPUSDT,unsubscribe
bitpin,BTC_USDT,disable
```

| Column | Description |
|---|---|
| `exchange` | exchange name, matching `exchanges.name` |
| `market` | the exchange's own symbol, matching `exchange_markets.market` |
| `action` | `subscribe`, `unsubscribe`, or `disable` (skipped entirely) |

## Usage

```bash
./market-sync.sh              # apply the actions in markets.csv
./market-sync.sh --reverse    # flip every action
```

## Configuration

Edit the variables at the top of `market-sync.sh`:

| Variable | Default | Description |
|---|---|---|
| `BASE_URL` | `http://localhost:8081/control-plane` | API base URL |
| `MAX_RETRIES` | `3` | retry attempts per market |
| `RETRY_DELAY` | `2` | seconds between retries |

Unlike the console, these are **not** read from `.env`.

## Notes

- Markets are processed sequentially, in file order.
- Any `2xx` counts as success.
- The header row and blank lines are skipped.
