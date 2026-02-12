# arbv2 (signal-only)

Signal-only sports arbitrage scanner for Kalshi and Polymarket.

- It ingests markets, parses/matches outcomes, streams orderbooks, evaluates arb, and emits signals.
- It does **not** place orders or execute trades.

## Current Pipeline

`ingest -> match -> price stream (WS) -> arb scan -> confirm gates -> output files`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

PowerShell activate:

```powershell
.venv\Scripts\Activate.ps1
```

## Required Environment Variables (WS)

Kalshi WS auth:

- `KALSHI_API_KEY` (or `KALSHI_ACCESS_KEY`)
- `KALSHI_PRIVATE_KEY_PATH`

Polymarket CLOB auth:

- `POLY_CLOB_API_KEY`
- `POLY_CLOB_API_SECRET`
- `POLY_CLOB_API_PASSPHRASE`

Optional endpoints/defaults:

- `KALSHI_BASE_URL` default `https://api.elections.kalshi.com/trade-api/v2`
- `KALSHI_WS_URL` default `wss://api.elections.kalshi.com/`
  - host-only value is normalized to `/trade-api/ws/v2`
- `POLYMARKET_BASE_URL` default `https://gamma-api.polymarket.com`
- `POLY_CLOB_REST_URL` default `https://clob.polymarket.com`
- `POLY_CLOB_WS_URL` default `wss://ws-subscriptions-clob.polymarket.com`
- `ARBV2_DB_PATH` default `arbv2.db`

PowerShell example:

```powershell
$env:KALSHI_API_KEY = "your_kalshi_key"
$env:KALSHI_PRIVATE_KEY_PATH = "C:\path\to\kalshi_private_key.pem"
$env:POLY_CLOB_API_KEY = "your_poly_key"
$env:POLY_CLOB_API_SECRET = "your_poly_secret"
$env:POLY_CLOB_API_PASSPHRASE = "your_poly_pass"
```

## CLI Commands

```bash
python -m arbv2 ingest
python -m arbv2 match
python -m arbv2 health
python -m arbv2 price --stream
python -m arbv2 live
```

Also available:

- `python -m arbv2 debug <kalshi_market_id> <polymarket_market_id>`
- `python -m arbv2 arb --mode EVENT_OUTCOME|BINARY_MIRROR`
- `python -m arbv2 run --interval-seconds 3600` (ingest+match loop only)

Notes:

- `live` currently purges `arbv2.db`, `arbv2.db-wal`, `arbv2.db-shm`, and `arbv2.db-journal` at startup.
- `price --stream` and `live` run websocket-only pricing paths.
- Kalshi snapshot polling is not used; use `--stream`.

## Runtime Behavior

- Price writes are queued and flushed asynchronously to SQLite.
- Arb scanning is event-driven from orderbook updates (not fixed-interval polling).
- Confirm/post-confirm gates are stream-health-first (heartbeat/subscription/consistency checks).
- `pending_signals` table persists active confirmed signals used for signal-duration backfill.
- On each `match`, orphaned rows are pruned from:
  - `prices` (markets no longer in `matches`)
  - `arb_scans` (pairs no longer in `matches`)

## Outputs

### CSV

- `arbv2_execution_intents.csv`
  - includes `time_to_confirm_ms` and `signal_duration_ms`
- `arbv2_confirm_rejections.csv`
  - only created/written when rejects occur

### SQLite (`arbv2.db`)

Core tables:

- `markets`
- `predicates`
- `matches`
- `prices`
- `arb_scans`
- `pending_signals`

## Health Command

`python -m arbv2 health` logs:

- table counts
- latest timestamps
- orphan counts
- coverage (`matched_pairs_with_both_prices`, `scan_pairs_present`)
- execution intent duration stats (p50/p90/p99, threshold buckets)
