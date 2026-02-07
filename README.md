# Arbv2 (signal-only)

Signal-only prediction-market arbitrage scanner for Kalshi ↔ Polymarket. It detects cross-venue opportunities and emits execution-intent signals. It does **not** place trades.

## Kalshi WebSocket setup (required)

Kalshi pricing now uses WebSockets only (polling removed) and requires RSA-signed headers. Set:

- `KALSHI_API_KEY` (key id)
- `KALSHI_PRIVATE_KEY_PATH` (path to PEM private key)
- `KALSHI_WS_URL` (optional; defaults to `wss://api.elections.kalshi.com/trade-api/ws/v2`)

Polymarket WS requires:

- `POLY_CLOB_API_KEY`
- `POLY_CLOB_API_SECRET`
- `POLY_CLOB_API_PASSPHRASE`

Example (PowerShell):

```powershell
$env:KALSHI_API_KEY = "your_kalshi_key"
$env:KALSHI_PRIVATE_KEY_PATH = "C:\path\to\kalshi_private_key.pem"
$env:POLY_CLOB_API_KEY = "your_poly_key"
$env:POLY_CLOB_API_SECRET = "your_poly_secret"
$env:POLY_CLOB_API_PASSPHRASE = "your_poly_pass"
```

## Run (arbv2)

```bash
python -m arbv2 ingest
python -m arbv2 match
python -m arbv2 price --stream
python -m arbv2 live
```

## Persistence telemetry (measurement only)

Persistence is tracked only for execution intents. Once an execution intent is emitted, the scanner monitors the same signal until its recomputed edge drops to break-even (<= 0). The duration is written back into `arbv2_execution_intents.csv` as `signal_duration_ms`, and `confirmation_time_ms` is recorded as `confirmed_at_ts - detected_ts`. This is measurement-only and does not affect signals.
