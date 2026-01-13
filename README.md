# Arbscan

Notification-only prediction-market arbitrage scanner. It detects cross-venue "true arb" opportunities and emits alerts to console and optional Slack. It does **not** place trades.

## What this is / isn't
- Scans mapped events for YES/NO cross-venue arb
- Sends notifications with prices, sizes, fees, slippage, and margins
- No trading or execution logic
- No fuzzy market discovery (only mapped events)

## Setup

Python 3.11+ is required.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` if needed (optional). Defaults are in `arbscan/config.py`.

## Run

Mock mode:

```bash
python -m arbscan run --mock
```

Semantic matching workflow (event-level):

```bash
python -m arbscan scan-catalog
python -m arbscan debug-db
python -m arbscan review --min-score 0.85 --out review_matches.csv
python -m arbscan run-live
python -m arbscan explain-event-match KXFEDDECISION-26JAN fed-decision-in-january
python -m arbscan explain-outcome-match KXFEDDECISION-26JAN-NOCHANGE POLY-NOCHANGE
```

Legacy mappings workflow (manual):

```bash
python -m arbscan validate-mappings
python -m arbscan run
```

Discovery commands:

```bash
python -m arbscan kalshi-search --query "election" --limit 50 --out kalshi_markets.json
python -m arbscan kalshi-series --ticker "ELECTIONS-24"
python -m arbscan polymarket-search --query "election" --limit 50 --out polymarket_markets.json
python -m arbscan generate-mappings --kalshi kalshi_markets.json --polymarket polymarket_markets.json --out mappings.skeleton.yml
```

## Mappings (legacy)
Define only the events you want to scan in `mappings.yml`:

```yaml
events:
  - canonical_event_id: "event-001"
    notes: "Resolution equivalence notes"
    venues:
      kalshi:
        ticker: "ELECTIONS-24-PRES"
      polymarket:
        market_id: "123456"
        yes_token_id: "987654"
        no_token_id: "987655"
```

**Important:** mapping correctness is on you. The scanner assumes the markets resolve to the same outcome.

## Arbitrage computation
For a binary event, it checks buying YES on venue A at ask price `pA` and NO on venue B at ask price `qB`.

```
(pA + feeA*pA + slippageA) + (qB + feeB*qB + slippageB) < 1.0
```

Only alerts when net margin (absolute per contract) exceeds `ARB_THRESHOLD` (default 0.01).

Costs are absolute per contract:

```
total_cost = p_yes + p_no + fee_yes + fee_no + slippage_yes + slippage_no
```

Kalshi orderbooks are bid-centric. If no explicit ask is present, we compute implied asks:

```
YES_ask = 1 - NO_bid
NO_ask  = 1 - YES_bid
```

## Semantic matching
`scan-catalog` builds a local SQLite DB (`ARBSCAN_DB_PATH`) with event groups and outcome markets, then scores Kalshi/Polymarket equivalence. It uses sentence-transformers if installed, otherwise falls back to text similarity.

Optional embeddings:

```bash
pip install sentence-transformers
```

Optional LLM extraction (uses `OPENAI_API_KEY` if set):
- When set, the scanner calls OpenAI to extract structured resolution specs.

## Notifications
Alerts include:
- event canonical id
- venue market ids and outcome mapping
- best executable prices and depth
- fees and slippage assumptions
- net margin (absolute and percent)
- timestamp and unique alert id

Slack: set `SLACK_WEBHOOK_URL`.

## Limitless connector
`arbscan/connectors/limitless_stub.py` is a stub with TODOs:
- identify on-chain sources or official APIs
- build indexing for best bid/ask
- add auth/rate limiting

## Mock mode
Mock data lives in `arbscan/data/*.json`. The sample dataset is designed to produce at least one alert when you run in mock mode.

## Live setup

Polymarket:
- Uses public WS market channel (no auth required).
- Configure `POLYMARKET_REST_URL` and `POLYMARKET_WS_URL` if defaults change.
- Discovery uses `POLYMARKET_GAMMA_URL`.

Kalshi:
- Public endpoints are used when possible.
- If signed requests are required, set `KALSHI_SIGNING_MODE=ed25519`, `KALSHI_API_KEY`, and `KALSHI_PRIVATE_KEY_PATH`.
- Requires `cryptography` when using signed requests.
- Base URL is `KALSHI_BASE_URL` (default `https://api.elections.kalshi.com/trade-api/v2`).

Limitless:
- Set `LIMITLESS_ADAPTER=http` and `LIMITLESS_HTTP_URL` if an official endpoint exists.
- `LIMITLESS_ADAPTER=onchain` raises a clear error until contracts/indexer are provided.

Common config:
- `SCAN_INTERVAL_SECONDS`, `ARB_THRESHOLD`, `DEDUPE_SECONDS`, `MARGIN_STEP_RESEND`, `SIZE_STEP_RESEND`
- `MIN_SIZE`, `MATCH_THRESHOLD`, `INCLUDE_REVIEW`, `ARBSCAN_DB_PATH`
- `POLYMARKET_SCAN_LIMIT`, `POLYMARKET_SCAN_PAGE_SIZE`, `KALSHI_SCAN_LIMIT`, `MATCH_CANDIDATE_LIMIT`
- `FEE_POLYMARKET`, `FEE_KALSHI`, `FEE_LIMITLESS`
- `SLIPPAGE_POLYMARKET`, `SLIPPAGE_KALSHI`, `SLIPPAGE_LIMITLESS`

## Limitations
- Mapping correctness is on the user
- Live connectors depend on venue API stability and payload formats
- No execution/trading logic
