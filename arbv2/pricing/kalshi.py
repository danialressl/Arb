import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import requests

from arbv2.config import Config
from arbv2.models import Market, PriceSnapshot
from arbv2.pricing.arb import update_orderbook
from arbv2.storage import insert_prices


logger = logging.getLogger(__name__)


async def poll_markets(
    config: Config,
    markets: List[Market],
    db_path: str,
    *,
    poll_seconds: int,
    run_once: bool = False,
) -> None:
    tickers = [m.market_id for m in markets]
    market_by_id = {m.market_id: m for m in markets}
    while True:
        snapshots = await asyncio.to_thread(_fetch_markets, config, tickers, market_by_id)
        if snapshots:
            insert_prices(db_path, snapshots)
            logger.info("Kalshi market updates: %d", len(snapshots))
        if run_once:
            break
        await asyncio.sleep(poll_seconds)


def _fetch_markets(
    config: Config,
    tickers: List[str],
    market_by_id: Dict[str, Market],
) -> List[PriceSnapshot]:
    url = config.kalshi_base_url.rstrip("/") + "/markets"
    snapshots: List[PriceSnapshot] = []
    for batch in _chunks(tickers, 200):
        snapshots.extend(_fetch_batch(config, url, batch, market_by_id))
    return snapshots


def _fetch_batch(
    config: Config,
    url: str,
    batch: List[str],
    market_by_id: Dict[str, Market],
) -> List[PriceSnapshot]:
    params = {
        "tickers": ",".join(batch),
        "status": "open",
        "mve_filter": "exclude",
        "limit": 1000,
    }
    try:
        resp = requests.get(url, params=params, timeout=config.http_timeout_seconds)
    except requests.RequestException as exc:
        logger.warning("Kalshi markets request failed: %s", exc)
        return []
    if resp.status_code == 413 and len(batch) > 1:
        mid = len(batch) // 2
        left = _fetch_batch(config, url, batch[:mid], market_by_id)
        right = _fetch_batch(config, url, batch[mid:], market_by_id)
        return left + right
    if resp.status_code != 200:
        logger.warning("Kalshi markets error %s", resp.status_code)
        return []
    try:
        data = resp.json()
    except json.JSONDecodeError:
        logger.warning("Kalshi markets response not JSON")
        return []
    snapshots: List[PriceSnapshot] = []
    for item in data.get("markets") or []:
        ticker = item.get("ticker")
        if not ticker or ticker not in market_by_id:
            continue
        best_bid = _parse_float(item.get("yes_bid_dollars"))
        best_ask = _parse_float(item.get("yes_ask_dollars"))
        no_bid = _parse_float(item.get("no_bid_dollars"))
        no_ask = _parse_float(item.get("no_ask_dollars"))
        qty = _parse_float(item.get("open_interest")) or 0.0
        ts_utc = _now_utc()
        ts_dt = datetime.fromisoformat(ts_utc)
        if qty > 0 and best_bid is not None and best_ask is not None:
            update_orderbook(
                "kalshi",
                ticker,
                "YES",
                bids=[(best_bid, qty)],
                asks=[(best_ask, qty)],
                last_update_ts_utc=ts_dt,
            )
            outcome_label = market_by_id[ticker].outcome_label
            if outcome_label:
                update_orderbook(
                    "kalshi",
                    ticker,
                    outcome_label,
                    bids=[(best_bid, qty)],
                    asks=[(best_ask, qty)],
                    last_update_ts_utc=ts_dt,
                )
        if qty > 0 and no_bid is not None and no_ask is not None:
            update_orderbook(
                "kalshi",
                ticker,
                "NO",
                bids=[(no_bid, qty)],
                asks=[(no_ask, qty)],
                last_update_ts_utc=ts_dt,
            )
        snapshots.append(
            PriceSnapshot(
                venue="kalshi",
                market_id=ticker,
                outcome_label=market_by_id[ticker].outcome_label or "",
                best_bid=best_bid,
                best_ask=best_ask,
                last_trade=None,
                ts_utc=ts_utc,
                raw_json=None,
            )
        )
    return snapshots


def _parse_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _chunks(items: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()
