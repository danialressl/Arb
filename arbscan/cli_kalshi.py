from __future__ import annotations

import json
import logging
from typing import Optional

from arbscan.config import AppConfig
from arbscan.http_client import get_json

logger = logging.getLogger(__name__)


def kalshi_search(
    config: AppConfig,
    query: str,
    limit: int,
    status: str,
    series_ticker: Optional[str],
    close_after: Optional[str],
    close_before: Optional[str],
    out_path: Optional[str],
) -> None:
    params = {
        "search": query,
        "limit": limit,
    }
    if status:
        params["status"] = status
    if series_ticker:
        params["series_ticker"] = series_ticker
    if close_after:
        params["close_time_min"] = close_after
    if close_before:
        params["close_time_max"] = close_before

    url = f"{config.kalshi_base_url}/markets"
    data = _get(url, params=params, timeout=config.kalshi_timeout_seconds)
    markets = data.get("markets") or data.get("data") or []
    _print_markets(markets)

    if out_path:
        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(markets, handle, indent=2)
        logger.info("Wrote %d markets to %s", len(markets), out_path)


def kalshi_series(config: AppConfig, series_ticker: str) -> None:
    url = f"{config.kalshi_base_url}/series/{series_ticker}"
    series = _get(url, timeout=config.kalshi_timeout_seconds)
    logger.info("Series: %s", series.get("title") or series_ticker)

    markets_url = f"{config.kalshi_base_url}/markets"
    data = _get(
        markets_url,
        params={"series_ticker": series_ticker, "status": "open"},
        timeout=config.kalshi_timeout_seconds,
    )
    markets = data.get("markets") or data.get("data") or []
    _print_markets(markets)


def _print_markets(markets) -> None:
    header = f"{'ticker':25} {'status':10} {'close_time':25} title"
    print(header)
    print("-" * len(header))
    for item in markets:
        ticker = item.get("ticker") or item.get("market_ticker") or item.get("id") or ""
        title = item.get("title") or item.get("question") or ""
        status = item.get("status") or ""
        close_time = item.get("close_time") or item.get("closeTime") or ""
        print(f"{ticker:25} {status:10} {close_time:25} {title}")


def _get(url: str, params: Optional[dict] = None, timeout: float = 10.0) -> dict:
    data, status = get_json(url, params=params, timeout=timeout)
    if not data or status is None:
        raise SystemExit("Kalshi request failed: no response")
    if status != 200:
        raise SystemExit(f"Kalshi request failed: {status}")
    return data
