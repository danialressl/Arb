from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Iterable, List, Optional

import requests
import yaml

from arbscan.config import AppConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketInfo:
    venue: str
    market_id: str
    title: str
    close_time: Optional[datetime]


def write_suggestions(config: AppConfig, output_path, limit: int = 50) -> None:
    poly_markets = _fetch_polymarket_markets(config)
    kalshi_markets = _fetch_kalshi_markets(config)

    suggestions = suggest_matches(poly_markets, kalshi_markets, limit=limit)
    payload = {
        "suggestions": [
            {
                "polymarket_market_id": s.polymarket.market_id,
                "kalshi_market_id": s.kalshi.market_id,
                "title_polymarket": s.polymarket.title,
                "title_kalshi": s.kalshi.title,
                "close_time_polymarket": s.polymarket.close_time.isoformat()
                if s.polymarket.close_time
                else None,
                "close_time_kalshi": s.kalshi.close_time.isoformat() if s.kalshi.close_time else None,
                "similarity": s.similarity,
                "notes": "Review before adding to mappings.yml",
            }
            for s in suggestions
        ]
    }

    with open(output_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)

    logger.info("Wrote %d suggestions to %s", len(suggestions), output_path)


@dataclass(frozen=True)
class Suggestion:
    polymarket: MarketInfo
    kalshi: MarketInfo
    similarity: float


def suggest_matches(
    poly_markets: Iterable[MarketInfo],
    kalshi_markets: Iterable[MarketInfo],
    limit: int = 50,
) -> List[Suggestion]:
    suggestions: List[Suggestion] = []
    for poly in poly_markets:
        for kalshi in kalshi_markets:
            similarity = _title_similarity(poly.title, kalshi.title)
            if similarity < 0.6:
                continue
            if not _date_close(poly.close_time, kalshi.close_time):
                continue
            suggestions.append(Suggestion(polymarket=poly, kalshi=kalshi, similarity=similarity))

    suggestions.sort(key=lambda s: s.similarity, reverse=True)
    return suggestions[:limit]


def _title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _date_close(a: Optional[datetime], b: Optional[datetime]) -> bool:
    if not a or not b:
        return True
    return abs((a - b).days) <= 7


def _fetch_polymarket_markets(config: AppConfig) -> List[MarketInfo]:
    url = f"{config.polymarket_rest_url}/markets"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Polymarket discover fetch failed: %s", exc)
        return []
    data = resp.json()
    items = data.get("markets") or data.get("data") or data
    return _parse_market_list("polymarket", items)


def _fetch_kalshi_markets(config: AppConfig) -> List[MarketInfo]:
    url = f"{config.kalshi_base_url}/markets"
    try:
        resp = requests.get(url, timeout=config.kalshi_timeout_seconds)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Kalshi discover fetch failed: %s", exc)
        return []
    data = resp.json()
    items = data.get("markets") or data.get("data") or data
    return _parse_market_list("kalshi", items)


def _parse_market_list(venue: str, items: Any) -> List[MarketInfo]:
    markets: List[MarketInfo] = []
    if isinstance(items, dict):
        items = items.get("markets") or items.get("data") or []
    if not isinstance(items, list):
        return markets

    for item in items:
        title = _pick_str(item, ["title", "question", "market_title", "event_title"])
        market_id = _pick_str(item, ["id", "market_id", "ticker", "condition_id"])
        close_time = _parse_time(_pick_str(item, ["close_time", "closeTime", "end_date", "endDate"]))
        if not title or not market_id:
            continue
        markets.append(
            MarketInfo(
                venue=venue,
                market_id=market_id,
                title=title,
                close_time=close_time,
            )
        )
    return markets


def _pick_str(item: Any, keys: List[str]) -> str:
    for key in keys:
        value = item.get(key) if isinstance(item, dict) else None
        if value:
            return str(value)
    return ""


def _parse_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
