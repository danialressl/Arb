from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from difflib import SequenceMatcher

from arbscan.config import AppConfig
from arbscan.http_client import get_json

logger = logging.getLogger(__name__)


def run_debug(config: AppConfig) -> None:
    kalshi_event_id = "kxfeddecision-26jan"
    poly_event_id = "fed-decision-in-january"

    kalshi_event = _fetch_kalshi_event(config, kalshi_event_id)
    poly_event = _fetch_polymarket_event(config, poly_event_id)

    if not kalshi_event or not poly_event:
        print("Failed to fetch one or both events.")
        return

    kalshi_title = kalshi_event.get("title") or ""
    poly_title = poly_event.get("title") or poly_event.get("question") or ""

    title_score = SequenceMatcher(None, kalshi_title.lower(), poly_title.lower()).ratio()
    kalshi_close = _parse_time(kalshi_event.get("close_time") or kalshi_event.get("closeTime"))
    poly_close = _parse_time(poly_event.get("endDate") or poly_event.get("close_time") or poly_event.get("end_date"))

    kalshi_rules = _kalshi_rules_text(kalshi_event)
    poly_rules = _poly_rules_text(poly_event)

    print("Event comparison")
    print(f"Kalshi title: {kalshi_title}")
    print(f"Polymarket title: {poly_title}")
    print(f"Title similarity: {title_score:.3f}")
    print(f"Kalshi close_time: {kalshi_close}")
    print(f"Polymarket end_time: {poly_close}")
    print("Kalshi rules (snippet):")
    print(kalshi_rules[:500])
    print("Polymarket rules (snippet):")
    print(poly_rules[:500])

    kalshi_markets = kalshi_event.get("markets") or []
    poly_markets = poly_event.get("markets") or []

    if not kalshi_markets:
        print("Kalshi market extraction failed: no markets on event")
    if not poly_markets:
        print("Polymarket market extraction failed: no markets on event")

    print("Kalshi markets:")
    for market in kalshi_markets:
        ticker = market.get("ticker") or market.get("market_ticker") or market.get("id") or ""
        title = market.get("title") or market.get("subtitle") or market.get("question") or ""
        print(f"  {ticker} | {title}")

    print("Polymarket markets:")
    for market in poly_markets:
        market_id = market.get("id") or market.get("market_id") or market.get("condition_id") or ""
        label = market.get("question") or market.get("title") or market_id
        print(f"  {market_id} | {label}")

    overlap_rows = _build_overlap(kalshi_markets, poly_markets)
    if overlap_rows:
        print("Overlap table")
        print("canonical_key | kalshi_market | polymarket_market | match")
        for row in overlap_rows:
            print(
                f"{row['key']:12} | {row['kalshi_market']:20} | {row['poly_market']:20} | {row['match']}"
            )
    else:
        print("No overlap rows found. Canonicalization may have failed.")


def _fetch_kalshi_event(config: AppConfig, event_id: str) -> Optional[dict]:
    for candidate in (event_id, event_id.upper()):
        url = f"{config.kalshi_base_url}/events/{candidate}"
        data, status = get_json(
            url, params={"include_markets": "true"}, timeout=config.kalshi_timeout_seconds
        )
        if status == 200 and data:
            event = data.get("event") or data
            markets = data.get("markets") if isinstance(data, dict) else None
            if markets and not event.get("markets"):
                event["markets"] = markets
            if not event.get("markets"):
                markets = _fetch_kalshi_event_markets(config, candidate)
                if markets:
                    event["markets"] = markets
            return event
    print("Kalshi event fetch failed: 404")
    return None


def _fetch_kalshi_event_markets(config: AppConfig, event_id: str) -> list:
    url = f"{config.kalshi_base_url}/events/{event_id}/markets"
    data, status = get_json(url, timeout=config.kalshi_timeout_seconds)
    if status != 200 or not data:
        return []
    if isinstance(data, list):
        return data
    return data.get("markets") or data.get("data") or []


def _fetch_polymarket_event(config: AppConfig, event_id: str) -> Optional[dict]:
    url = f"{config.polymarket_gamma_url}/events/{event_id}"
    data, status = get_json(url, timeout=10)
    if status == 200 and data:
        return data.get("event") or data

    for params in ({"slug": event_id}, {"search": event_id}):
        url = f"{config.polymarket_gamma_url}/events"
        data, status = get_json(url, params=params, timeout=10)
        if status == 200 and data:
            if isinstance(data, list):
                events = data
            else:
                events = data.get("events") or data.get("data") or data
            if isinstance(events, list) and events:
                return events[0]

    print("Polymarket event fetch failed: 422")
    return None


def _kalshi_rules_text(event: dict) -> str:
    parts = [
        event.get("title") or "",
        event.get("rules_primary") or event.get("rulesPrimary") or "",
        event.get("rules_secondary") or event.get("rulesSecondary") or "",
    ]
    return "\n".join(part for part in parts if part)


def _poly_rules_text(event: dict) -> str:
    parts = [
        event.get("question") or event.get("title") or "",
        event.get("description") or "",
        event.get("resolutionSource") or event.get("resolution_source") or "",
    ]
    return "\n".join(part for part in parts if part)


def _parse_time(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return value


def _canonicalize(label: str) -> str:
    if not label:
        return "UNKNOWN"
    lowered = label.lower()
    if "no change" in lowered or "unchanged" in lowered or "0 bps" in lowered:
        return "NO_CHANGE"
    if "25" in lowered and ("cut" in lowered or "decrease" in lowered or "down" in lowered):
        return "CUT_25"
    if "50" in lowered and ("cut" in lowered or "decrease" in lowered or "down" in lowered):
        return "CUT_50_PLUS"
    if "25" in lowered and ("hike" in lowered or "increase" in lowered or "up" in lowered):
        return "HIKE_25_PLUS"
    if "50" in lowered and ("hike" in lowered or "increase" in lowered or "up" in lowered):
        return "HIKE_50_PLUS"
    return "UNKNOWN"


def _build_overlap(kalshi_markets: list, poly_markets: list) -> list[dict]:
    kalshi_by_key = {}
    for market in kalshi_markets:
        title = market.get("title") or market.get("subtitle") or market.get("question") or ""
        key = _canonicalize(title)
        ticker = market.get("ticker") or market.get("market_ticker") or market.get("id") or ""
        if key != "UNKNOWN":
            kalshi_by_key[key] = ticker

    poly_by_key = {}
    for market in poly_markets:
        label = market.get("question") or market.get("title") or ""
        key = _canonicalize(label)
        market_id = market.get("id") or market.get("market_id") or market.get("condition_id") or ""
        if key != "UNKNOWN":
            poly_by_key[key] = market_id

    rows = []
    keys = sorted(set(kalshi_by_key.keys()) | set(poly_by_key.keys()))
    for key in keys:
        rows.append(
            {
                "key": key,
                "kalshi_market": kalshi_by_key.get(key, ""),
                "poly_market": poly_by_key.get(key, ""),
                "match": "MATCH" if key in kalshi_by_key and key in poly_by_key else "NO_MATCH",
            }
        )
    return rows
