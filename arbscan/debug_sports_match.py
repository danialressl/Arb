from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

from arbscan.catalog import (
    _parse_sports_predicate,
    _parse_sports_predicate_from_event,
    sports_predicates_equivalent,
)
from arbscan.config import AppConfig
from arbscan.http_client import get_json


def run_debug(config: AppConfig, kalshi_input: str, poly_input: str) -> None:
    kalshi_ticker = _extract_kalshi_ticker(kalshi_input)
    poly_key = _extract_polymarket_key(poly_input)

    kalshi_market = _fetch_kalshi_market(config, kalshi_ticker)
    poly_market = _find_polymarket_market(config, poly_key)

    if not kalshi_market:
        print(f"Kalshi market not found for: {kalshi_ticker}")
        return
    if not poly_market:
        print(f"Polymarket market not found for: {poly_key}")
        return

    kalshi_label = kalshi_market.get("title") or kalshi_market.get("subtitle") or ""
    poly_label = poly_market.get("question") or poly_market.get("title") or ""

    kalshi_pred = _parse_sports_predicate(kalshi_label)
    kalshi_time = kalshi_market.get("expected_expiration_time") or kalshi_market.get("close_time")
    if kalshi_time and kalshi_pred is not None:
        kalshi_pred["event_date"] = str(kalshi_time)
    poly_pred = _parse_sports_predicate(poly_label, poly_market.get("outcomes") or [])
    if poly_pred is not None:
        poly_time = poly_market.get("endDate")
        if poly_time:
            poly_pred["event_date"] = str(poly_time)
    poly_event_title = None
    if not poly_pred:
        poly_event = _find_polymarket_event_for_market(config, poly_market)
        if poly_event:
            poly_event_title = poly_event.get("title") or poly_event.get("question")
            if poly_event_title:
                poly_pred = _parse_sports_predicate_from_event(poly_event_title, poly_label)
                poly_time = poly_event.get("endDate") or poly_market.get("endDate")
                if poly_time and poly_pred is not None:
                    poly_pred["event_date"] = str(poly_time)

    print("Kalshi market")
    print(f"  ticker: {kalshi_ticker}")
    print(f"  title: {kalshi_label}")
    print(f"  predicate: {kalshi_pred}")

    print("Polymarket market")
    print(f"  id: {poly_market.get('id')}")
    print(f"  slug: {poly_market.get('slug')}")
    print(f"  question: {poly_label}")
    if poly_event_title:
        print(f"  event_title: {poly_event_title}")
    print(f"  predicate: {poly_pred}")

    print("Match result")
    if kalshi_pred and poly_pred:
        result = sports_predicates_equivalent(kalshi_pred, poly_pred)
        print(f"  equivalent: {result['equivalent']}")
        print(f"  reason: {result['reason']}")
    else:
        print("  equivalent: False")
        print("  reason: missing predicate")


def _extract_kalshi_ticker(value: str) -> str:
    if value.startswith("http"):
        path = urlparse(value).path.strip("/")
        if path:
            return path.split("/")[-1].upper()
    return value.strip().upper()


def _extract_polymarket_key(value: str) -> str:
    if value.startswith("http"):
        path = urlparse(value).path.strip("/")
        if path:
            return path.split("/")[-1]
    return value.strip()


def _fetch_kalshi_market(config: AppConfig, ticker: str) -> Optional[dict]:
    if not ticker:
        return None
    data, status = get_json(
        f"{config.kalshi_base_url}/markets/{ticker}", timeout=config.kalshi_timeout_seconds
    )
    if status != 200 or not data:
        return None
    return data.get("market") or data


def _find_polymarket_market(config: AppConfig, key: str) -> Optional[dict]:
    if not key:
        return None
    direct = _fetch_polymarket_market_by_slug(config, key)
    if direct:
        return direct
    direct = _fetch_polymarket_by_slug(config, key)
    if direct:
        return direct
    url = f"{config.polymarket_gamma_url}/markets"
    offset = 0
    page_size = max(config.polymarket_scan_page_size, 1)
    max_total = max(config.polymarket_scan_limit, page_size)
    headers = {"User-Agent": "arbscan"}

    while offset < max_total:
        params = {"active": "true", "limit": page_size, "offset": offset}
        data, status = get_json(url, params=params, headers=headers, timeout=10)
        if status != 200 or not data:
            return None
        batch = data.get("markets") if isinstance(data, dict) else data
        if not isinstance(batch, list):
            return None
        for market in batch:
            if _polymarket_key_matches(market, key):
                return market
        if len(batch) < page_size:
            break
        offset += page_size
    return None


def _polymarket_key_matches(market: dict, key: str) -> bool:
    if str(market.get("id")) == key:
        return True
    if str(market.get("conditionId")) == key:
        return True
    slug = market.get("slug")
    if isinstance(slug, str) and slug.lower() == key.lower():
        return True
    return False


def _fetch_polymarket_market_by_slug(config: AppConfig, slug: str) -> Optional[dict]:
    url = f"{config.polymarket_gamma_url}/markets/slug/{slug}"
    headers = {"User-Agent": "arbscan"}
    data, status = get_json(url, headers=headers, timeout=10)
    if status != 200 or not data:
        return None
    if isinstance(data, dict) and data.get("id"):
        return data
    return None


def _find_polymarket_event_for_market(config: AppConfig, market: dict) -> Optional[dict]:
    nested_events = market.get("events")
    if isinstance(nested_events, list) and nested_events:
        if isinstance(nested_events[0], dict):
            return nested_events[0]
    market_id = str(market.get("id") or "")
    market_slug = str(market.get("slug") or "")
    for candidate in _event_slug_candidates(market_slug):
        event = _fetch_polymarket_event_by_slug(config, candidate)
        if event:
            return event
    url = f"{config.polymarket_gamma_url}/events"
    offset = 0
    page_size = max(config.polymarket_scan_page_size, 1)
    max_total = max(config.polymarket_scan_limit, page_size)
    headers = {"User-Agent": "arbscan"}

    while offset < max_total:
        params = {"active": "true", "limit": page_size, "offset": offset}
        data, status = get_json(url, params=params, headers=headers, timeout=10)
        if status != 200 or not data:
            return None
        batch = data.get("events") if isinstance(data, dict) else data
        if not isinstance(batch, list):
            return None
        for event in batch:
            markets = event.get("markets") or []
            if not isinstance(markets, list):
                continue
            for nested in markets:
                if str(nested.get("id") or "") == market_id:
                    return event
                if market_slug and str(nested.get("slug") or "").lower() == market_slug.lower():
                    return event
        if len(batch) < page_size:
            break
        offset += page_size
    return None


def _event_slug_candidates(market_slug: str) -> list[str]:
    if not market_slug:
        return []
    candidates = [market_slug]
    if "-" in market_slug:
        candidates.append(market_slug.rsplit("-", 1)[0])
    return list(dict.fromkeys([c for c in candidates if c]))


def _fetch_polymarket_event_by_slug(config: AppConfig, slug: str) -> Optional[dict]:
    url = f"{config.polymarket_gamma_url}/events"
    headers = {"User-Agent": "arbscan"}
    for param in ("slug", "search", "query"):
        data, status = get_json(url, params={param: slug}, headers=headers, timeout=10)
        if status != 200 or not data:
            continue
        batch = data.get("events") if isinstance(data, dict) else data
        if not isinstance(batch, list):
            continue
        for event in batch:
            if str(event.get("slug") or "").lower() == slug.lower():
                return event
        if batch:
            return batch[0]
    return None


def _fetch_polymarket_by_slug(config: AppConfig, slug: str) -> Optional[dict]:
    url = f"{config.polymarket_gamma_url}/markets"
    headers = {"User-Agent": "arbscan"}
    for param in ("slug", "search", "query"):
        data, status = get_json(url, params={param: slug}, headers=headers, timeout=10)
        if status != 200 or not data:
            continue
        batch = data.get("markets") if isinstance(data, dict) else data
        if not isinstance(batch, list):
            continue
        for market in batch:
            if _polymarket_key_matches(market, slug):
                return market
    return None
