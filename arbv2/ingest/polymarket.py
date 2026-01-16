import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

from arbv2.config import Config
from arbv2.models import Market
from arbv2.teams import canonicalize_team

logger = logging.getLogger(__name__)


def ingest_polymarket(config: Config) -> List[Market]:
    events = _fetch_events(config)
    markets: List[Market] = []
    logged_event_shape = False
    for event in events:
        if not logged_event_shape and ("tags" not in event or "markets" not in event):
            logger.warning("Polymarket event missing tags/markets: %s", list(event.keys()))
            logged_event_shape = True
        # Events are filtered by tag_id at fetch time; keep all returned events.
        event_id = event.get("id") or event.get("slug") or ""
        event_title = event.get("title") or event.get("slug") or str(event_id)
        status = _normalize_polymarket_status(event)
        for market in event.get("markets") or []:
            if not _is_moneyline_market(market):
                continue
            title = market.get("question") or market.get("title") or event_title
            market_id = market.get("id") or market.get("conditionId")
            if not market_id:
                continue
            market_type = "sports_moneyline"
            event_date = _polymarket_event_date(market)
            outcomes = _parse_json_list(market.get("outcomes"))
            exploded = _explode_outcomes(market_id, outcomes)
            if exploded:
                for exploded_id, outcome_label in exploded:
                    markets.append(
                        Market(
                            venue="polymarket",
                            market_id=exploded_id,
                            event_id=str(event_id),
                            title=str(title),
                            event_title=str(event_title),
                            market_type=market_type,
                            status=status,
                            event_date=event_date,
                            outcome_label=outcome_label,
                            raw_json=market,
                        )
                    )
                continue
            outcome_label = _polymarket_outcome_label(market)
            markets.append(
                Market(
                    venue="polymarket",
                    market_id=str(market_id),
                    event_id=str(event_id),
                    title=str(title),
                    event_title=str(event_title),
                    market_type=market_type,
                    status=status,
                    event_date=event_date,
                    outcome_label=outcome_label,
                    raw_json=market,
                )
            )
    return markets


def _fetch_events(config: Config) -> List[Dict[str, object]]:
    events: List[Dict[str, object]] = []
    series_ids = _series_ids(config)
    if not series_ids:
        series_ids = []
    remaining = config.ingest_limit if config.ingest_limit > 0 else None
    for series_id in series_ids:
        if remaining is not None and remaining <= 0:
            break
        offset = 0
        limit = 1000 if remaining is None else min(1000, remaining)
        while True:
            params = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "series_id": str(series_id),
                "tag_id": str(config.polymarket_games_tag_id),
                "limit": limit,
                "offset": offset,
            }
            data = _get_json(config, "/events", params=params)
            if not isinstance(data, list):
                if data:
                    logger.warning("Polymarket events response unexpected shape")
                break
            if not data:
                break
            for item in data:
                if isinstance(item, dict):
                    events.append(item)
            offset += len(data)
            if remaining is not None:
                remaining -= len(data)
                if remaining <= 0:
                    break
            if len(data) < limit:
                break
    if events:
        return events if config.ingest_limit <= 0 else events[: config.ingest_limit]
    # Fallback if series list is empty or returns nothing.
    offset = 0
    limit = 1000 if config.ingest_limit <= 0 else min(max(config.ingest_limit, 1), 1000)
    while config.ingest_limit <= 0 or len(events) < config.ingest_limit:
        params = {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": limit,
            "offset": offset,
        }
        data = _get_json(config, "/events", params=params)
        if not isinstance(data, list):
            if data:
                logger.warning("Polymarket events response unexpected shape")
            break
        if not data:
            break
        for item in data:
            if isinstance(item, dict):
                events.append(item)
        offset += len(data)
        if len(data) < limit:
            break
    return events if config.ingest_limit <= 0 else events[: config.ingest_limit]


def _series_ids(config: Config) -> List[str]:
    if config.polymarket_series_ids.strip():
        return [item.strip() for item in config.polymarket_series_ids.split(",") if item.strip()]
    data = _get_json(config, "/sports")
    series_ids: List[str] = []
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            series_id = item.get("series")
            if series_id:
                series_ids.append(str(series_id))
    return series_ids


def _is_moneyline_market(market: Dict[str, object]) -> bool:
    if str(market.get("sportsMarketType") or "").strip().lower() != "moneyline":
        return False
    outcomes = _parse_json_list(market.get("outcomes"))
    return len(outcomes) == 2


def _parse_json_list(value: object) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except json.JSONDecodeError:
            return []
    return []


def _get_json(config: Config, path: str, params: Optional[Dict[str, object]] = None) -> object:
    url = config.polymarket_base_url.rstrip("/") + path
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=config.http_timeout_seconds)
        except requests.RequestException as exc:
            if attempt >= 3:
                logger.warning("Polymarket request failed: %s", exc)
                return {}
            time.sleep(0.5 * attempt)
            continue
        if resp.status_code >= 500 or resp.status_code == 429:
            if attempt >= 3:
                logger.warning("Polymarket request error %s for %s", resp.status_code, url)
                return {}
            time.sleep(0.5 * attempt)
            continue
        if resp.status_code != 200:
            logger.warning("Polymarket request error %s for %s", resp.status_code, url)
            return {}
        try:
            return resp.json()
        except json.JSONDecodeError:
            logger.warning("Polymarket response not JSON for %s", url)
            return {}


def _normalize_polymarket_status(event: Dict[str, object]) -> Optional[str]:
    resolved = event.get("resolved")
    archived = event.get("archived")
    closed = event.get("closed")
    active = event.get("active")
    if resolved is True or archived is True:
        return "resolved"
    if closed is True:
        return "closed"
    if active is True and closed is False:
        return "open"
    return None


def _polymarket_event_date(market: Dict[str, object]) -> Optional[str]:
    value = market.get("gameStartTime") or market.get("endDate") or market.get("endDateIso")
    return _to_utc_date(value)


def _polymarket_outcome_label(market: Dict[str, object]) -> Optional[str]:
    question = str(market.get("question") or market.get("title") or "")
    group_title = str(market.get("groupItemTitle") or "")
    if "draw" in question.lower() or "draw" in group_title.lower():
        return "DRAW"
    if "tie" in question.lower() or "tie" in group_title.lower():
        return "DRAW"
    match = re.search(r"will\s+(.+?)\s+win", question, re.IGNORECASE)
    if match:
        return _normalize_outcome(match.group(1))
    return None


def _normalize_outcome(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"[^\w\s]", " ", str(value).upper())
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    if text in {"TIE", "DRAW"}:
        return "DRAW"
    return canonicalize_team(text)


def _explode_outcomes(base_id: str, outcomes: List[str]) -> List[tuple]:
    if not outcomes:
        return []
    normalized = [str(outcome).strip() for outcome in outcomes if str(outcome).strip()]
    if not normalized:
        return []
    lower = {value.lower() for value in normalized}
    if lower.issubset({"yes", "no"}):
        return []
    exploded = []
    for outcome in normalized:
        label = _normalize_outcome(outcome)
        if not label:
            continue
        slug = re.sub(r"\s+", "_", label)
        exploded.append((f"{base_id}:{slug}", label))
    return exploded


def _to_utc_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        try:
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date().isoformat()
