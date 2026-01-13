from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
import unicodedata
from typing import List, Optional

from arbscan.config import AppConfig
# Semantic core: ingestion + domain/predicate parsing. DO NOT RUN IN LIVE LOOP.
from arbscan.canonical import (
    detect_event_domain,
    parse_rate_delta_outcome,
    predicate_from_dict,
    predicate_key,
    predicate_to_dict,
)
# Semantic core: market equivalence logic. DO NOT RUN IN LIVE LOOP.
from arbscan.event_matching import HARD_REASONS
from arbscan.http_client import get_json
from arbscan.models import EventDomain, EventGroup, OutcomeMarket
from arbscan.storage import (
    init_db,
    list_markets_by_venue,
    mark_markets_closed,
    upsert_event_group,
    upsert_market,
    upsert_market_match,
    upsert_outcome,
)

logger = logging.getLogger(__name__)


def scan_catalog(config: AppConfig, sports_only: bool = False) -> None:
    init_db(config.db_path)

    mark_markets_closed(config.db_path, "kalshi")
    mark_markets_closed(config.db_path, "polymarket")

    if sports_only:
        kalshi_count, kalshi_kept, kalshi_samples = _ingest_kalshi_sports_markets(config)
        poly_stats = _ingest_polymarket_sports_conditions(config)
        match_counts = _build_sports_market_matches(config)
        _log_sports_stats(
            kalshi_count,
            kalshi_kept,
            kalshi_samples,
            poly_stats,
            match_counts,
            config,
        )
        return

    kalshi_events = _fetch_kalshi_events(config)
    poly_events = _fetch_polymarket_events(config)

    kalshi_groups = _ingest_kalshi_events(config, kalshi_events)
    poly_groups = _ingest_polymarket_events(config, poly_events)

    match_counts = _build_market_matches(config)
    market_counts = {
        "kalshi": {
            "open": len(list_markets_by_venue(config.db_path, "kalshi", status="open")),
            "total": len(list_markets_by_venue(config.db_path, "kalshi")),
        },
        "polymarket": {
            "open": len(list_markets_by_venue(config.db_path, "polymarket", status="open")),
            "total": len(list_markets_by_venue(config.db_path, "polymarket")),
        },
    }

    logger.info(
        "Catalog scan complete: %d Kalshi events, %d Polymarket events",
        len(kalshi_groups),
        len(poly_groups),
    )
    logger.info(
        "Market counts: kalshi=%d/%d polymarket=%d/%d (open/total)",
        market_counts["kalshi"]["open"],
        market_counts["kalshi"]["total"],
        market_counts["polymarket"]["open"],
        market_counts["polymarket"]["total"],
    )
    _log_quality_stats(kalshi_groups, poly_groups)
    _log_domain_stats(config)
    logger.info(
        "Market match counts: total=%d active=%d inactive=%d",
        match_counts["total"],
        match_counts["active"],
        match_counts["inactive"],
    )
    if match_counts["total"] == 0:
        logger.warning(
            "No market_matches created. Check domain detection and predicate parsing."
        )


def _fetch_kalshi_events(config: AppConfig, with_nested_markets: bool = False) -> List[dict]:
    url = f"{config.kalshi_base_url}/events"
    events = []
    cursor = None
    retries = 0
    max_total = max(config.kalshi_scan_limit, 1)

    while len(events) < max_total:
        params = {"status": "open"}
        if with_nested_markets:
            params["with_nested_markets"] = "true"
        if cursor:
            params["cursor"] = cursor
        data, status = get_json(url, params=params, timeout=config.kalshi_timeout_seconds)
        if status == 429 and retries < 3:
            time.sleep(1.5 * (retries + 1))
            retries += 1
            continue
        if status != 200 or not data:
            logger.warning("Kalshi events fetch failed: %s", status)
            break
        batch = data.get("events") or data.get("data") or []
        if not isinstance(batch, list):
            break
        events.extend(batch)
        cursor = data.get("cursor") or data.get("next_cursor")
        if not cursor or not batch:
            break
        time.sleep(max(0.0, 1.0 / max(config.kalshi_rate_limit_per_second, 1.0)))

    return events[:max_total]


def _fetch_polymarket_events(config: AppConfig) -> List[dict]:
    url = f"{config.polymarket_gamma_url}/events"
    events = []
    offset = 0
    page_size = max(config.polymarket_scan_page_size, 1)
    max_total = max(config.polymarket_scan_limit, page_size)

    while offset < max_total:
        params = {"active": "true", "limit": page_size, "offset": offset}
        data, status = get_json(url, params=params, timeout=10)
        if status != 200 or not data:
            logger.warning("Polymarket events fetch failed: %s", status)
            break
        batch = data.get("events") if isinstance(data, dict) else data
        if not isinstance(batch, list):
            break
        events.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return events[:max_total]


def _fetch_polymarket_conditions(config: AppConfig) -> List[dict]:
    url = f"{config.polymarket_gamma_url}/markets"
    markets = []
    offset = 0
    page_size = max(config.polymarket_scan_page_size, 1)
    max_total = max(config.polymarket_scan_limit, page_size)
    headers = {"User-Agent": "arbscan"}

    while offset < max_total:
        params = {"active": "true", "limit": page_size, "offset": offset}
        data, status = get_json(url, params=params, headers=headers, timeout=10)
        if status != 200 or not data:
            logger.warning("Polymarket markets fetch failed: %s", status)
            break
        batch = data.get("markets") if isinstance(data, dict) else data
        if not isinstance(batch, list):
            break
        markets.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return markets[:max_total]


def _ingest_kalshi_events(config: AppConfig, events: List[dict]) -> List[EventGroup]:
    groups = []
    for idx, event in enumerate(events, start=1):
        _log_progress("Kalshi event ingest", idx, len(events))
        event_id = event.get("event_ticker") or event.get("ticker") or event.get("id")
        if not event_id:
            continue
        detail_raw, status = get_json(
            f"{config.kalshi_base_url}/events/{event_id}", timeout=config.kalshi_timeout_seconds
        )
        if status != 200 or not detail_raw:
            logger.warning("Kalshi event detail fetch failed for %s: %s", event_id, status)
            continue
        detail = detail_raw.get("event") or detail_raw
        title = detail.get("title") or ""
        rules_primary = detail.get("rules_primary") or detail.get("rulesPrimary") or ""
        rules_secondary = detail.get("rules_secondary") or detail.get("rulesSecondary") or ""
        rules_text = "\n".join(part for part in [title, rules_primary, rules_secondary] if part)
        close_time = _parse_time(detail.get("close_time") or detail.get("closeTime"))

        outcomes = []
        markets = detail.get("markets") or detail_raw.get("markets") or []
        for market in markets:
            market_id = market.get("ticker") or market.get("market_ticker") or market.get("id") or ""
            label = market.get("title") or market.get("subtitle") or market.get("question") or market_id
            market_rules = market.get("rules_primary") or market.get("rulesPrimary") or ""
            # Market-level domain detection uses market label + event context.
            market_domain = detect_event_domain(label, f"{title} {rules_text}")
            predicate = (
                parse_rate_delta_outcome(label, rules_text)
                if market_domain == EventDomain.RATE_DELTA
                else None
            )
            outcome_key = predicate_key(predicate) if predicate else "UNKNOWN"
            close_time = _parse_time(market.get("close_time") or detail.get("close_time"))
            is_active = str(market.get("status") or "").lower() == "active"
            status = _status_for_close_time(close_time, is_active=is_active)
            outcomes.append(
                OutcomeMarket(
                    outcome_key=outcome_key,
                    display_label=label,
                    market_id=str(market_id),
                    rules_text=market_rules,
                    event_domain=market_domain,
                    canonical_predicate=predicate,
                )
            )
            upsert_outcome(
                config.db_path,
                "kalshi",
                str(event_id),
                outcome_key,
                label,
                str(market_id),
                market_rules,
                market_domain,
                json.dumps(predicate_to_dict(predicate)) if predicate else None,
                None,
                None,
            )
            upsert_market(
                config.db_path,
                "kalshi",
                str(market_id),
                str(event_id),
                label,
                market_domain,
                json.dumps(predicate_to_dict(predicate)) if predicate else None,
                close_time.isoformat() if close_time else None,
                status,
                market_rules,
                None,
                None,
                market,
            )

        group = EventGroup(
            venue="kalshi",
            event_id=str(event_id),
            title=title,
            close_time=close_time,
            rules_text=rules_text,
            event_domain="MARKET_LEVEL",
            outcomes=outcomes,
        )
        upsert_event_group(
            config.db_path,
            "kalshi",
            str(event_id),
            title,
            close_time.isoformat() if close_time else None,
            rules_text,
            "MARKET_LEVEL",
            detail_raw,
        )
        groups.append(group)
    return groups


def _ingest_kalshi_sports_markets(config: AppConfig) -> tuple[int, int, list]:
    markets = _fetch_kalshi_markets(
        config,
        max_pages=10,
        extra_params={"mve_filter": "exclude"},
    )
    total = len(markets)
    kept = 0
    samples = []
    stats = {
        "events_with_tie": 0,
        "metadata_missing": 0,
        "metadata_not_game": 0,
        "events_game": 0,
        "events_total": 0,
        "markets_seen": 0,
        "markets_volume_filtered": 0,
        "markets_label_filtered": 0,
        "markets_predicate_failed": 0,
    }
    event_markets: dict[str, list] = {}
    tie_events = set()
    for market in markets:
        event_ticker = market.get("event_ticker") or ""
        if not event_ticker:
            continue
        event_markets.setdefault(event_ticker, []).append(market)
        yes_label = str(market.get("yes_sub_title") or market.get("yes_subtitle") or "").lower()
        if yes_label == "tie":
            tie_events.add(event_ticker)

    for event_ticker, event_market_list in event_markets.items():
        stats["events_total"] += 1
        metadata = _fetch_kalshi_event_metadata(config, event_ticker)
        if not metadata:
            stats["metadata_missing"] += 1
            continue
        if config.sports_league_filter_kalshi:
            competition = str(metadata.get("competition") or "").lower()
            if config.sports_league_filter_kalshi.lower() != competition:
                continue
        if str(metadata.get("competition_scope") or "").lower() != "game":
            stats["metadata_not_game"] += 1
            continue
        stats["events_game"] += 1
        if event_ticker in tie_events:
            stats["events_with_tie"] += 1
            continue
        for market in event_market_list:
            stats["markets_seen"] += 1
            label = _kalshi_label(market, "", "")
            if not label:
                stats["markets_label_filtered"] += 1
                continue
            volume_24h = _kalshi_float(market.get("volume_24h"))
            if volume_24h is not None and volume_24h < config.kalshi_sports_min_volume_24h:
                stats["markets_volume_filtered"] += 1
                continue
            if len(samples) < 5 and label:
                samples.append(label)
            if not _is_sports_match_title(label):
                stats["markets_label_filtered"] += 1
                continue
            if label.lower().startswith("yes ") or "," in label:
                stats["markets_label_filtered"] += 1
                continue
            if _has_spread_terms(label):
                stats["markets_label_filtered"] += 1
                continue
            status = str(market.get("status") or "").lower()
            if status != "active":
                continue
            close_time = _parse_time(
                market.get("expected_expiration_time") or market.get("close_time")
            )
            if not _within_sports_window(close_time, config.sports_window_days):
                continue
            predicate = _parse_sports_predicate(label)
            if not predicate:
                stats["markets_predicate_failed"] += 1
                continue
            if close_time:
                predicate["event_date"] = close_time.isoformat()
            kept += 1
            status_label = _status_for_close_time(close_time, is_active=True)
            upsert_market(
                config.db_path,
                "kalshi",
                str(market.get("ticker") or market.get("market_ticker") or market.get("id")),
                str(event_ticker),
                label,
                "SPORTS_WINNER",
                json.dumps(predicate, sort_keys=True),
                close_time.isoformat() if close_time else None,
                status_label,
                market.get("rules_primary") or "",
                None,
                None,
                market,
            )
        time.sleep(max(0.0, 1.0 / max(config.kalshi_rate_limit_per_second, 1.0)))
    logger.info(
        "Kalshi sports filter stats: markets=%d events_total=%d events_game=%d metadata_missing=%d metadata_not_game=%d events_with_tie=%d markets_seen=%d volume_filtered=%d label_filtered=%d predicate_failed=%d",
        total,
        stats["events_total"],
        stats["events_game"],
        stats["metadata_missing"],
        stats["metadata_not_game"],
        stats["events_with_tie"],
        stats["markets_seen"],
        stats["markets_volume_filtered"],
        stats["markets_label_filtered"],
        stats["markets_predicate_failed"],
    )
    return total, kept, samples


def _ingest_polymarket_events(config: AppConfig, events: List[dict]) -> List[EventGroup]:
    groups = []
    for idx, event in enumerate(events, start=1):
        _log_progress("Polymarket event ingest", idx, len(events))
        event_id = event.get("slug") or event.get("id") or event.get("event_id")
        if not event_id:
            continue
        detail, status = get_json(
            f"{config.polymarket_gamma_url}/events/{event_id}", timeout=10
        )
        if status != 200 or not detail:
            detail = event
        title = detail.get("title") or detail.get("question") or ""
        description = detail.get("description") or ""
        resolution_source = detail.get("resolutionSource") or detail.get("resolution_source") or ""
        rules_text = "\n".join(part for part in [title, description, resolution_source] if part)
        close_time = _parse_time(detail.get("endDate") or detail.get("close_time") or detail.get("end_date"))

        outcomes = []
        markets = detail.get("markets") or detail.get("outcomes") or []
        for market in markets:
            market_id = market.get("id") or market.get("market_id") or market.get("condition_id") or ""
            label = market.get("question") or market.get("title") or market_id
            market_rules = market.get("description") or ""
            market_domain = detect_event_domain(label, f"{title} {rules_text}")
            predicate = (
                parse_rate_delta_outcome(label, rules_text)
                if market_domain == EventDomain.RATE_DELTA
                else None
            )
            outcome_key = predicate_key(predicate) if predicate else "UNKNOWN"
            yes_token, no_token = _extract_poly_tokens(market)
            close_time = _parse_time(market.get("endDate") or detail.get("endDate"))
            is_active = bool(market.get("active") or detail.get("active"))
            status = _status_for_close_time(close_time, is_active=is_active)
            outcomes.append(
                OutcomeMarket(
                    outcome_key=outcome_key,
                    display_label=label,
                    market_id=str(market_id),
                    rules_text=market_rules,
                    event_domain=market_domain,
                    canonical_predicate=predicate,
                    yes_token_id=yes_token,
                    no_token_id=no_token,
                )
            )
            upsert_outcome(
                config.db_path,
                "polymarket",
                str(event_id),
                outcome_key,
                label,
                str(market_id),
                market_rules,
                market_domain,
                json.dumps(predicate_to_dict(predicate)) if predicate else None,
                yes_token,
                no_token,
            )
            upsert_market(
                config.db_path,
                "polymarket",
                str(market_id),
                str(event_id),
                label,
                market_domain,
                json.dumps(predicate_to_dict(predicate)) if predicate else None,
                close_time.isoformat() if close_time else None,
                status,
                market_rules,
                yes_token,
                no_token,
                market,
            )

        group = EventGroup(
            venue="polymarket",
            event_id=str(event_id),
            title=title,
            close_time=close_time,
            rules_text=rules_text,
            event_domain="MARKET_LEVEL",
            outcomes=outcomes,
        )
        upsert_event_group(
            config.db_path,
            "polymarket",
            str(event_id),
            title,
            close_time.isoformat() if close_time else None,
            rules_text,
            "MARKET_LEVEL",
            detail,
        )
        groups.append(group)
    return groups


def _ingest_polymarket_sports_conditions(config: AppConfig) -> dict:
    events = _fetch_polymarket_events(config)
    total_events = len(events)
    total_conditions = 0
    kept = 0
    samples = []

    for event in events:
        category = str(event.get("category") or "").lower()
        if category != "sports":
            continue
        event_title = event.get("title") or event.get("question") or ""
        if not event_title:
            continue
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue
        for market in markets:
            total_conditions += 1
            question = market.get("question") or market.get("title") or ""
            if len(samples) < 5 and question:
                samples.append(question)
            if not _is_sports_head_to_head(event_title) and not _is_sports_head_to_head(question):
                continue
            if _has_spread_terms(question) or _has_spread_terms(event_title):
                continue
            if not _is_binary_condition(market):
                continue
            if not bool(market.get("active", True)):
                continue
            if config.sports_league_filter_poly:
                series_slug = str(event.get("seriesSlug") or "").lower()
                if config.sports_league_filter_poly.lower() != series_slug:
                    continue
            outcomes = _extract_outcomes(market)
            predicate = _parse_sports_predicate(question, outcomes)
            if not predicate:
                predicate = _parse_sports_predicate_from_event(event_title, question)
            if not predicate:
                continue
            close_time = _parse_time(market.get("endDate") or event.get("endDate"))
            if not _within_sports_window(close_time, config.sports_window_days):
                continue
            if close_time:
                predicate["event_date"] = close_time.isoformat()
            kept += 1
            status_label = _status_for_close_time(close_time, is_active=True)
            event_id = event.get("slug") or event.get("id") or ""
            yes_token, no_token = _extract_poly_tokens_for_condition(market, outcomes)
            upsert_market(
                config.db_path,
                "polymarket",
                str(market.get("id") or market.get("conditionId") or ""),
                str(event_id),
                question,
                "SPORTS_WINNER",
                json.dumps(predicate, sort_keys=True),
                close_time.isoformat() if close_time else None,
                status_label,
                market.get("description") or "",
                yes_token,
                no_token,
                market,
            )

    return {
        "events": total_events,
        "conditions": total_conditions,
        "kept": kept,
        "samples": samples,
    }


def _fetch_kalshi_event_metadata(config: AppConfig, event_ticker: str) -> Optional[dict]:
    if not event_ticker:
        return None
    data, status = get_json(
        f"{config.kalshi_base_url}/events/{event_ticker}/metadata",
        timeout=config.kalshi_timeout_seconds,
    )
    if status != 200 or not data:
        return None
    return data


def _kalshi_market_has_tie(markets: list) -> bool:
    for market in markets:
        yes_label = str(market.get("yes_sub_title") or market.get("yes_subtitle") or "").lower()
        if yes_label == "tie":
            return True
    return False


def _kalshi_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _within_sports_window(value: Optional[datetime], window_days: int) -> bool:
    if not value:
        return False
    now = datetime.now(timezone.utc) if value.tzinfo else datetime.utcnow()
    delta = value - now
    return timedelta(days=0) <= delta <= timedelta(days=window_days)


def _build_market_matches(config: AppConfig) -> dict:
    kalshi_markets = list_markets_by_venue(config.db_path, "kalshi", status="open")
    poly_markets = list_markets_by_venue(config.db_path, "polymarket", status="open")
    kalshi_map = _group_by_predicate(kalshi_markets)
    poly_map = _group_by_predicate(poly_markets)

    total = 0
    active = 0
    inactive = 0

    for key, kalshi_list in kalshi_map.items():
        poly_list = poly_map.get(key, [])
        if len(kalshi_list) != 1 or len(poly_list) != 1:
            continue
        kalshi_market = kalshi_list[0]
        poly_market = poly_list[0]
        total += 1
        is_active = True
        active += 1
        match_id = f"{kalshi_market['market_id']}::{poly_market['market_id']}"
        upsert_market_match(
            config.db_path,
            match_id,
            kalshi_market["market_id"],
            poly_market["market_id"],
            kalshi_market["domain"],
            kalshi_market["canonical_predicate"],
            is_active,
        )

    return {"total": total, "active": active, "inactive": inactive}


def _build_sports_market_matches(config: AppConfig) -> dict:
    kalshi_markets = list_markets_by_venue(config.db_path, "kalshi", status="open")
    poly_markets = list_markets_by_venue(config.db_path, "polymarket", status="open")

    total = 0
    active = 0
    inactive = 0

    for kalshi_market in kalshi_markets:
        if kalshi_market.get("domain") != "SPORTS_WINNER":
            continue
        k_raw = kalshi_market.get("canonical_predicate")
        if not k_raw:
            continue
        try:
            k_pred = json.loads(k_raw)
        except json.JSONDecodeError:
            continue
        for poly_market in poly_markets:
            if poly_market.get("domain") != "SPORTS_WINNER":
                continue
            p_raw = poly_market.get("canonical_predicate")
            if not p_raw:
                continue
            try:
                p_pred = json.loads(p_raw)
            except json.JSONDecodeError:
                continue
            result = sports_predicates_equivalent(k_pred, p_pred)
            if not result["equivalent"]:
                continue
            if result["reason"] in ("missing_event_date", "invalid_event_date"):
                logger.warning(
                    "Sports predicate match without event_date: %s vs %s",
                    kalshi_market.get("market_id"),
                    poly_market.get("market_id"),
                )
            total += 1
            is_active = kalshi_market["status"] == "open" and poly_market["status"] == "open"
            if is_active:
                active += 1
            else:
                inactive += 1
            match_id = f"{kalshi_market['market_id']}::{poly_market['market_id']}"
            upsert_market_match(
                config.db_path,
                match_id,
                kalshi_market["market_id"],
                poly_market["market_id"],
                "SPORTS_WINNER",
                kalshi_market["canonical_predicate"],
                is_active,
            )

    if total == 0:
        _debug_sports_no_matches(kalshi_markets, poly_markets)
    return {"total": total, "active": active, "inactive": inactive}


def _debug_sports_no_matches(kalshi_markets: list, poly_markets: list) -> None:
    kalshi_preds = _sample_predicates(kalshi_markets)
    poly_preds = _sample_predicates(poly_markets)
    logger.warning(
        "Sports matches=0. Kalshi predicates sample=%s Polymarket predicates sample=%s",
        kalshi_preds,
        poly_preds,
    )


def _sample_predicates(markets: list, limit: int = 5) -> list:
    samples = []
    for market in markets:
        raw = market.get("canonical_predicate")
        if not raw:
            continue
        try:
            predicate = json.loads(raw)
        except json.JSONDecodeError:
            continue
        samples.append(predicate)
        if len(samples) >= limit:
            break
    return samples


def _group_by_predicate(markets: List[dict]) -> dict:
    grouped = {}
    for market in markets:
        raw = market.get("canonical_predicate")
        if not raw:
            continue
        predicate = predicate_from_dict(json.loads(raw))
        if not predicate:
            continue
        key = predicate_key(predicate)
        if not key:
            continue
        grouped.setdefault(key, []).append(market)
    return grouped


def _status_for_close_time(close_time: Optional[datetime], is_active: Optional[bool] = None) -> str:
    if is_active is True:
        return "open"
    if not close_time:
        return "open"
    if close_time.tzinfo is None:
        close_time = close_time.replace(tzinfo=datetime.utcnow().astimezone().tzinfo)
    now = datetime.now(close_time.tzinfo)
    if close_time < now:
        return "closed"
    return "open"


def _with_market_labels(rules_text: str, markets: List[dict]) -> str:
    return rules_text


def _fetch_kalshi_markets(
    config: AppConfig,
    max_pages: Optional[int] = None,
    extra_params: Optional[dict] = None,
) -> List[dict]:
    url = f"{config.kalshi_base_url}/markets"
    markets = []
    cursor = None
    retries = 0
    pages = 0
    while True:
        params = {"status": "open"}
        if extra_params:
            params.update(extra_params)
        if cursor:
            params["cursor"] = cursor
        data, status = get_json(url, params=params, timeout=config.kalshi_timeout_seconds)
        if status == 429 and retries < 3:
            time.sleep(1.5 * (retries + 1))
            retries += 1
            continue
        if status != 200 or not data:
            logger.warning("Kalshi markets fetch failed: %s", status)
            break
        batch = data.get("markets") or data.get("data") or []
        if not isinstance(batch, list):
            break
        markets.extend(batch)
        cursor = data.get("cursor") or data.get("next_cursor")
        if not cursor or not batch:
            break
        pages += 1
        if max_pages is not None and pages >= max_pages:
            logger.info("Kalshi markets fetch capped at %d pages (sports-only).", max_pages)
            break
        time.sleep(max(0.0, 1.0 / max(config.kalshi_rate_limit_per_second, 1.0)))
    return markets


def _kalshi_event_title_map(config: AppConfig) -> dict:
    events = _fetch_kalshi_events(config)
    mapping = {}
    for event in events:
        ticker = event.get("event_ticker") or event.get("ticker") or event.get("id") or ""
        title = event.get("title") or ""
        if ticker and title:
            mapping[ticker] = title
    return mapping


def _is_sports_match_title(text: str) -> bool:
    lowered = text.lower()
    if " @ " in lowered:
        return True
    if " at " in lowered:
        return True
    if re.search(r"\\b(vs|beat|beats|defeat|defeats)\\b", lowered):
        return True
    if " win " in lowered and (" match" in lowered or " matchup" in lowered):
        return True
    return "who will win" in lowered or " or " in lowered


def _is_sports_head_to_head(text: str) -> bool:
    lowered = text.lower()
    if " @ " in lowered or " vs " in lowered:
        return True
    if re.search(r"\\b(beat|beats|defeat|defeats)\\b", lowered):
        return True
    if " or " in lowered and (" matchup" in lowered or " match" in lowered):
        return True
    return False


def _kalshi_label(market: dict, event_title: str, event_subtitle: str) -> str:
    raw_title = market.get("title") or ""
    raw_subtitle = market.get("subtitle") or ""
    if raw_title and not _looks_like_outcome_text(raw_title):
        return raw_title
    if raw_subtitle and not _looks_like_outcome_text(raw_subtitle):
        return raw_subtitle
    if event_title and not _looks_like_outcome_text(event_title):
        return event_title
    if event_subtitle and not _looks_like_outcome_text(event_subtitle):
        return event_subtitle
    return ""


def _looks_like_outcome_text(label: str) -> bool:
    lowered = label.lower()
    if "," in lowered and (lowered.count("yes ") > 0 or lowered.count("no ") > 0):
        return True
    return False


def _has_spread_terms(text: str) -> bool:
    lowered = text.lower()
    banned = [" over ", " under ", "points", "runs", "goals", "spread", "total"]
    return any(term in lowered for term in banned)


def _is_binary_condition(condition: dict) -> bool:
    outcomes = condition.get("outcomes") or condition.get("possibleOutcomes") or []
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            outcomes = []
    if isinstance(outcomes, list):
        lowered = []
        for outcome in outcomes:
            if isinstance(outcome, dict):
                lowered.append(str(outcome.get("name") or outcome.get("outcome") or "").lower())
            else:
                lowered.append(str(outcome).lower())
        if "yes" in lowered and "no" in lowered:
            return True
        return len(lowered) == 2
    return False


def _extract_poly_token(condition: dict, outcome_label: str) -> str:
    clob_tokens = condition.get("clobTokenIds")
    if isinstance(clob_tokens, dict):
        return str(clob_tokens.get(outcome_label.upper()) or clob_tokens.get(outcome_label.lower()) or "")
    tokens = condition.get("tokens") or condition.get("outcomes") or []
    for token in tokens:
        if isinstance(token, dict):
            outcome = str(token.get("outcome") or token.get("name") or "").lower()
            if outcome == outcome_label.lower():
                return str(token.get("token_id") or token.get("id") or "")
    return ""


def _extract_outcomes(condition: dict) -> list:
    outcomes = condition.get("outcomes") or condition.get("possibleOutcomes") or []
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            return []
    if isinstance(outcomes, list):
        return outcomes
    return []


def _extract_poly_tokens_for_condition(condition: dict, outcomes: list) -> tuple[str, str]:
    clob_tokens = condition.get("clobTokenIds")
    if isinstance(clob_tokens, dict):
        yes = str(clob_tokens.get("YES") or clob_tokens.get("yes") or "")
        no = str(clob_tokens.get("NO") or clob_tokens.get("no") or "")
        if yes and no:
            return yes, no
    if isinstance(clob_tokens, list) and len(clob_tokens) >= 2:
        return str(clob_tokens[0]), str(clob_tokens[1])
    if outcomes:
        yes = _extract_poly_token(condition, str(outcomes[0]))
        no = _extract_poly_token(condition, str(outcomes[1])) if len(outcomes) > 1 else ""
        return yes, no
    return "", ""


def _parse_sports_predicate(label: str, outcomes: Optional[list] = None) -> Optional[dict]:
    text = re.sub(r"[^\w\s@]", " ", label.lower())
    text = re.sub(r"\s+", " ", text).strip()
    win_vs_match = re.search(r"will (.+?) win (?:the )?(.+?) (?:vs|@) (.+)", text)
    if win_vs_match:
        winner = _normalize_team_name(win_vs_match.group(1))
        team_a = _normalize_team_name(win_vs_match.group(2))
        team_b = _normalize_team_name(win_vs_match.group(3))
        if winner and team_a and team_b and winner in (team_a, team_b):
            return _normalize_sports_predicate(
                {"team_a": team_a, "team_b": team_b, "winner": winner}
            )
    or_match = re.search(r"will (.+?) or (.+?) win", text)
    if or_match:
        team_a, team_b = _normalize_teams(or_match.group(1), or_match.group(2))
        if team_a and team_b:
            return _normalize_sports_predicate(
                {"team_a": team_a, "team_b": team_b, "winner": team_a}
            )
    win_match = re.search(r"will (.+?) (?:beat|defeat|win (?:vs|v|against)) (.+)", text)
    if win_match:
        team_a, team_b = _normalize_teams(win_match.group(1), win_match.group(2))
        if team_a and team_b:
            return _normalize_sports_predicate(
                {"team_a": team_a, "team_b": team_b, "winner": team_a}
            )
    if outcomes and len(outcomes) == 2:
        team_a = _normalize_team_name(str(outcomes[0]))
        team_b = _normalize_team_name(str(outcomes[1]))
        if team_a and team_b:
            return _normalize_sports_predicate(
                {"team_a": team_a, "team_b": team_b, "winner": team_a}
            )
    if " vs " in text:
        left, right = text.split(" vs ", 1)
        team_a, team_b = _normalize_teams(left, right)
        winner = team_a
    elif " at " in text:
        left, right = text.split(" at ", 1)
        team_a, team_b = _normalize_teams(left, right)
        winner = team_a
    elif " @ " in text:
        left, right = text.split(" @ ", 1)
        team_a, team_b = _normalize_teams(left, right)
        winner = team_a
    elif " beats " in text:
        left, right = text.split(" beats ", 1)
        team_a, team_b = _normalize_teams(left, right)
        winner = team_a
    elif " defeat " in text:
        left, right = text.split(" defeat ", 1)
        team_a, team_b = _normalize_teams(left, right)
        winner = team_a
    else:
        return None
    if not team_a or not team_b:
        return None
    return _normalize_sports_predicate({"team_a": team_a, "team_b": team_b, "winner": winner})


def _parse_sports_predicate_from_event(event_title: str, market_title: str) -> Optional[dict]:
    base = _parse_sports_predicate(event_title)
    if not base:
        return None
    winner = _normalize_team_name(market_title)
    if winner == base.get("team_a") or winner == base.get("team_b"):
        base["winner"] = winner
        return _normalize_sports_predicate(base)
    return None


def _normalize_teams(team_a: str, team_b: str) -> tuple[str, str]:
    return _normalize_team_name(team_a), _normalize_team_name(team_b)


def _normalize_team_name(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text or "")
    lowered = re.sub(r"[^\w\s]", "", normalized.lower()).strip()
    for prefix in ("will ", "who will win ", "who wins ", "who will ", "the "):
        if lowered.startswith(prefix):
            lowered = lowered[len(prefix):]
    for token in (
        " in ",
        " at ",
        " on ",
        " during ",
        " match",
        " matchup",
        " game",
        " round",
        " win",
        " wins",
        " winner",
    ):
        if token in lowered:
            lowered = lowered.split(token, 1)[0].strip()
    return lowered.strip()




def _normalize_sports_predicate(predicate: dict) -> dict:
    team_a = predicate.get("team_a")
    team_b = predicate.get("team_b")
    if team_a and team_b and team_a > team_b:
        predicate = dict(predicate)
        predicate["team_a"], predicate["team_b"] = team_b, team_a
    return predicate


def sports_predicates_equivalent(left: dict, right: dict) -> dict:
    left_a = normalize_text_tokens(left.get("team_a"))
    left_b = normalize_text_tokens(left.get("team_b"))
    right_a = normalize_text_tokens(right.get("team_a"))
    right_b = normalize_text_tokens(right.get("team_b"))
    left_w = normalize_text_tokens(left.get("winner"))
    right_w = normalize_text_tokens(right.get("winner"))

    if not left_a or not left_b or not right_a or not right_b:
        return {"equivalent": False, "reason": "missing_team_tokens"}

    left_a_f, left_b_f = _strip_shared_tokens(left_a, left_b)
    right_a_f, right_b_f = _strip_shared_tokens(right_a, right_b)

    if (
        _token_overlap_ratio(_tokens_len_ge(left_a_f, 3), _tokens_len_ge(left_b_f, 3)) >= 0.4
        or _token_overlap_ratio(_tokens_len_ge(right_a_f, 3), _tokens_len_ge(right_b_f, 3)) >= 0.4
    ):
        return {"equivalent": False, "reason": "role_separation_failed"}

    if not _tokens_contain(left_a_f, right_a_f):
        return {"equivalent": False, "reason": "team_a_mismatch"}
    if not _tokens_contain(left_b_f, right_b_f):
        return {"equivalent": False, "reason": "team_b_mismatch"}

    winner_a = _tokens_contain(left_w, left_a_f) and _tokens_contain(right_w, right_a_f)
    winner_b = _tokens_contain(left_w, left_b_f) and _tokens_contain(right_w, right_b_f)
    if winner_a and winner_b:
        return {"equivalent": False, "reason": "winner_ambiguous"}
    if not (winner_a or winner_b):
        return {"equivalent": False, "reason": "winner_mismatch"}

    time_ok, time_reason = _event_time_ok(left.get("event_date"), right.get("event_date"))
    if not time_ok:
        return {"equivalent": False, "reason": time_reason}

    return {"equivalent": True, "reason": "ok"}


def normalize_text_tokens(text: Optional[str]) -> set:
    normalized = unicodedata.normalize("NFKD", text or "")
    lowered = re.sub(r"[^\w\s]", " ", normalized.lower())
    tokens = [tok for tok in lowered.split() if tok]
    return set(tokens)


def _tokens_contain(left: set, right: set) -> bool:
    if not left or not right:
        return False
    return left.issubset(right) or right.issubset(left)


def _token_overlap_ratio(left: set, right: set) -> float:
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / float(min(len(left), len(right)))


def _tokens_len_ge(tokens: set, min_len: int) -> set:
    return {tok for tok in tokens if len(tok) >= min_len}


def _strip_shared_tokens(left: set, right: set) -> tuple[set, set]:
    shared = left.intersection(right)
    if not shared:
        return left, right
    left_only = left.difference(shared)
    right_only = right.difference(shared)
    if not left_only or not right_only:
        return left, right
    return left_only, right_only


def _event_time_ok(left: Optional[str], right: Optional[str]) -> tuple[bool, str]:
    if not left or not right:
        return True, "missing_event_date"
    left_dt = _parse_event_datetime(left)
    right_dt = _parse_event_datetime(right)
    if not left_dt or not right_dt:
        return True, "invalid_event_date"
    delta = abs((left_dt - right_dt).total_seconds())
    if delta > timedelta(hours=24).total_seconds():
        return False, "event_time_mismatch"
    return True, "ok"


def _parse_event_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None


def _group_by_sports_predicate(markets: List[dict]) -> dict:
    grouped = {}
    for market in markets:
        if market.get("domain") != "SPORTS_WINNER":
            continue
        raw = market.get("canonical_predicate")
        if not raw:
            continue
        try:
            predicate = json.loads(raw)
        except json.JSONDecodeError:
            continue
        key = f"{predicate.get('team_a')}|{predicate.get('team_b')}|{predicate.get('winner')}"
        grouped.setdefault(key, []).append(market)
    return grouped


def _within_time_window(a: Optional[str], b: Optional[str]) -> bool:
    if not a or not b:
        return True
    try:
        a_dt = datetime.fromisoformat(a.replace("Z", "+00:00"))
        b_dt = datetime.fromisoformat(b.replace("Z", "+00:00"))
    except ValueError:
        return True
    delta = abs((a_dt - b_dt).total_seconds())
    return delta <= timedelta(hours=24).total_seconds()


def _log_sports_stats(
    kalshi_total: int,
    kalshi_kept: int,
    kalshi_samples: list,
    poly_stats: dict,
    match_counts: dict,
    config: AppConfig,
) -> None:
    logger.info(
        "Sports ingest: kalshi_total=%d kalshi_kept=%d",
        kalshi_total,
        kalshi_kept,
    )
    logger.info(
        "Sports ingest: polymarket_events=%d conditions=%d kept=%d",
        poly_stats["events"],
        poly_stats["conditions"],
        poly_stats["kept"],
    )
    if poly_stats["kept"] < 50:
        logger.error("Sports conditions < 50. Check Polymarket ingestion filters.")
    if poly_stats["kept"] == 0 and poly_stats.get("samples"):
        logger.warning("Polymarket sports sample labels: %s", poly_stats["samples"])
    logger.info(
        "Sports match counts: total=%d active=%d inactive=%d",
        match_counts["total"],
        match_counts["active"],
        match_counts["inactive"],
    )
    if match_counts["total"] == 0:
        logger.critical("No sports market_matches created.")
        logger.critical("Sample Polymarket labels: %s", poly_stats.get("samples", []))
        logger.critical("Sample Kalshi labels: %s", kalshi_samples)


def _extract_poly_tokens(market: dict) -> tuple[str, str]:
    clob_tokens = market.get("clobTokenIds")
    if isinstance(clob_tokens, dict):
        yes = str(clob_tokens.get("YES") or clob_tokens.get("yes") or "")
        no = str(clob_tokens.get("NO") or clob_tokens.get("no") or "")
        if yes or no:
            return yes, no
    if isinstance(clob_tokens, list) and len(clob_tokens) >= 2:
        return str(clob_tokens[0]), str(clob_tokens[1])
    return "", ""


def _log_quality_stats(
    kalshi_groups: List[EventGroup], poly_groups: List[EventGroup]
) -> None:
    kalshi_lengths = [len(group.rules_text or "") for group in kalshi_groups]
    poly_lengths = [len(group.rules_text or "") for group in poly_groups]
    logger.info(
        "Kalshi event rules coverage: %d/%d non-empty",
        sum(1 for length in kalshi_lengths if length > 0),
        len(kalshi_lengths),
    )
    logger.info(
        "Polymarket event rules coverage: %d/%d non-empty",
        sum(1 for length in poly_lengths if length > 0),
        len(poly_lengths),
    )


def _log_domain_stats(config: AppConfig) -> None:
    kalshi_markets = list_markets_by_venue(config.db_path, "kalshi", status="open")
    poly_markets = list_markets_by_venue(config.db_path, "polymarket", status="open")
    domain_counts = {}
    predicate_total = 0
    predicate_present = 0
    rate_delta_markets = 0
    unparsed_rate_delta = []

    for market in kalshi_markets + poly_markets:
        domain = market.get("domain") or "UNKNOWN"
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
        predicate_total += 1
        if domain == EventDomain.RATE_DELTA:
            rate_delta_markets += 1
            if not market.get("canonical_predicate"):
                unparsed_rate_delta.append(market.get("title") or market.get("market_id") or "")
        if market.get("canonical_predicate"):
            predicate_present += 1

    logger.info("Market domain counts: %s", domain_counts)
    logger.info("Rate-delta markets: %d", rate_delta_markets)
    logger.info("Market predicates parsed: %d/%d", predicate_present, predicate_total)
    parsed_ratio = (predicate_present / rate_delta_markets) if rate_delta_markets else 0.0
    logger.info("Rate-delta parsed ratio: %.2f", parsed_ratio)
    if rate_delta_markets < 50:
        logger.warning("Rate-delta market count is low: %d", rate_delta_markets)
    if rate_delta_markets > 0 and parsed_ratio < 0.7:
        logger.warning("Rate-delta parsed ratio below 0.7: %.2f", parsed_ratio)
        if unparsed_rate_delta:
            logger.warning(
                "Top unparsed rate-delta labels: %s",
                unparsed_rate_delta[:10],
            )


def _log_reject_stats(stats: dict) -> None:
    if not stats:
        return
    total = sum(stats.values())
    logger.info("Rejection reason counts: %s (total=%d)", stats, total)


def _log_progress(label: str, current: int, total: int, step: int = 25) -> None:
    if total == 0:
        return
    if current == 1 or current == total or current % step == 0:
        pct = (current / total) * 100.0
        logger.info("%s progress: %d/%d (%.1f%%)", label, current, total, pct)


def _log_similarity_stats(stats: dict, status_counts: dict, reject_stats: dict) -> None:
    score_samples = stats.get("score_samples") or []
    total_comparisons = stats.get("total_comparisons", 0)
    top_pairs = stats.get("top_pairs") or []
    logger.info("Total comparisons: %d", total_comparisons)
    logger.info("Match status counts: %s", status_counts)

    if score_samples:
        min_score = min(score_samples)
        max_score = max(score_samples)
        median = _median(score_samples)
        logger.info(
            "Event score stats: min=%.3f median=%.3f max=%.3f",
            min_score,
            median,
            max_score,
        )
        if max_score < 0.3:
            logger.error("Max text similarity < 0.3; embedding/text pipeline may be broken.")

    if top_pairs:
        logger.info("Top similarity pairs:")
        for item in top_pairs:
            logger.info(
                "  %s <-> %s : %.3f (text=%.3f) | times=%s/%s",
                item["kalshi_event_id"],
                item["polymarket_event_id"],
                item["score"],
                item["text_score"],
                item["kalshi_time"],
                item["poly_time"],
            )

    if status_counts.get("NO", 0) == sum(status_counts.values()):
        top_reasons = sorted(reject_stats.items(), key=lambda item: item[1], reverse=True)[:3]
        logger.critical(
            "All matches are NO. Top rejection reasons: %s. Suggest: python -m arbscan explain-event-match <kalshi_event> <polymarket_event>",
            top_reasons,
        )

    if reject_stats:
        total = sum(reject_stats.values())
        for reason, count in sorted(reject_stats.items(), key=lambda item: item[1], reverse=True):
            pct = (count / total) * 100 if total else 0
            logger.info("Rejection reason %s: %d (%.1f%%)", reason, count, pct)
        hard_counts = {k: v for k, v in reject_stats.items() if k in HARD_REASONS}
        if hard_counts:
            top_reason, top_count = max(hard_counts.items(), key=lambda item: item[1])
            hard_total = sum(hard_counts.values())
            if hard_total and (top_count / hard_total) > 0.9:
                logger.warning(
                    "Single hard reason dominates rejections: %s (%.1f%%)",
                    top_reason,
                    (top_count / hard_total) * 100,
                )


def _median(values: list[float]) -> float:
    sorted_vals = sorted(values)
    mid = len(sorted_vals) // 2
    if len(sorted_vals) % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0
    return sorted_vals[mid]
