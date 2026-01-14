import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from arbv2.config import Config
from arbv2.models import Market
from arbv2.teams import canonicalize_team

logger = logging.getLogger(__name__)

_CACHE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "kalshi_game_series.json"
_CACHE_TTL_SECONDS = 24 * 60 * 60


def ingest_kalshi(config: Config) -> List[Market]:
    markets: List[Market] = []
    game_series = _load_cached_game_series()
    if game_series:
        logger.info("Kalshi series cache hit games=%d path=%s", len(game_series), _CACHE_PATH)
    else:
        series_list = _fetch_series(config)
        logger.info("Kalshi series total sports=%d", len(series_list))
        series_cache: Dict[str, Dict[str, object]] = {}
        game_series = []
        checked_series = 0
        logged_series = 0
        for item in series_list:
            if not isinstance(item, dict):
                continue
            ticker = item.get("ticker")
            if not ticker:
                continue
            checked_series += 1
            if checked_series >= logged_series + 50:
                logger.info(
                    "Kalshi series progress: checked=%d games=%d total=%d",
                    checked_series,
                    len(game_series),
                    len(series_list),
                )
                logged_series = checked_series
            detail = _fetch_series_detail(config, str(ticker), series_cache)
            if _is_game_series(detail):
                game_series.append(str(ticker))
        logger.info("Kalshi series kept games=%d", len(game_series))
        _write_cached_game_series(game_series)
    if not game_series:
        logger.warning("Kalshi ingest found no game series")
        return markets

    sample_titles: List[str] = []
    for series_ticker in game_series:
        cursor = None
        series_count = 0
        while len(markets) < config.ingest_limit:
            data = _fetch_markets_page(config, series_ticker, cursor=cursor, limit=1000)
            batch = data.get("markets") or []
            if not batch:
                break
            for market in batch:
                if len(markets) >= config.ingest_limit:
                    break
                if not isinstance(market, dict):
                    continue
                market_id = market.get("ticker")
                if not market_id:
                    continue
                status = str(market.get("status") or "").strip().lower()
                if status and status not in {"open", "active"}:
                    continue
                title = market.get("title")
                if not title:
                    continue
                event_ticker = market.get("event_ticker") or market_id
                event_title = market.get("event_title") or title or event_ticker
                market_type = "sports_moneyline"
                status = _normalize_kalshi_status(market)
                event_date = _kalshi_event_date(market)
                outcome_label = _kalshi_outcome_label(market)
                if _title_looks_wrong(str(title)):
                    logger.error("Kalshi market title looks like outcome text: %s", title)
                markets.append(
                    Market(
                        venue="kalshi",
                        market_id=str(market_id),
                        event_id=str(event_ticker),
                        series_ticker=series_ticker,
                        title=str(title),
                        event_title=str(event_title),
                        market_type=market_type,
                        status=status,
                        event_date=event_date,
                        outcome_label=outcome_label,
                        raw_json=market,
                    )
                )
                series_count += 1
                if len(sample_titles) < 5:
                    sample_titles.append(str(title))
            cursor = data.get("cursor")
            if not cursor:
                break
        logger.info("Kalshi markets fetched series=%s count=%d", series_ticker, series_count)
        if len(markets) >= config.ingest_limit:
            break
    logger.info("Kalshi markets kept total=%d", len(markets))
    if sample_titles:
        logger.info("Kalshi sample market titles: %s", sample_titles)
    return markets


def _fetch_series(config: Config) -> List[Dict[str, object]]:
    series: List[Dict[str, object]] = []
    cursor = None
    while True:
        params: Dict[str, object] = {"category": "Sports"}
        if cursor:
            params["cursor"] = cursor
        data = _get_json(config, "/series", params=params)
        batch = data.get("series") or []
        for item in batch:
            if isinstance(item, dict):
                series.append(item)
        cursor = data.get("cursor")
        if not cursor:
            break
    return series


def _fetch_markets_page(
    config: Config, series_ticker: str, cursor: Optional[str], limit: int
) -> Dict[str, object]:
    params: Dict[str, object] = {
        "series_ticker": series_ticker,
        "limit": limit,
        "mve_filter": "exclude",
    }
    if cursor:
        params["cursor"] = cursor
    return _get_json(config, "/markets", params=params)


def _fetch_series_detail(
    config: Config, series_ticker: str, cache: Dict[str, Dict[str, object]]
) -> Dict[str, object]:
    if series_ticker in cache:
        return cache[series_ticker]
    data = _get_json(config, f"/series/{series_ticker}")
    detail = data.get("series") if isinstance(data, dict) else None
    detail_obj = detail if isinstance(detail, dict) else {}
    cache[series_ticker] = detail_obj
    return detail_obj


def _is_game_series(series_detail: Dict[str, object]) -> bool:
    if not series_detail:
        return False
    category = series_detail.get("category")
    if str(category).strip() != "Sports":
        return False
    metadata = series_detail.get("product_metadata")
    if not isinstance(metadata, dict):
        return False
    scope = metadata.get("scope")
    return str(scope).strip() == "Game"


def _title_looks_wrong(title: str) -> bool:
    lower = title.lower()
    return lower.count("yes ") >= 2 or "," in title


def _load_cached_game_series() -> List[str]:
    if not _CACHE_PATH.exists():
        return []
    try:
        data = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    generated_at = data.get("generated_at")
    if not isinstance(generated_at, (int, float)):
        return []
    if time.time() - float(generated_at) > _CACHE_TTL_SECONDS:
        return []
    series = data.get("game_series")
    if not isinstance(series, list):
        return []
    return [str(item) for item in series if isinstance(item, str) and item.strip()]


def _write_cached_game_series(game_series: List[str]) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {"generated_at": int(time.time()), "game_series": game_series}
        _CACHE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError:
        logger.warning("Kalshi series cache write failed: %s", _CACHE_PATH)


def _normalize_kalshi_status(market: Dict[str, object]) -> Optional[str]:
    result = str(market.get("result") or "").strip()
    if result:
        return "resolved"
    status = str(market.get("status") or "").strip().lower()
    if status == "active":
        return "open"
    if status in {"closed", "expired"}:
        return "closed"
    return None


def _kalshi_event_date(market: Dict[str, object]) -> Optional[str]:
    value = market.get("expected_expiration_time")
    return _to_utc_date(value)


def _kalshi_outcome_label(market: Dict[str, object]) -> Optional[str]:
    value = market.get("yes_sub_title") or market.get("yes_subtitle")
    return _normalize_outcome(value)


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


def _get_json(config: Config, path: str, params: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    url = config.kalshi_base_url.rstrip("/") + path
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=config.http_timeout_seconds)
        except requests.RequestException as exc:
            if attempt >= 3:
                logger.warning("Kalshi request failed: %s", exc)
                return {}
            time.sleep(0.5 * attempt)
            continue
        if resp.status_code >= 500 or resp.status_code == 429:
            if attempt >= 3:
                logger.warning("Kalshi request error %s for %s", resp.status_code, url)
                return {}
            time.sleep(0.5 * attempt)
            continue
        if resp.status_code != 200:
            logger.warning("Kalshi request error %s for %s", resp.status_code, url)
            return {}
        try:
            return resp.json()
        except json.JSONDecodeError:
            logger.warning("Kalshi response not JSON for %s", url)
            return {}
