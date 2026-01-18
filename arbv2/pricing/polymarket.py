import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from arbv2.config import Config
from arbv2.models import Market, PriceSnapshot
from arbv2.pricing.arb import update_orderbook
from arbv2.storage import insert_prices
from arbv2.teams import canonicalize_team, league_hint_from_text, league_hint_from_url


logger = logging.getLogger(__name__)


def validate_clob_auth(config: Config) -> None:
    missing = []
    if not config.polymarket_clob_api_key:
        missing.append("POLY_CLOB_API_KEY")
    if not config.polymarket_clob_api_secret:
        missing.append("POLY_CLOB_API_SECRET")
    if not config.polymarket_clob_api_passphrase:
        missing.append("POLY_CLOB_API_PASSPHRASE")
    if missing:
        raise RuntimeError(f"Missing Polymarket CLOB auth env vars: {', '.join(missing)}")


def build_token_map(markets: Iterable[Market]) -> Dict[str, List[Tuple[Market, str]]]:
    token_map: Dict[str, List[Tuple[Market, str]]] = {}
    for market in markets:
        for token_id, outcome_label in _resolve_token_ids(market):
            _add_token_target(token_map, token_id, market, outcome_label)
    return token_map


def _add_token_target(
    token_map: Dict[str, List[Tuple[Market, str]]],
    token_id: str,
    market: Market,
    outcome_label: str,
) -> None:
    if not outcome_label:
        return
    entries = token_map.setdefault(token_id, [])
    if any(existing_market.market_id == market.market_id and existing_label == outcome_label for existing_market, existing_label in entries):
        return
    entries.append((market, outcome_label))


def fetch_book_snapshot(
    config: Config,
    token_id: str,
    targets: List[Tuple[Market, str]],
) -> List[PriceSnapshot]:
    url = config.polymarket_clob_rest_url.rstrip("/") + "/book"
    try:
        resp = requests.get(url, params={"token_id": token_id}, timeout=config.http_timeout_seconds)
    except requests.RequestException as exc:
        logger.warning("Polymarket CLOB book request failed: %s", exc)
        return []
    if resp.status_code != 200:
        logger.warning("Polymarket CLOB book error %s for %s", resp.status_code, token_id)
        return []
    try:
        data = resp.json()
    except json.JSONDecodeError:
        logger.warning("Polymarket CLOB book response not JSON for %s", token_id)
        return []
    bids = data.get("bids") or data.get("buys") or []
    asks = data.get("asks") or data.get("sells") or []
    best_bid = _best_bid(bids)
    best_ask = _best_ask(asks)
    last_trade = _parse_float(data.get("last_trade_price"))
    ts_utc = _ts_from_ms(data.get("timestamp")) or _now_utc()
    snapshots: List[PriceSnapshot] = []
    for market, outcome_label in targets:
        update_orderbook(
            "polymarket",
            market.market_id,
            outcome_label,
            _levels_to_list(bids, reverse=True),
            _levels_to_list(asks, reverse=False),
            _to_dt(ts_utc),
        )
        snapshots.append(
            PriceSnapshot(
                venue="polymarket",
                market_id=market.market_id,
                outcome_label=outcome_label,
                best_bid=best_bid,
                best_ask=best_ask,
                last_trade=last_trade,
                ts_utc=ts_utc,
                raw_json=None,
            )
        )
    return snapshots


async def stream_books(config: Config, token_map: Dict[str, List[Tuple[Market, str]]], db_path: str) -> None:
    validate_clob_auth(config)
    if not token_map:
        logger.warning("No Polymarket tokens to subscribe to")
        return
    try:
        import websockets  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("websockets is required for --stream; install it first.") from exc
    url = config.polymarket_clob_ws_url.rstrip("/") + "/ws/market"
    asset_ids = list(token_map.keys())
    orderbooks: Dict[str, Dict[str, Dict[float, float]]] = {}
    last_trade: Dict[str, Optional[float]] = {}
    backoff_seconds = 1

    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
                logger.info("Polymarket WS subscribed assets=%d", len(asset_ids))
                ping_task = asyncio.create_task(_ping(ws))
                message_count = 0
                try:
                    async for message in ws:
                        message_count += 1
                        if message_count % 100 == 0:
                            logger.debug("Polymarket WS messages=%d", message_count)
                        snapshots = _handle_message(message, token_map, orderbooks, last_trade)
                        if snapshots:
                            insert_prices(db_path, snapshots)
                finally:
                    ping_task.cancel()
        except asyncio.CancelledError:
            logger.info("Polymarket WS stream cancelled")
            return
        except Exception as exc:
            logger.warning("Polymarket WS stream ended: %s", exc)
        await asyncio.sleep(backoff_seconds)
        backoff_seconds = min(backoff_seconds * 2, 60)


def _handle_message(
    message: str,
    token_map: Dict[str, List[Tuple[Market, str]]],
    orderbooks: Dict[str, Dict[str, Dict[float, float]]],
    last_trade: Dict[str, Optional[float]],
) -> List[PriceSnapshot]:
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        snapshots: List[PriceSnapshot] = []
        for item in data:
            if isinstance(item, dict):
                snapshots.extend(_handle_message(json.dumps(item), token_map, orderbooks, last_trade))
        return snapshots
    if not isinstance(data, dict):
        return []
    return _handle_message_dict(data, token_map, orderbooks, last_trade)


def _handle_message_dict(
    data: Dict[str, object],
    token_map: Dict[str, List[Tuple[Market, str]]],
    orderbooks: Dict[str, Dict[str, Dict[float, float]]],
    last_trade: Dict[str, Optional[float]],
) -> List[PriceSnapshot]:
    event_type = data.get("event_type")
    asset_id = data.get("asset_id")
    if not asset_id:
        return []
    targets = token_map.get(asset_id)
    if not targets:
        return []

    if event_type == "book":
        bids = data.get("bids") or data.get("buys") or []
        asks = data.get("asks") or data.get("sells") or []
        orderbooks[asset_id] = {
            "bids": _levels_to_map(bids),
            "asks": _levels_to_map(asks),
        }
        for market, outcome_label in targets:
            update_orderbook(
                "polymarket",
                market.market_id,
                outcome_label,
                _levels_to_list(bids, reverse=True),
                _levels_to_list(asks, reverse=False),
                _to_dt(_ts_from_ms(data.get("timestamp")) or _now_utc()),
            )
    elif event_type == "price_change":
        book = orderbooks.setdefault(asset_id, {"bids": {}, "asks": {}})
        for change in data.get("price_changes") or []:
            side = str(change.get("side") or "").upper()
            price = _parse_float(change.get("price"))
            size = _parse_float(change.get("size"))
            if price is None or size is None:
                continue
            if side == "BUY":
                _apply_level_change(book["bids"], price, size)
            elif side == "SELL":
                _apply_level_change(book["asks"], price, size)
        for market, outcome_label in targets:
            update_orderbook(
                "polymarket",
                market.market_id,
                outcome_label,
                _levels_map_to_list(book["bids"], reverse=True),
                _levels_map_to_list(book["asks"], reverse=False),
                _to_dt(_ts_from_ms(data.get("timestamp")) or _now_utc()),
            )
        if "best_bid" in data or "best_ask" in data:
            return [
                _snapshot_from_best(
                    market,
                    outcome_label,
                    _parse_float(data.get("best_bid")),
                    _parse_float(data.get("best_ask")),
                    last_trade.get(asset_id),
                    _ts_from_ms(data.get("timestamp")) or _now_utc(),
                )
                for market, outcome_label in targets
            ]
    elif event_type == "last_trade_price":
        last_trade[asset_id] = _parse_float(data.get("price"))
    elif event_type == "best_bid_ask":
        return [
            _snapshot_from_best(
                market,
                outcome_label,
                _parse_float(data.get("best_bid")),
                _parse_float(data.get("best_ask")),
                last_trade.get(asset_id),
                _ts_from_ms(data.get("timestamp")) or _now_utc(),
            )
            for market, outcome_label in targets
        ]
    else:
        return []

    book = orderbooks.get(asset_id)
    if not book:
        return []
    best_bid = _best_bid_from_map(book["bids"])
    best_ask = _best_ask_from_map(book["asks"])
    for market, outcome_label in targets:
        update_orderbook(
            "polymarket",
            market.market_id,
            outcome_label,
            _levels_map_to_list(book["bids"], reverse=True),
            _levels_map_to_list(book["asks"], reverse=False),
            _to_dt(_ts_from_ms(data.get("timestamp")) or _now_utc()),
        )
    return [
        _snapshot_from_best(
            market,
            outcome_label,
            best_bid,
            best_ask,
            last_trade.get(asset_id),
            _ts_from_ms(data.get("timestamp")) or _now_utc(),
        )
        for market, outcome_label in targets
    ]


def _snapshot_from_best(
    market: Market,
    outcome_label: str,
    best_bid: Optional[float],
    best_ask: Optional[float],
    last_trade_price: Optional[float],
    ts_utc: str,
) -> PriceSnapshot:
    return PriceSnapshot(
        venue="polymarket",
        market_id=market.market_id,
        outcome_label=outcome_label,
        best_bid=best_bid,
        best_ask=best_ask,
        last_trade=last_trade_price,
        ts_utc=ts_utc,
        raw_json=None,
    )


def _apply_level_change(levels: Dict[float, float], price: float, size: float) -> None:
    if size <= 0:
        levels.pop(price, None)
    else:
        levels[price] = size


def _levels_to_map(levels: Iterable[Dict[str, str]]) -> Dict[float, float]:
    mapping: Dict[float, float] = {}
    for level in levels:
        price = _parse_float(level.get("price"))
        size = _parse_float(level.get("size"))
        if price is None or size is None:
            continue
        mapping[price] = size
    return mapping


def _levels_to_list(levels: Iterable[Dict[str, str]], reverse: bool) -> List[tuple]:
    mapped = _levels_to_map(levels)
    return _levels_map_to_list(mapped, reverse=reverse)


def _levels_map_to_list(levels: Dict[float, float], reverse: bool) -> List[tuple]:
    items = [(price, size) for price, size in levels.items()]
    return sorted(items, key=lambda x: x[0], reverse=reverse)


def _best_bid(levels: Iterable[Dict[str, str]]) -> Optional[float]:
    prices = [_parse_float(level.get("price")) for level in levels]
    prices = [p for p in prices if p is not None]
    return max(prices) if prices else None


def _best_ask(levels: Iterable[Dict[str, str]]) -> Optional[float]:
    prices = [_parse_float(level.get("price")) for level in levels]
    prices = [p for p in prices if p is not None]
    return min(prices) if prices else None


def _best_bid_from_map(levels: Dict[float, float]) -> Optional[float]:
    return max(levels.keys()) if levels else None


def _best_ask_from_map(levels: Dict[float, float]) -> Optional[float]:
    return min(levels.keys()) if levels else None


async def _ping(ws) -> None:
    while True:
        await asyncio.sleep(10)
        await ws.send("PING")


def _resolve_token_ids(market: Market) -> List[tuple]:
    raw = market.raw_json or {}
    league_hint = _polymarket_league_hint(raw, market)
    outcomes = _parse_json_list(raw.get("outcomes"))
    tokens = _parse_json_list(raw.get("clobTokenIds"))
    if not outcomes or not tokens or len(outcomes) != len(tokens):
        return []
    if _is_yes_no(outcomes):
        entries: List[tuple] = []
        yes_idx = _find_outcome_index(outcomes, "YES")
        no_idx = _find_outcome_index(outcomes, "NO")
        if yes_idx is not None:
            if market.outcome_label:
                entries.append((tokens[yes_idx], market.outcome_label))
            entries.append((tokens[yes_idx], "YES"))
        if no_idx is not None:
            entries.append((tokens[no_idx], "NO"))
        return entries
    target = _normalize_outcome_label(market.outcome_label, league_hint)
    if not target:
        return []
    for idx, outcome in enumerate(outcomes):
        if _normalize_outcome_label(outcome, league_hint) == target:
            return [(tokens[idx], target)]
    return []


def _find_outcome_index(outcomes: List[str], target: str) -> Optional[int]:
    for idx, outcome in enumerate(outcomes):
        if _normalize_outcome_label(outcome, None) == target:
            return idx
    return None


def _parse_json_list(value: object) -> List[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
    return []


def _is_yes_no(outcomes: List[str]) -> bool:
    lowered = {o.strip().lower() for o in outcomes if o.strip()}
    return lowered.issubset({"yes", "no"})


def _normalize_outcome_label(value: Optional[str], league_hint: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"[^\w\s]", " ", str(value).upper())
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    if text in {"TIE", "DRAW"}:
        return "DRAW"
    return canonicalize_team(text, league_hint)


def _polymarket_league_hint(raw: Dict[str, object], market: Market) -> Optional[str]:
    hint = league_hint_from_url(raw.get("resolutionSource"))
    if hint:
        return hint
    description = str(raw.get("description") or "")
    text = " ".join([description, market.title or "", market.event_title or ""]).strip()
    return league_hint_from_text(text)


def _parse_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ts_from_ms(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        ms = float(value)
    except (TypeError, ValueError):
        return None
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.isoformat()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_dt(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value)
