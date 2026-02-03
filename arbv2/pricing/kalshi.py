import asyncio
import base64
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from arbv2.config import Config
from arbv2.models import Market, PriceSnapshot
from arbv2.pricing.arb import update_orderbook
from arbv2.storage import insert_prices


logger = logging.getLogger(__name__)


async def stream_books(
    config: Config,
    markets: List[Market],
    db_path: str,
) -> None:
    if not markets:
        logger.warning("No Kalshi markets to subscribe to")
        return
    if not config.kalshi_api_key:
        raise RuntimeError("Missing Kalshi API key; set KALSHI_API_KEY.")
    if not config.kalshi_private_key_path:
        raise RuntimeError("Missing Kalshi private key; set KALSHI_PRIVATE_KEY_PATH.")
    try:
        import websockets  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("websockets is required for Kalshi WS; install it first.") from exc
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except ModuleNotFoundError as exc:
        raise RuntimeError("cryptography is required for Kalshi WS auth; install it first.") from exc

    market_by_ticker = {m.market_id: m for m in markets}
    market_id_to_ticker: Dict[str, str] = {}
    levels_by_ticker: Dict[str, Dict[str, Dict[float, float]]] = {}
    ws_url = config.kalshi_ws_url.rstrip("/")
    tickers = list(market_by_ticker.keys())
    delta_count = 0

    while True:
        try:
            ws_headers = _kalshi_ws_headers(
                config.kalshi_api_key,
                config.kalshi_private_key_path,
                config.kalshi_ws_url,
                hashes,
                serialization,
                padding,
            )
            connect_kwargs = _ws_connect_kwargs(ws_headers)
            async with websockets.connect(ws_url, **connect_kwargs) as ws:
                params = {
                    "channels": ["orderbook_delta"],
                    "send_initial_snapshot": True,
                }
                if len(tickers) == 1:
                    params["market_ticker"] = tickers[0]
                else:
                    params["market_tickers"] = tickers
                subscribe = {"id": 1, "cmd": "subscribe", "params": params}
                await ws.send(json.dumps(subscribe))
                logger.info("Kalshi WS connected and subscribed markets=%d", len(tickers))
                async for message in ws:
                    snapshots = _handle_ws_message(
                        message,
                        levels_by_ticker,
                        market_id_to_ticker,
                        market_by_ticker,
                    )
                    if snapshots:
                        insert_prices(db_path, snapshots)
                    delta_count += 1
                    if delta_count % 200 == 0:
                        logger.debug("Kalshi WS deltas processed=%d", delta_count)
        except asyncio.CancelledError:
            logger.info("Kalshi WS stream cancelled")
            return
        except Exception as exc:
            logger.warning("Kalshi WS stream ended: %s", exc)
            continue


def _handle_ws_message(
    message: str,
    levels_by_ticker: Dict[str, Dict[str, Dict[float, float]]],
    market_id_to_ticker: Dict[str, str],
    market_by_ticker: Dict[str, Market],
) -> List[PriceSnapshot]:
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, dict):
        return []
    msg_type = data.get("type")
    if msg_type == "orderbook_snapshot":
        return _handle_snapshot(data.get("msg") or {}, levels_by_ticker, market_id_to_ticker, market_by_ticker)
    if msg_type == "orderbook_delta":
        return _handle_delta(data.get("msg") or {}, levels_by_ticker, market_id_to_ticker, market_by_ticker)
    if data.get("cmd") == "subscribed":
        logger.info("Kalshi WS subscribed response received")
    return []


def _handle_snapshot(
    msg: Dict[str, object],
    levels_by_ticker: Dict[str, Dict[str, Dict[float, float]]],
    market_id_to_ticker: Dict[str, str],
    market_by_ticker: Dict[str, Market],
) -> List[PriceSnapshot]:
    market_ticker = str(msg.get("market_ticker") or "")
    market_id = str(msg.get("market_id") or "")
    if market_ticker:
        market_id_to_ticker[market_id] = market_ticker
    if not market_ticker:
        market_ticker = market_id_to_ticker.get(market_id, "")
    if not market_ticker:
        return []
    yes_levels = _levels_from_snapshot(msg.get("yes_dollars") or [])
    no_levels = _levels_from_snapshot(msg.get("no_dollars") or [])
    levels_by_ticker[market_ticker] = {"yes": yes_levels, "no": no_levels}
    ts_dt = _parse_ws_ts(msg.get("ts")) or datetime.now(timezone.utc)
    outcome_label = market_by_ticker.get(market_ticker).outcome_label if market_ticker in market_by_ticker else None
    _publish_books(market_ticker, outcome_label, yes_levels, no_levels, ts_dt)
    logger.debug(
        "Kalshi WS snapshot market=%s yes_levels=%d no_levels=%d",
        market_ticker,
        len(yes_levels),
        len(no_levels),
    )
    return _build_snapshot_rows(market_ticker, outcome_label, yes_levels, no_levels, ts_dt)


def _handle_delta(
    msg: Dict[str, object],
    levels_by_ticker: Dict[str, Dict[str, Dict[float, float]]],
    market_id_to_ticker: Dict[str, str],
    market_by_ticker: Dict[str, Market],
) -> List[PriceSnapshot]:
    market_ticker = str(msg.get("market_ticker") or "")
    market_id = str(msg.get("market_id") or "")
    if market_ticker:
        market_id_to_ticker[market_id] = market_ticker
    if not market_ticker:
        market_ticker = market_id_to_ticker.get(market_id, "")
    if not market_ticker:
        return []
    side = str(msg.get("side") or "").lower()
    price = _parse_float(msg.get("price_dollars"))
    delta = _parse_float(msg.get("delta"))
    if price is None or delta is None or side not in {"yes", "no"}:
        return []
    levels = levels_by_ticker.setdefault(market_ticker, {"yes": {}, "no": {}})
    _apply_delta(levels[side], price, delta)
    ts_dt = _parse_ws_ts(msg.get("ts")) or datetime.now(timezone.utc)
    yes_levels = levels["yes"]
    no_levels = levels["no"]
    outcome_label = market_by_ticker.get(market_ticker).outcome_label if market_ticker in market_by_ticker else None
    _publish_books(market_ticker, outcome_label, yes_levels, no_levels, ts_dt)
    # Avoid per-tick spam; aggregate logging handled by caller.
    return _build_snapshot_rows(market_ticker, outcome_label, yes_levels, no_levels, ts_dt)


def _levels_from_snapshot(raw_levels: Iterable[Iterable[object]]) -> Dict[float, float]:
    levels: Dict[float, float] = {}
    for level in raw_levels:
        try:
            price = _parse_float(level[0])
            size = _parse_float(level[1])
        except (IndexError, TypeError):
            continue
        if price is None or size is None:
            continue
        levels[price] = size
    return levels


def _apply_delta(levels: Dict[float, float], price: float, delta: float) -> None:
    new_size = levels.get(price, 0.0) + delta
    if new_size <= 0:
        levels.pop(price, None)
    else:
        levels[price] = new_size


def _publish_books(
    market_ticker: str,
    outcome_label: Optional[str],
    yes_levels: Dict[float, float],
    no_levels: Dict[float, float],
    ts_dt: datetime,
) -> None:
    bids_yes, asks_yes, bids_no, asks_no = _build_books(yes_levels, no_levels)
    update_orderbook("kalshi", market_ticker, "YES", bids_yes, asks_yes, ts_dt)
    if outcome_label:
        update_orderbook("kalshi", market_ticker, outcome_label, bids_yes, asks_yes, ts_dt)
    update_orderbook("kalshi", market_ticker, "NO", bids_no, asks_no, ts_dt)


def _build_books(
    yes_levels: Dict[float, float],
    no_levels: Dict[float, float],
) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]], List[Tuple[float, float]], List[Tuple[float, float]]]:
    bids_yes = sorted([(price, size) for price, size in yes_levels.items()], key=lambda x: x[0], reverse=True)
    bids_no = sorted([(price, size) for price, size in no_levels.items()], key=lambda x: x[0], reverse=True)
    asks_yes = _derive_asks_from_bids(no_levels)
    asks_no = _derive_asks_from_bids(yes_levels)
    return bids_yes, asks_yes, bids_no, asks_no


def _derive_asks_from_bids(levels: Dict[float, float]) -> List[Tuple[float, float]]:
    asks = []
    for price, size in levels.items():
        ask_price = 1.0 - price
        if ask_price < 0.0:
            ask_price = 0.0
        if ask_price > 1.0:
            ask_price = 1.0
        asks.append((ask_price, size))
    return sorted(asks, key=lambda x: x[0])


def _build_snapshot_rows(
    market_ticker: str,
    outcome_label: Optional[str],
    yes_levels: Dict[float, float],
    no_levels: Dict[float, float],
    ts_dt: datetime,
) -> List[PriceSnapshot]:
    bids_yes, asks_yes, _, _ = _build_books(yes_levels, no_levels)
    best_bid = bids_yes[0][0] if bids_yes else None
    best_ask = asks_yes[0][0] if asks_yes else None
    ts_utc = ts_dt.isoformat()
    label = outcome_label or ""
    return [
        PriceSnapshot(
            venue="kalshi",
            market_id=market_ticker,
            outcome_label=label,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade=None,
            ts_utc=ts_utc,
            raw_json=None,
        )
    ]


def _parse_ws_ts(value: object) -> Optional[datetime]:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _ws_connect_kwargs(headers: Dict[str, str]) -> Dict[str, object]:
    try:
        import inspect
        import websockets  # type: ignore
    except Exception:
        return {"extra_headers": headers}
    try:
        params = inspect.signature(websockets.connect).parameters
    except (TypeError, ValueError):
        return {"extra_headers": headers}
    if "extra_headers" in params:
        return {"extra_headers": headers}
    if "additional_headers" in params:
        return {"additional_headers": headers}
    if "headers" in params:
        return {"headers": headers}
    return {}


def _kalshi_ws_headers(
    api_key: str,
    private_key_path: str,
    ws_url: str,
    hashes_module,
    serialization_module,
    padding_module,
) -> Dict[str, str]:
    with open(private_key_path, "rb") as handle:
        private_key = serialization_module.load_pem_private_key(handle.read(), password=None)
    timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    path = urlparse(ws_url).path or "/"
    message = f"{timestamp}GET{path}"
    signature = private_key.sign(
        message.encode("utf-8"),
        padding_module.PSS(
            mgf=padding_module.MGF1(hashes_module.SHA256()),
            salt_length=padding_module.PSS.DIGEST_LENGTH,
        ),
        hashes_module.SHA256(),
    )
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


def _parse_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
