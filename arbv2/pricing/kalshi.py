import asyncio
import base64
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

from arbv2.config import Config
from arbv2.models import Market, PriceSnapshot
from arbv2.pricing.arb import (
    record_stream_sequence,
    set_stream_backlog_warning,
    set_stream_connected,
    set_stream_health,
    set_stream_subscribed,
    touch_stream_heartbeat,
    update_orderbook,
)
from arbv2.storage import insert_prices


logger = logging.getLogger(__name__)
PRICE_WRITE_QUEUE_MAXSIZE = 1024


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
    ws_url = _normalize_kalshi_ws_url(config.kalshi_ws_url)
    tickers = list(market_by_ticker.keys())
    delta_count = 0

    while True:
        heartbeat_task: Optional[asyncio.Task] = None
        writer_task: Optional[asyncio.Task] = None
        write_queue: Optional[asyncio.Queue] = None
        try:
            ws_headers = _kalshi_ws_headers(
                config.kalshi_api_key,
                config.kalshi_private_key_path,
                ws_url,
                hashes,
                serialization,
                padding,
            )
            connect_kwargs = _ws_connect_kwargs(ws_headers)
            connect_kwargs["ping_interval"] = 20
            connect_kwargs["ping_timeout"] = 20
            async with websockets.connect(ws_url, **connect_kwargs) as ws:
                set_stream_connected("kalshi", True)
                set_stream_subscribed("kalshi", False)
                set_stream_backlog_warning("kalshi", False)
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
                set_stream_subscribed("kalshi", True)
                touch_stream_heartbeat("kalshi")
                heartbeat_task = asyncio.create_task(_connection_heartbeat("kalshi"))
                write_queue = asyncio.Queue(maxsize=PRICE_WRITE_QUEUE_MAXSIZE)
                writer_task = asyncio.create_task(_price_writer("kalshi", db_path, write_queue))
                set_stream_health("kalshi", True)
                async for message in ws:
                    touch_stream_heartbeat("kalshi")
                    payload = _decode_message(message)
                    if payload is not None:
                        record_stream_sequence("kalshi", _extract_sequence(payload))
                        backlog_warning = _extract_backlog_warning(payload)
                        if backlog_warning is not None:
                            set_stream_backlog_warning("kalshi", backlog_warning)
                    snapshots = _handle_ws_message(
                        payload if payload is not None else message,
                        levels_by_ticker,
                        market_id_to_ticker,
                        market_by_ticker,
                    )
                    if snapshots and write_queue is not None:
                        _enqueue_snapshots(write_queue, snapshots, "kalshi")
                    delta_count += 1
                    if delta_count % 200 == 0:
                        logger.debug("Kalshi WS deltas processed=%d", delta_count)
        except asyncio.CancelledError:
            logger.info("Kalshi WS stream cancelled")
            set_stream_health("kalshi", False)
            set_stream_connected("kalshi", False)
            return
        except Exception as exc:
            logger.warning("Kalshi WS stream ended: %s", exc)
            set_stream_health("kalshi", False)
            set_stream_connected("kalshi", False)
            continue
        finally:
            if writer_task and write_queue is not None:
                await _stop_price_writer(writer_task, write_queue, "kalshi")
            if heartbeat_task:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass


def _handle_ws_message(
    message: object,
    levels_by_ticker: Dict[str, Dict[str, Dict[float, float]]],
    market_id_to_ticker: Dict[str, str],
    market_by_ticker: Dict[str, Market],
) -> List[PriceSnapshot]:
    if isinstance(message, dict):
        data = message
    elif isinstance(message, str):
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return []
    else:
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


async def _connection_heartbeat(venue: str, *, interval_seconds: float = 1.0) -> None:
    while True:
        touch_stream_heartbeat(venue)
        await asyncio.sleep(interval_seconds)


def _decode_message(message: str) -> Optional[Dict[str, object]]:
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        return payload
    return None


def _extract_sequence(payload: Dict[str, object]) -> Optional[int]:
    for key in ("seq", "sequence", "sequence_number"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
    msg = payload.get("msg")
    if isinstance(msg, dict):
        for key in ("seq", "sequence", "sequence_number"):
            value = msg.get(key)
            if isinstance(value, int):
                return value
    return None


def _extract_backlog_warning(payload: Dict[str, object]) -> Optional[bool]:
    if payload.get("type") == "warning":
        code = str(payload.get("code") or "").lower()
        text = str(payload.get("msg") or payload.get("message") or "").lower()
        if "backlog" in code or "backlog" in text:
            return True
    status = payload.get("backlog_warning")
    if isinstance(status, bool):
        return status
    return None


def _enqueue_snapshots(write_queue: asyncio.Queue, snapshots: List[PriceSnapshot], venue: str) -> None:
    try:
        write_queue.put_nowait(snapshots)
        if write_queue.maxsize and write_queue.qsize() < (write_queue.maxsize // 2):
            set_stream_backlog_warning(venue, False)
        return
    except asyncio.QueueFull:
        pass

    # Keep the most recent snapshots if the writer falls behind.
    try:
        write_queue.get_nowait()
    except asyncio.QueueEmpty:
        pass
    try:
        write_queue.put_nowait(snapshots)
    except asyncio.QueueFull:
        logger.debug("%s price write queue still full; dropping snapshots=%d", venue, len(snapshots))
        return
    set_stream_backlog_warning(venue, True)


async def _price_writer(venue: str, db_path: str, write_queue: asyncio.Queue) -> None:
    while True:
        batch = await write_queue.get()
        if batch is None:
            return
        merged = list(batch)
        while True:
            try:
                next_batch = write_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if next_batch is None:
                if merged:
                    await asyncio.to_thread(insert_prices, db_path, merged)
                return
            merged.extend(next_batch)
        if not merged:
            continue
        try:
            await asyncio.to_thread(insert_prices, db_path, merged)
        except Exception as exc:
            logger.warning("%s price write failed: %s", venue, exc)


async def _stop_price_writer(writer_task: asyncio.Task, write_queue: asyncio.Queue, venue: str) -> None:
    try:
        write_queue.put_nowait(None)
    except asyncio.QueueFull:
        try:
            write_queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
        try:
            write_queue.put_nowait(None)
        except asyncio.QueueFull:
            logger.debug("%s price writer queue full during shutdown", venue)
            writer_task.cancel()
    try:
        await writer_task
    except asyncio.CancelledError:
        pass


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


def _normalize_kalshi_ws_url(raw_url: str) -> str:
    """Kalshi WS auth signature depends on path; default host-only URL needs v2 path."""
    parsed = urlparse(raw_url)
    if not parsed.path or parsed.path == "/":
        parsed = parsed._replace(path="/trade-api/ws/v2")
    return urlunparse(parsed).rstrip("/")


def _parse_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
