import base64
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from arbv2.config import Config
from arbv2.pricing.polymarket import build_token_map
from arbv2.storage import fetch_markets


logger = logging.getLogger(__name__)

_KALSHI_SUCCESS_STATUSES = {"executed", "filled"}
_KALSHI_TERMINAL_STATUSES = _KALSHI_SUCCESS_STATUSES.union(
    {"canceled", "cancelled", "rejected", "expired"}
)
_POLY_SUCCESS_STATUSES = {"matched", "filled", "executed"}
_POLY_TERMINAL_STATUSES = _POLY_SUCCESS_STATUSES.union(
    {"canceled", "cancelled", "rejected", "expired", "unmatched"}
)

_POLY_CLIENT = None
_POLY_CLIENT_KEY: Optional[tuple] = None
_KALSHI_PRIVATE_KEY_CACHE: Dict[str, object] = {}


@dataclass
class _ExecutionError(Exception):
    error_type: str
    detail: str

    def __str__(self) -> str:
        return f"{self.error_type}: {self.detail}"


def execute_confirmed_signal(
    config: Config,
    *,
    db_path: str,
    signal: Dict[str, object],
    decision: Dict[str, object],
    intent: Dict[str, object],
    max_order_usd: float,
    confirm_timeout_seconds: float = 8.0,
) -> Dict[str, object]:
    legs = signal.get("legs") or []
    submitted_ts = int(time.time() * 1000)
    result: Dict[str, object] = {
        "execution_id": uuid.uuid4().hex,
        "intent_id": intent.get("intent_id"),
        "type": "ORDER_EXECUTION",
        "event_id": intent.get("event_id") or signal.get("event_id"),
        "venues": [leg.get("venue") for leg in legs],
        "markets": [leg.get("market_id") for leg in legs],
        "sides": [leg.get("side") for leg in legs],
        "limit_prices": decision.get("limit_prices") or [leg.get("eff_vwap") for leg in legs],
        "size_usd": intent.get("size_usd") or signal.get("size"),
        "detected_ts": intent.get("detected_ts") or signal.get("detected_ts"),
        "confirmed_at_ts": intent.get("confirmed_at_ts"),
        "max_order_usd": max_order_usd,
        "contracts": None,
        "target_spend_usd": None,
        "actual_spend_usd": None,
        "order_submitted_ts": submitted_ts,
        "order_confirmed_ts": submitted_ts,
        "order_confirm_ms": 0,
        "status": "error",
        "error_type": None,
        "error_detail": None,
        "leg_results": [],
    }
    try:
        if not isinstance(legs, list) or not legs:
            raise _ExecutionError("missing_legs", "Signal legs are required for execution.")
        prices = _normalize_limit_prices(result["limit_prices"], len(legs))
        if not prices:
            raise _ExecutionError("missing_limit_prices", "Unable to compute order prices.")

        contracts = _contracts_for_budget(
            max_order_usd=max_order_usd,
            signal_size=_to_float(signal.get("size")),
            effective_prices=prices,
        )
        if contracts <= 0:
            raise _ExecutionError("budget_too_small", "Budget is too small for at least one paired contract.")
        total_price = sum(prices)
        result["contracts"] = contracts
        result["target_spend_usd"] = max_order_usd
        result["actual_spend_usd"] = contracts * total_price

        leg_results: List[Dict[str, object]] = []
        failure: Optional[Dict[str, object]] = None
        for idx, leg in enumerate(legs):
            if failure:
                leg_results.append(
                    {
                        "venue": leg.get("venue"),
                        "market_id": leg.get("market_id"),
                        "side": leg.get("side"),
                        "status": "skipped_after_failure",
                        "order_id": None,
                        "submitted_ts": None,
                        "confirmed_ts": None,
                        "error_type": "skipped_after_failure",
                        "error_detail": "A previous leg failed; subsequent orders were not submitted.",
                    }
                )
                continue
            leg_result = _execute_leg(
                config,
                db_path=db_path,
                leg=leg,
                limit_price=prices[idx],
                contracts=contracts,
                timeout_seconds=confirm_timeout_seconds,
            )
            leg_results.append(leg_result)
            if not leg_result.get("ok"):
                failure = leg_result

        result["leg_results"] = leg_results
        confirmed_candidates = [lr.get("confirmed_ts") for lr in leg_results if _to_int(lr.get("confirmed_ts")) is not None]
        if confirmed_candidates:
            result["order_confirmed_ts"] = max(int(ts) for ts in confirmed_candidates)
        else:
            result["order_confirmed_ts"] = int(time.time() * 1000)
        result["order_confirm_ms"] = max(
            0,
            int(result["order_confirmed_ts"]) - int(result["order_submitted_ts"]),
        )
        if failure:
            result["status"] = "error"
            result["error_type"] = failure.get("error_type")
            result["error_detail"] = failure.get("error_detail")
            return result
        result["status"] = "confirmed"
        return result
    except _ExecutionError as exc:
        result["error_type"] = exc.error_type
        result["error_detail"] = exc.detail
        result["order_confirmed_ts"] = int(time.time() * 1000)
        result["order_confirm_ms"] = max(
            0,
            int(result["order_confirmed_ts"]) - int(result["order_submitted_ts"]),
        )
        return result
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Order execution failed unexpectedly")
        result["error_type"] = "unexpected_exception"
        result["error_detail"] = str(exc)
        result["order_confirmed_ts"] = int(time.time() * 1000)
        result["order_confirm_ms"] = max(
            0,
            int(result["order_confirmed_ts"]) - int(result["order_submitted_ts"]),
        )
        return result


def _execute_leg(
    config: Config,
    *,
    db_path: str,
    leg: Dict[str, object],
    limit_price: float,
    contracts: int,
    timeout_seconds: float,
) -> Dict[str, object]:
    venue = str(leg.get("venue") or "")
    market_id = str(leg.get("market_id") or "")
    side = str(leg.get("side") or "YES").upper()
    outcome_label = str(leg.get("outcome_label") or "")
    submitted_ts = int(time.time() * 1000)
    base = {
        "venue": venue,
        "market_id": market_id,
        "side": side,
        "outcome_label": outcome_label,
        "limit_price": limit_price,
        "contracts": contracts,
        "submitted_ts": submitted_ts,
        "confirmed_ts": submitted_ts,
        "ok": False,
        "status": "error",
        "order_id": None,
        "error_type": None,
        "error_detail": None,
    }
    if not venue or not market_id:
        base["error_type"] = "invalid_leg"
        base["error_detail"] = "Missing venue or market_id on signal leg."
        return base
    if side not in {"YES", "NO"}:
        base["error_type"] = "invalid_side"
        base["error_detail"] = f"Unsupported side: {side}"
        return base
    try:
        if venue == "kalshi":
            order_id, status, confirmed_ts = _place_kalshi_order(
                config,
                market_id=market_id,
                side=side,
                limit_price=limit_price,
                contracts=contracts,
                timeout_seconds=timeout_seconds,
            )
        elif venue == "polymarket":
            order_id, status, confirmed_ts = _place_polymarket_order(
                config,
                db_path=db_path,
                market_id=market_id,
                outcome_label=outcome_label,
                limit_price=limit_price,
                contracts=contracts,
                timeout_seconds=timeout_seconds,
            )
        else:
            raise _ExecutionError("unsupported_venue", f"Unsupported venue: {venue}")
        base["order_id"] = order_id
        base["status"] = status
        base["confirmed_ts"] = confirmed_ts
        base["ok"] = True
        return base
    except _ExecutionError as exc:
        base["error_type"] = exc.error_type
        base["error_detail"] = exc.detail
        base["confirmed_ts"] = int(time.time() * 1000)
        return base
    except Exception as exc:  # pragma: no cover - defensive logging
        base["error_type"] = "unexpected_exception"
        base["error_detail"] = str(exc)
        base["confirmed_ts"] = int(time.time() * 1000)
        return base


def _contracts_for_budget(
    *,
    max_order_usd: float,
    signal_size: Optional[float],
    effective_prices: List[float],
) -> int:
    if max_order_usd <= 0:
        return 0
    total_price = sum(effective_prices)
    if total_price <= 0:
        return 0
    contracts_from_budget = int(max_order_usd / total_price)
    if contracts_from_budget <= 0:
        return 0
    if signal_size is None or signal_size <= 0:
        return contracts_from_budget
    return min(contracts_from_budget, int(signal_size))


def _normalize_limit_prices(raw_prices: object, legs_count: int) -> List[float]:
    if not isinstance(raw_prices, list):
        return []
    values: List[float] = []
    for idx in range(legs_count):
        if idx >= len(raw_prices):
            return []
        price = _to_float(raw_prices[idx])
        if price is None or price <= 0:
            return []
        values.append(price)
    return values


def _place_kalshi_order(
    config: Config,
    *,
    market_id: str,
    side: str,
    limit_price: float,
    contracts: int,
    timeout_seconds: float,
) -> Tuple[str, str, int]:
    if not config.kalshi_api_key:
        raise _ExecutionError("missing_kalshi_api_key", "KALSHI_API_KEY is required for order execution.")
    if not config.kalshi_private_key_path:
        raise _ExecutionError(
            "missing_kalshi_private_key",
            "KALSHI_PRIVATE_KEY_PATH is required for order execution.",
        )
    if contracts <= 0:
        raise _ExecutionError("invalid_contracts", "Contracts must be positive.")
    side_token = "yes" if side == "YES" else "no"
    price_cents = max(1, min(99, int(round(limit_price * 100))))
    payload = {
        "ticker": market_id,
        "client_order_id": uuid.uuid4().hex[:32],
        "type": "limit",
        "action": "buy",
        "side": side_token,
        "count": int(contracts),
        "time_in_force": "fill_or_kill",
    }
    if side_token == "yes":
        payload["yes_price"] = price_cents
    else:
        payload["no_price"] = price_cents

    path = "/portfolio/orders"
    url = config.kalshi_base_url.rstrip("/") + path
    headers = _kalshi_headers(config, method="POST", path=path)
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=config.http_timeout_seconds)
    except requests.RequestException as exc:
        raise _ExecutionError("kalshi_request_error", str(exc))
    if resp.status_code != 200:
        raise _ExecutionError(
            f"kalshi_http_{resp.status_code}",
            resp.text.strip()[:500],
        )
    data = _safe_json(resp)
    order_id = _extract_order_id(data)
    if not order_id:
        raise _ExecutionError("kalshi_missing_order_id", _json_compact(data)[:500])
    status, confirmed_ts = _poll_kalshi_status(config, order_id, timeout_seconds=timeout_seconds)
    if status not in _KALSHI_SUCCESS_STATUSES:
        raise _ExecutionError(f"kalshi_{status}", f"Order {order_id} finished with status={status}")
    return order_id, status, confirmed_ts


def _poll_kalshi_status(config: Config, order_id: str, *, timeout_seconds: float) -> Tuple[str, int]:
    deadline = time.time() + max(timeout_seconds, 0.1)
    path = f"/portfolio/orders/{order_id}"
    url = config.kalshi_base_url.rstrip("/") + path
    last_status = "unknown"
    while time.time() < deadline:
        headers = _kalshi_headers(config, method="GET", path=path)
        try:
            resp = requests.get(url, headers=headers, timeout=config.http_timeout_seconds)
        except requests.RequestException:
            time.sleep(0.2)
            continue
        if resp.status_code != 200:
            time.sleep(0.2)
            continue
        data = _safe_json(resp)
        status = _extract_status(data).lower()
        if status:
            last_status = status
        if status in _KALSHI_TERMINAL_STATUSES:
            return status, int(time.time() * 1000)
        time.sleep(0.2)
    return "timeout" if last_status == "unknown" else last_status, int(time.time() * 1000)


def _kalshi_headers(config: Config, *, method: str, path: str) -> Dict[str, str]:
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency is required at runtime
        raise _ExecutionError("missing_cryptography", str(exc))
    private_key = _KALSHI_PRIVATE_KEY_CACHE.get(config.kalshi_private_key_path)
    if private_key is None:
        with open(config.kalshi_private_key_path, "rb") as handle:
            private_key = serialization.load_pem_private_key(handle.read(), password=None)
        _KALSHI_PRIVATE_KEY_CACHE[config.kalshi_private_key_path] = private_key
    timestamp = str(int(time.time() * 1000))
    parsed = urlparse(path)
    signed_path = parsed.path or "/"
    if parsed.query:
        signed_path += f"?{parsed.query}"
    message = f"{timestamp}{method.upper()}{signed_path}"
    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": config.kalshi_api_key,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


def _place_polymarket_order(
    config: Config,
    *,
    db_path: str,
    market_id: str,
    outcome_label: str,
    limit_price: float,
    contracts: int,
    timeout_seconds: float,
) -> Tuple[str, str, int]:
    if contracts <= 0:
        raise _ExecutionError("invalid_contracts", "Contracts must be positive.")
    token_id = _resolve_polymarket_token_id(db_path, market_id, outcome_label)
    if not token_id:
        raise _ExecutionError(
            "missing_polymarket_token",
            f"Unable to resolve token for market_id={market_id} outcome={outcome_label}",
        )
    client = _get_polymarket_client(config)
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.exceptions import PolyApiException
        from py_clob_client.order_builder.constants import BUY
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency is required at runtime
        raise _ExecutionError("missing_py_clob_client", str(exc))
    try:
        order = client.create_order(
            OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=float(contracts),
                side=BUY,
            )
        )
        post_response = client.post_order(order, OrderType.FOK)
    except PolyApiException as exc:
        raise _ExecutionError("polymarket_api_error", str(exc))
    except Exception as exc:
        raise _ExecutionError("polymarket_order_error", str(exc))

    order_id = _extract_order_id(post_response)
    if not order_id:
        raise _ExecutionError("polymarket_missing_order_id", _json_compact(post_response)[:500])
    status, confirmed_ts = _poll_polymarket_status(client, order_id, timeout_seconds=timeout_seconds)
    if status not in _POLY_SUCCESS_STATUSES:
        raise _ExecutionError(f"polymarket_{status}", f"Order {order_id} finished with status={status}")
    return order_id, status, confirmed_ts


def _poll_polymarket_status(client, order_id: str, *, timeout_seconds: float) -> Tuple[str, int]:
    deadline = time.time() + max(timeout_seconds, 0.1)
    last_status = "unknown"
    while time.time() < deadline:
        try:
            data = client.get_order(order_id)
        except Exception:
            time.sleep(0.2)
            continue
        status = _extract_status(data).lower()
        if status:
            last_status = status
        if status in _POLY_TERMINAL_STATUSES:
            return status, int(time.time() * 1000)
        time.sleep(0.2)
    return "timeout" if last_status == "unknown" else last_status, int(time.time() * 1000)


def _get_polymarket_client(config: Config):
    global _POLY_CLIENT, _POLY_CLIENT_KEY
    if not config.polymarket_private_key:
        raise _ExecutionError(
            "missing_polymarket_private_key",
            "POLY_PRIVATE_KEY is required for order execution.",
        )
    if not config.polymarket_clob_api_key or not config.polymarket_clob_api_secret or not config.polymarket_clob_api_passphrase:
        raise _ExecutionError(
            "missing_polymarket_api_creds",
            "POLY_CLOB_API_KEY/POLY_CLOB_API_SECRET/POLY_CLOB_API_PASSPHRASE are required.",
        )
    key = (
        config.polymarket_clob_rest_url,
        config.polymarket_chain_id,
        config.polymarket_private_key,
        config.polymarket_signature_type,
        config.polymarket_funder,
        config.polymarket_clob_api_key,
        config.polymarket_clob_api_secret,
        config.polymarket_clob_api_passphrase,
    )
    if _POLY_CLIENT is not None and _POLY_CLIENT_KEY == key:
        return _POLY_CLIENT
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency is required at runtime
        raise _ExecutionError("missing_py_clob_client", str(exc))
    creds = ApiCreds(
        api_key=config.polymarket_clob_api_key,
        api_secret=config.polymarket_clob_api_secret,
        api_passphrase=config.polymarket_clob_api_passphrase,
    )
    client = ClobClient(
        config.polymarket_clob_rest_url,
        chain_id=config.polymarket_chain_id,
        key=config.polymarket_private_key,
        creds=creds,
        signature_type=config.polymarket_signature_type,
        funder=config.polymarket_funder or None,
    )
    client.set_api_creds(creds)
    _POLY_CLIENT = client
    _POLY_CLIENT_KEY = key
    return client


def _resolve_polymarket_token_id(db_path: str, market_id: str, outcome_label: str) -> Optional[str]:
    markets = fetch_markets(db_path, venue="polymarket")
    token_map = build_token_map(markets)
    target_outcome = _normalize_label(outcome_label)
    for token_id, targets in token_map.items():
        for market, label in targets:
            if market.market_id != market_id:
                continue
            if _normalize_label(label) == target_outcome:
                return token_id
    return None


def _normalize_label(value: object) -> str:
    return str(value or "").strip().upper()


def _extract_order_id(payload: object) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for key in ("orderID", "orderId", "order_id", "id"):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value)
    order = payload.get("order")
    if isinstance(order, dict):
        for key in ("orderID", "orderId", "order_id", "id"):
            value = order.get(key)
            if value is not None and str(value).strip():
                return str(value)
    return None


def _extract_status(payload: object) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("status", "order_status", "state"):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value)
    order = payload.get("order")
    if isinstance(order, dict):
        for key in ("status", "order_status", "state"):
            value = order.get(key)
            if value is not None and str(value).strip():
                return str(value)
    return ""


def _safe_json(resp: requests.Response) -> Dict[str, object]:
    try:
        data = resp.json()
    except ValueError:
        return {"raw": resp.text}
    if isinstance(data, dict):
        return data
    return {"raw": data}


def _json_compact(value: object) -> str:
    try:
        return json.dumps(value, separators=(",", ":"), default=str)
    except TypeError:
        return str(value)


def _to_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
