import hashlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple


ORDERBOOKS: Dict[Tuple[str, str, str], Dict[str, object]] = {}

logger = logging.getLogger(__name__)

KALSHI_FEE_BPS = float(os.getenv("ARBV2_KALSHI_FEE_BPS", "100"))
POLY_FEE_BPS = float(os.getenv("ARBV2_POLY_FEE_BPS", "0"))
SLIPPAGE_BPS = float(os.getenv("ARBV2_ARB_SLIPPAGE_BPS", "10"))
MAX_STALENESS_SECONDS = float(os.getenv("ARBV2_ARB_MAX_STALENESS_SECONDS", "3"))
MAX_SYNC_SKEW_SECONDS = float(os.getenv("ARBV2_ARB_MAX_SYNC_SKEW_SECONDS", "2"))
POLY_MAX_AGE_SECONDS = 1.5
KALSHI_MAX_AGE_SECONDS = 5.0
EVENT_MIN_ROI = float(os.getenv("ARBV2_ARB_EVENT_MIN_ROI", "0.008"))
EVENT_MIN_ABS_PROFIT = float(os.getenv("ARBV2_ARB_EVENT_MIN_ABS_PROFIT", "1.0"))
BINARY_MIN_ROI = float(os.getenv("ARBV2_ARB_BINARY_MIN_ROI", "0.008"))
BINARY_MIN_ABS_PROFIT = float(os.getenv("ARBV2_ARB_BINARY_MIN_ABS_PROFIT", "1.0"))
Q_MIN = float(os.getenv("ARBV2_ARB_Q_MIN", "100"))
Q_MAX = float(os.getenv("ARBV2_ARB_Q_MAX", "5000"))
COOLDOWN_SECONDS = float(os.getenv("ARBV2_ARB_COOLDOWN_SECONDS", "45"))
HYSTERESIS_BPS = float(os.getenv("ARBV2_ARB_HYSTERESIS_BPS", "200"))
EVENT_LOCK_SECONDS = float(os.getenv("ARBV2_ARB_EVENT_LOCK_SECONDS", "20"))
EVENT_LOCK_IMPROVEMENT_BPS = float(os.getenv("ARBV2_ARB_EVENT_LOCK_IMPROVEMENT_BPS", "300"))
CONFIRM_MODE = os.getenv("ARBV2_CONFIRM_MODE", "on_trigger")
TRIGGER_CONFIRM_TTL_MS = int(os.getenv("ARBV2_TRIGGER_CONFIRM_TTL_MS", "2500"))
MAX_SLIPPAGE_BPS = int(os.getenv("ARBV2_MAX_SLIPPAGE_BPS", "20"))
MIN_EDGE_BPS = int(os.getenv("ARBV2_MIN_EDGE_BPS", "40"))
RECENT_FIRE_TTL_MS = int(os.getenv("ARBV2_RECENT_FIRE_TTL_MS", "5000"))
IDEMPOTENCY_BUCKET_MS = int(os.getenv("ARBV2_IDEMPOTENCY_BUCKET_MS", "2000"))
POST_CONFIRM_ENABLED = os.getenv("ARBV2_POST_CONFIRM_ENABLED", "true") == "true"
POST_CONFIRM_WINDOW_MS = int(os.getenv("ARBV2_POST_CONFIRM_WINDOW_MS", "400"))
POST_CONFIRM_MAX_EDGE_DECAY_BPS = int(os.getenv("ARBV2_POST_CONFIRM_MAX_EDGE_DECAY_BPS", "50"))


class ArbMode(str, Enum):
    EVENT_OUTCOME = "EVENT_OUTCOME"
    BINARY_MIRROR = "BINARY_MIRROR"


@dataclass
class EventMarkets:
    kalshi_by_outcome: Dict[str, str]
    polymarket_by_outcome: Dict[str, str]
    outcomes: List[str]
    event_id: Optional[str] = None
    outcome_id: Optional[str] = None


class SignalSuppressor:
    def __init__(
        self,
        *,
        cooldown_seconds: float = COOLDOWN_SECONDS,
        hysteresis_bps: float = HYSTERESIS_BPS,
        event_lock_seconds: float = EVENT_LOCK_SECONDS,
        improvement_bps: float = EVENT_LOCK_IMPROVEMENT_BPS,
    ) -> None:
        self.cooldown_seconds = cooldown_seconds
        self.hysteresis_bps = hysteresis_bps
        self.event_lock_seconds = event_lock_seconds
        self.improvement_bps = improvement_bps
        self.last_emit: Dict[Tuple[object, ...], Dict[str, float]] = {}
        self.event_lock: Dict[Tuple[str, str], Dict[str, object]] = {}

    def should_emit(
        self,
        *,
        arb_type: ArbMode,
        key: Tuple[object, ...],
        event_key: Optional[str],
        edge_bps: float,
        size: float,
        now_ts: float,
    ) -> Tuple[bool, str]:
        # Direction is excluded so any EVENT_OUTCOME for the event shares the same lock.
        lock_key = (event_key or "UNKNOWN", arb_type.value)
        lock = self.event_lock.get(lock_key)
        if lock:
            age = now_ts - float(lock["ts"])
            if age < self.event_lock_seconds:
                if key != lock["best_key"] and edge_bps < float(lock["best_edge_bps"]) + self.improvement_bps:
                    return False, "event_lock"

        last = self.last_emit.get(key)
        if last:
            age = now_ts - float(last["ts"])
            if age < self.cooldown_seconds:
                last_edge = float(last["edge_bps"])
                last_size = float(last.get("size", 0.0))
                improvement = edge_bps - last_edge
                if edge_bps < last_edge + self.hysteresis_bps:
                    return False, "cooldown"
                if size < last_size and improvement < self.hysteresis_bps:
                    return False, "cooldown"

        self.last_emit[key] = {"ts": now_ts, "edge_bps": edge_bps, "size": size}
        if event_key:
            if not lock or edge_bps >= float(lock["best_edge_bps"]):
                self.event_lock[lock_key] = {
                    "ts": now_ts,
                    "best_key": key,
                    "best_edge_bps": edge_bps,
                }
        return True, ""


SIGNAL_SUPPRESSOR = SignalSuppressor()
CONFIRM_IDEMPOTENCY: Dict[str, int] = {}
POST_CONFIRM_CACHE: Dict[str, Dict[str, float]] = {}


def update_orderbook(
    venue: str,
    market_id: str,
    outcome_label: str,
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    last_update_ts_utc: datetime,
) -> None:
    key = (venue, market_id, outcome_label)
    ORDERBOOKS[key] = {
        "bids": sorted(bids, key=lambda x: x[0], reverse=True),
        "asks": sorted(asks, key=lambda x: x[0]),
        "last_update_ts_utc": _ensure_utc(last_update_ts_utc),
        "fetched_at_ts": time.time(),
    }


def get_fill_stats(
    venue: str,
    market_id: str,
    outcome_label: str,
    side: str,
    Q: float,
) -> Dict[str, object]:
    if Q <= 0:
        return {"vwap_price": None, "worst_price": None, "filled_qty": 0.0, "ok": False}
    book = ORDERBOOKS.get((venue, market_id, outcome_label))
    if not book:
        return {"vwap_price": None, "worst_price": None, "filled_qty": 0.0, "ok": False}
    side = side.upper()
    if side not in {"YES", "NO"}:
        raise ValueError("Side must be YES or NO")
    asks = book.get("asks") or []
    filled_qty = 0.0
    cost = 0.0
    worst = None
    for price, qty in asks:
        if qty <= 0:
            continue
        take = min(qty, Q - filled_qty)
        cost += price * take
        filled_qty += take
        worst = price
        if filled_qty >= Q:
            break
    if filled_qty < Q or worst is None:
        return {"vwap_price": None, "worst_price": None, "filled_qty": filled_qty, "ok": False}
    vwap = cost / Q
    return {"vwap_price": vwap, "worst_price": worst, "filled_qty": filled_qty, "ok": True}


def apply_fees(venue: str, raw_price: Optional[float], Q: float) -> Optional[float]:
    if raw_price is None:
        return None
    price = raw_price
    if venue == "kalshi":
        price *= 1 + KALSHI_FEE_BPS / 10_000
    if venue == "polymarket":
        price *= 1 + POLY_FEE_BPS / 10_000
    if SLIPPAGE_BPS > 0:
        price *= 1 + SLIPPAGE_BPS / 10_000
    return price


def evaluate_arb_at_size(
    event: EventMarkets,
    arb_mode: ArbMode,
    Q: float,
    now_utc: Optional[datetime] = None,
) -> Optional[Dict[str, object]]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if Q <= 0:
        return None
    if arb_mode == ArbMode.EVENT_OUTCOME:
        return _evaluate_event_outcome(event, Q, now_utc)
    if arb_mode == ArbMode.BINARY_MIRROR:
        return _evaluate_binary_mirror(event, Q, now_utc)
    return None


def evaluate_profit_at_size(
    event: EventMarkets,
    arb_mode: ArbMode,
    Q: float,
    now_utc: Optional[datetime] = None,
) -> Optional[Dict[str, object]]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if Q <= 0:
        return None
    if arb_mode == ArbMode.EVENT_OUTCOME:
        return _profit_event_outcome(event, Q, now_utc)
    if arb_mode == ArbMode.BINARY_MIRROR:
        return _profit_binary_mirror(event, Q, now_utc)
    return None


def evaluate_profit_rows(
    event: EventMarkets,
    arb_mode: ArbMode,
    Q: float,
    now_utc: Optional[datetime] = None,
) -> List[Dict[str, object]]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if Q <= 0:
        return []
    if arb_mode == ArbMode.EVENT_OUTCOME:
        return _profit_event_outcome_rows(event, Q, now_utc)
    if arb_mode == ArbMode.BINARY_MIRROR:
        return _profit_binary_mirror_rows(event, Q, now_utc)
    return []


def find_best_arb(
    event: EventMarkets,
    arb_mode: ArbMode,
    Q_min: float = Q_MIN,
    Q_max: float = Q_MAX,
) -> Optional[Dict[str, object]]:
    if Q_min <= 0 or Q_max < Q_min:
        return None
    best = None
    Q = Q_min
    while Q <= Q_max:
        result = evaluate_arb_at_size(event, arb_mode, Q)
        if result is None:
            break
        best = result
        Q *= 2
    return best


def _maybe_build_signal(best: Optional[Dict[str, object]], best_profit, candidate: Optional[Dict[str, object]]):
    if not candidate:
        return best
    if best is None:
        return candidate
    if candidate["profit"] > best_profit:
        return candidate
    return best


def _direction_signal(
    event: EventMarkets,
    arb_type: ArbMode,
    outcome_label: str,
    direction: str,
    legs: List[Dict[str, object]],
    eff_prices: List[Optional[float]],
    Q: float,
    now_utc: datetime,
) -> Optional[Dict[str, object]]:
    if any(price is None for price in eff_prices):
        return None
    total_price = sum(eff_prices)
    if total_price >= 1.0:
        return None
    total_cost = Q * total_price
    payout = Q * 1.0
    profit = payout - total_cost
    if profit <= 0:
        return None
    roi = profit / total_cost if total_cost else 0.0
    min_roi, min_profit = _thresholds(arb_type)
    if roi < min_roi or profit < min_profit:
        return None
    return {
        "arb_type": arb_type.value,
        "event_id": event.event_id,
        "market_id": outcome_label,
        "outcome_label": outcome_label,
        "direction": direction,
        "size": Q,
        "legs": legs,
        "total_cost": total_cost,
        "payout": payout,
        "profit": profit,
        "roi": roi,
        "ts_utc": now_utc.isoformat(),
    }


def _profit_signal(
    event: EventMarkets,
    arb_type: ArbMode,
    kalshi_market_id: str,
    polymarket_market_id: str,
    direction: str,
    eff_prices: List[Optional[float]],
    Q: float,
    now_utc: datetime,
) -> Optional[Dict[str, object]]:
    if any(price is None for price in eff_prices):
        return None
    total_price = sum(eff_prices)
    total_cost = Q * total_price
    payout = Q * 1.0
    profit = payout - total_cost
    roi = profit / total_cost if total_cost else 0.0
    return {
        "arb_type": arb_type.value,
        "event_id": event.event_id,
        "kalshi_market_id": kalshi_market_id,
        "polymarket_market_id": polymarket_market_id,
        "direction": direction,
        "size": Q,
        "total_cost": total_cost,
        "payout": payout,
        "profit": profit,
        "roi": roi,
        "ts_utc": now_utc.isoformat(),
    }


def _evaluate_event_outcome(event: EventMarkets, Q: float, now_utc: datetime) -> Optional[Dict[str, object]]:
    if len(event.outcomes) != 2:
        return None
    keys = [
        ("kalshi", event.kalshi_by_outcome.get(event.outcomes[0]), event.outcomes[0]),
        ("kalshi", event.kalshi_by_outcome.get(event.outcomes[1]), event.outcomes[1]),
        ("polymarket", event.polymarket_by_outcome.get(event.outcomes[0]), event.outcomes[0]),
        ("polymarket", event.polymarket_by_outcome.get(event.outcomes[1]), event.outcomes[1]),
    ]
    if not _books_fresh_by_venue(keys):
        return None
    outcome_a, outcome_b = event.outcomes
    if not _books_fresh(
        [
            ("kalshi", event.kalshi_by_outcome.get(outcome_a), outcome_a),
            ("kalshi", event.kalshi_by_outcome.get(outcome_b), outcome_b),
            ("polymarket", event.polymarket_by_outcome.get(outcome_a), outcome_a),
            ("polymarket", event.polymarket_by_outcome.get(outcome_b), outcome_b),
        ],
        now_utc,
    ):
        return None

    k_a = event.kalshi_by_outcome.get(outcome_a)
    k_b = event.kalshi_by_outcome.get(outcome_b)
    p_a = event.polymarket_by_outcome.get(outcome_a)
    p_b = event.polymarket_by_outcome.get(outcome_b)
    if not k_a or not k_b or not p_a or not p_b:
        return None

    k_a_stats = get_fill_stats("kalshi", k_a, outcome_a, "YES", Q)
    k_b_stats = get_fill_stats("kalshi", k_b, outcome_b, "YES", Q)
    p_a_stats = get_fill_stats("polymarket", p_a, outcome_a, "YES", Q)
    p_b_stats = get_fill_stats("polymarket", p_b, outcome_b, "YES", Q)
    if not all(stat["ok"] for stat in (k_a_stats, k_b_stats, p_a_stats, p_b_stats)):
        return None

    raw_k_a = k_a_stats["vwap_price"]
    raw_k_b = k_b_stats["vwap_price"]
    raw_p_a = p_a_stats["vwap_price"]
    raw_p_b = p_b_stats["vwap_price"]
    eff_k_a = apply_fees("kalshi", raw_k_a, Q)
    eff_k_b = apply_fees("kalshi", raw_k_b, Q)
    eff_p_a = apply_fees("polymarket", raw_p_a, Q)
    eff_p_b = apply_fees("polymarket", raw_p_b, Q)

    best = None
    best_profit = None

    legs = [
        {
            "venue": "kalshi",
            "side": "YES",
            "market_id": k_a,
            "outcome_label": outcome_a,
            "raw_vwap": raw_k_a,
            "eff_vwap": eff_k_a,
            "worst_price": k_a_stats["worst_price"],
        },
        {
            "venue": "polymarket",
            "side": "YES",
            "market_id": p_b,
            "outcome_label": outcome_b,
            "raw_vwap": raw_p_b,
            "eff_vwap": eff_p_b,
            "worst_price": p_b_stats["worst_price"],
        },
    ]
    best = _maybe_build_signal(
        best,
        best_profit,
        _direction_signal(
            event,
            ArbMode.EVENT_OUTCOME,
            f"{outcome_a}|{outcome_b}",
            "KALSHI:A + POLY:B",
            legs,
            [eff_k_a, eff_p_b],
            Q,
            now_utc,
        ),
    )
    if best:
        best_profit = best["profit"]

    legs = [
        {
            "venue": "polymarket",
            "side": "YES",
            "market_id": p_a,
            "outcome_label": outcome_a,
            "raw_vwap": raw_p_a,
            "eff_vwap": eff_p_a,
            "worst_price": p_a_stats["worst_price"],
        },
        {
            "venue": "kalshi",
            "side": "YES",
            "market_id": k_b,
            "outcome_label": outcome_b,
            "raw_vwap": raw_k_b,
            "eff_vwap": eff_k_b,
            "worst_price": k_b_stats["worst_price"],
        },
    ]
    best = _maybe_build_signal(
        best,
        best_profit,
        _direction_signal(
            event,
            ArbMode.EVENT_OUTCOME,
            f"{outcome_a}|{outcome_b}",
            "POLY:A + KALSHI:B",
            legs,
            [eff_p_a, eff_k_b],
            Q,
            now_utc,
        ),
    )
    return best


def _evaluate_binary_mirror(event: EventMarkets, Q: float, now_utc: datetime) -> Optional[Dict[str, object]]:
    if len(event.outcomes) != 2:
        return None
    keys = [
        ("kalshi", event.kalshi_by_outcome.get("YES"), "YES"),
        ("kalshi", event.kalshi_by_outcome.get("NO"), "NO"),
        ("polymarket", event.polymarket_by_outcome.get("YES"), "YES"),
        ("polymarket", event.polymarket_by_outcome.get("NO"), "NO"),
    ]
    if not _books_fresh_by_venue(keys):
        return None
    if not _books_fresh(
        [
            ("kalshi", event.kalshi_by_outcome.get("YES"), "YES"),
            ("kalshi", event.kalshi_by_outcome.get("NO"), "NO"),
            ("polymarket", event.polymarket_by_outcome.get("YES"), "YES"),
            ("polymarket", event.polymarket_by_outcome.get("NO"), "NO"),
        ],
        now_utc,
    ):
        return None

    k_yes = event.kalshi_by_outcome.get("YES")
    k_no = event.kalshi_by_outcome.get("NO")
    p_yes = event.polymarket_by_outcome.get("YES")
    p_no = event.polymarket_by_outcome.get("NO")
    if not k_yes or not k_no or not p_yes or not p_no:
        return None

    k_yes_stats = get_fill_stats("kalshi", k_yes, "YES", "YES", Q)
    k_no_stats = get_fill_stats("kalshi", k_no, "NO", "NO", Q)
    p_yes_stats = get_fill_stats("polymarket", p_yes, "YES", "YES", Q)
    p_no_stats = get_fill_stats("polymarket", p_no, "NO", "NO", Q)
    if not all(stat["ok"] for stat in (k_yes_stats, k_no_stats, p_yes_stats, p_no_stats)):
        return None

    raw_k_yes = k_yes_stats["vwap_price"]
    raw_k_no = k_no_stats["vwap_price"]
    raw_p_yes = p_yes_stats["vwap_price"]
    raw_p_no = p_no_stats["vwap_price"]
    eff_k_yes = apply_fees("kalshi", raw_k_yes, Q)
    eff_k_no = apply_fees("kalshi", raw_k_no, Q)
    eff_p_yes = apply_fees("polymarket", raw_p_yes, Q)
    eff_p_no = apply_fees("polymarket", raw_p_no, Q)

    best = None
    best_profit = None

    legs = [
        {
            "venue": "kalshi",
            "side": "YES",
            "market_id": k_yes,
            "outcome_label": "YES",
            "raw_vwap": raw_k_yes,
            "eff_vwap": eff_k_yes,
            "worst_price": k_yes_stats["worst_price"],
        },
        {
            "venue": "polymarket",
            "side": "NO",
            "market_id": p_no,
            "outcome_label": "NO",
            "raw_vwap": raw_p_no,
            "eff_vwap": eff_p_no,
            "worst_price": p_no_stats["worst_price"],
        },
    ]
    best = _maybe_build_signal(
        best,
        best_profit,
        _direction_signal(
            event,
            ArbMode.BINARY_MIRROR,
            "YES/NO",
            "KALSHI:YES + POLY:NO",
            legs,
            [eff_k_yes, eff_p_no],
            Q,
            now_utc,
        ),
    )
    if best:
        best_profit = best["profit"]

    legs = [
        {
            "venue": "polymarket",
            "side": "YES",
            "market_id": p_yes,
            "outcome_label": "YES",
            "raw_vwap": raw_p_yes,
            "eff_vwap": eff_p_yes,
            "worst_price": p_yes_stats["worst_price"],
        },
        {
            "venue": "kalshi",
            "side": "NO",
            "market_id": k_no,
            "outcome_label": "NO",
            "raw_vwap": raw_k_no,
            "eff_vwap": eff_k_no,
            "worst_price": k_no_stats["worst_price"],
        },
    ]
    best = _maybe_build_signal(
        best,
        best_profit,
        _direction_signal(
            event,
            ArbMode.BINARY_MIRROR,
            "YES/NO",
            "POLY:YES + KALSHI:NO",
            legs,
            [eff_p_yes, eff_k_no],
            Q,
            now_utc,
        ),
    )
    return best


def _profit_event_outcome(event: EventMarkets, Q: float, now_utc: datetime) -> Optional[Dict[str, object]]:
    rows = _profit_event_outcome_rows(event, Q, now_utc)
    if not rows:
        return None
    return max(rows, key=lambda item: item["profit"])


def _profit_binary_mirror(event: EventMarkets, Q: float, now_utc: datetime) -> Optional[Dict[str, object]]:
    rows = _profit_binary_mirror_rows(event, Q, now_utc)
    if not rows:
        return None
    return max(rows, key=lambda item: item["profit"])


def _profit_event_outcome_rows(event: EventMarkets, Q: float, now_utc: datetime) -> List[Dict[str, object]]:
    if len(event.outcomes) != 2:
        return []
    outcome_a, outcome_b = event.outcomes
    keys = [
        ("kalshi", event.kalshi_by_outcome.get(outcome_a), outcome_a),
        ("kalshi", event.kalshi_by_outcome.get(outcome_b), outcome_b),
        ("polymarket", event.polymarket_by_outcome.get(outcome_a), outcome_a),
        ("polymarket", event.polymarket_by_outcome.get(outcome_b), outcome_b),
    ]
    if not _books_fresh_by_venue(keys):
        return []
    if not _books_fresh(
        [
            ("kalshi", event.kalshi_by_outcome.get(outcome_a), outcome_a),
            ("kalshi", event.kalshi_by_outcome.get(outcome_b), outcome_b),
            ("polymarket", event.polymarket_by_outcome.get(outcome_a), outcome_a),
            ("polymarket", event.polymarket_by_outcome.get(outcome_b), outcome_b),
        ],
        now_utc,
    ):
        return []
    k_a = event.kalshi_by_outcome.get(outcome_a)
    k_b = event.kalshi_by_outcome.get(outcome_b)
    p_a = event.polymarket_by_outcome.get(outcome_a)
    p_b = event.polymarket_by_outcome.get(outcome_b)
    if not k_a or not k_b or not p_a or not p_b:
        return []
    k_a_stats = get_fill_stats("kalshi", k_a, outcome_a, "YES", Q)
    k_b_stats = get_fill_stats("kalshi", k_b, outcome_b, "YES", Q)
    p_a_stats = get_fill_stats("polymarket", p_a, outcome_a, "YES", Q)
    p_b_stats = get_fill_stats("polymarket", p_b, outcome_b, "YES", Q)
    if not all(stat["ok"] for stat in (k_a_stats, k_b_stats, p_a_stats, p_b_stats)):
        return []
    eff_k_a = apply_fees("kalshi", k_a_stats["vwap_price"], Q)
    eff_k_b = apply_fees("kalshi", k_b_stats["vwap_price"], Q)
    eff_p_a = apply_fees("polymarket", p_a_stats["vwap_price"], Q)
    eff_p_b = apply_fees("polymarket", p_b_stats["vwap_price"], Q)
    rows = [
        _profit_signal(
            event,
            ArbMode.EVENT_OUTCOME,
            k_a,
            p_b,
            "KALSHI:A + POLY:B",
            [eff_k_a, eff_p_b],
            Q,
            now_utc,
        ),
        _profit_signal(
            event,
            ArbMode.EVENT_OUTCOME,
            k_b,
            p_a,
            "POLY:A + KALSHI:B",
            [eff_p_a, eff_k_b],
            Q,
            now_utc,
        ),
    ]
    return [row for row in rows if row]


def _profit_binary_mirror_rows(event: EventMarkets, Q: float, now_utc: datetime) -> List[Dict[str, object]]:
    if len(event.outcomes) != 2:
        return []
    keys = [
        ("kalshi", event.kalshi_by_outcome.get("YES"), "YES"),
        ("kalshi", event.kalshi_by_outcome.get("NO"), "NO"),
        ("polymarket", event.polymarket_by_outcome.get("YES"), "YES"),
        ("polymarket", event.polymarket_by_outcome.get("NO"), "NO"),
    ]
    if not _books_fresh_by_venue(keys):
        return []
    if not _books_fresh(
        [
            ("kalshi", event.kalshi_by_outcome.get("YES"), "YES"),
            ("kalshi", event.kalshi_by_outcome.get("NO"), "NO"),
            ("polymarket", event.polymarket_by_outcome.get("YES"), "YES"),
            ("polymarket", event.polymarket_by_outcome.get("NO"), "NO"),
        ],
        now_utc,
    ):
        return []
    k_yes = event.kalshi_by_outcome.get("YES")
    k_no = event.kalshi_by_outcome.get("NO")
    p_yes = event.polymarket_by_outcome.get("YES")
    p_no = event.polymarket_by_outcome.get("NO")
    if not k_yes or not k_no or not p_yes or not p_no:
        return []
    k_yes_stats = get_fill_stats("kalshi", k_yes, "YES", "YES", Q)
    k_no_stats = get_fill_stats("kalshi", k_no, "NO", "NO", Q)
    p_yes_stats = get_fill_stats("polymarket", p_yes, "YES", "YES", Q)
    p_no_stats = get_fill_stats("polymarket", p_no, "NO", "NO", Q)
    if not all(stat["ok"] for stat in (k_yes_stats, k_no_stats, p_yes_stats, p_no_stats)):
        return []
    eff_k_yes = apply_fees("kalshi", k_yes_stats["vwap_price"], Q)
    eff_k_no = apply_fees("kalshi", k_no_stats["vwap_price"], Q)
    eff_p_yes = apply_fees("polymarket", p_yes_stats["vwap_price"], Q)
    eff_p_no = apply_fees("polymarket", p_no_stats["vwap_price"], Q)
    rows = [
        _profit_signal(
            event,
            ArbMode.BINARY_MIRROR,
            k_yes,
            p_no,
            "KALSHI:YES + POLY:NO",
            [eff_k_yes, eff_p_no],
            Q,
            now_utc,
        ),
        _profit_signal(
            event,
            ArbMode.BINARY_MIRROR,
            k_no,
            p_yes,
            "POLY:YES + KALSHI:NO",
            [eff_p_yes, eff_k_no],
            Q,
            now_utc,
        ),
    ]
    return [row for row in rows if row]


def _books_fresh(keys: List[Tuple[str, Optional[str], str]], now_utc: datetime) -> bool:
    last_updates: List[datetime] = []
    for venue, market_id, outcome in keys:
        if not market_id:
            return False
        book = ORDERBOOKS.get((venue, market_id, outcome))
        if not book:
            return False
        last_update = book.get("last_update_ts_utc")
        if not isinstance(last_update, datetime):
            return False
        last_update = _ensure_utc(last_update)
        last_updates.append(last_update)
        if MAX_STALENESS_SECONDS > 0:
            age = (now_utc - last_update).total_seconds()
            if age > MAX_STALENESS_SECONDS:
                return False
    if MAX_SYNC_SKEW_SECONDS > 0 and last_updates:
        skew = (max(last_updates) - min(last_updates)).total_seconds()
        if skew > MAX_SYNC_SKEW_SECONDS:
            return False
    return True


def _books_fresh_by_venue(keys: List[Tuple[str, Optional[str], str]]) -> bool:
    now_ts = time.time()
    for venue, market_id, outcome in keys:
        if not market_id:
            return False
        book = ORDERBOOKS.get((venue, market_id, outcome))
        if not book:
            return False
        fetched_at = book.get("fetched_at_ts")
        if not isinstance(fetched_at, (int, float)):
            return False
        age_seconds = now_ts - float(fetched_at)
        if venue == "polymarket":
            if age_seconds > POLY_MAX_AGE_SECONDS:
                return False
        elif venue == "kalshi":
            if age_seconds > KALSHI_MAX_AGE_SECONDS:
                return False
    return True


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _thresholds(arb_type: ArbMode) -> Tuple[float, float]:
    if arb_type == ArbMode.BINARY_MIRROR:
        return BINARY_MIN_ROI, BINARY_MIN_ABS_PROFIT
    return EVENT_MIN_ROI, EVENT_MIN_ABS_PROFIT


def signal_edge_bps(signal: Dict[str, object]) -> float:
    size = float(signal.get("size") or 0.0)
    profit = float(signal.get("profit") or 0.0)
    if size <= 0:
        return 0.0
    return (profit / size) * 10_000


def signal_fingerprint(
    event: EventMarkets,
    arb_type: ArbMode,
    signal: Dict[str, object],
) -> Tuple[Tuple[object, ...], str]:
    event_key = event.event_id or "|".join(event.outcomes) if event.outcomes else "UNKNOWN"
    direction_raw = str(signal.get("direction") or "")
    if arb_type == ArbMode.EVENT_OUTCOME:
        direction = direction_raw.replace(" ", "").upper()
        outcomes = event.outcomes if len(event.outcomes) == 2 else ["UNKNOWN", "UNKNOWN"]
        outcome_by_letter = {"A": outcomes[0], "B": outcomes[1]}
        venue_to_letter: Dict[str, str] = {}
        for part in direction.split("+"):
            if ":" not in part:
                continue
            venue_token, letter = part.split(":", 1)
            if letter not in {"A", "B"}:
                continue
            if venue_token in {"KALSHI", "KAL"}:
                venue_to_letter["kalshi"] = letter
            elif venue_token in {"POLY", "POLYMARKET"}:
                venue_to_letter["polymarket"] = letter
        venue_pair = ("kalshi", "polymarket")
        if venue_to_letter.get("kalshi") and venue_to_letter.get("polymarket"):
            outcome_pair = (
                outcome_by_letter.get(venue_to_letter["kalshi"], outcomes[0]),
                outcome_by_letter.get(venue_to_letter["polymarket"], outcomes[1]),
            )
        else:
            outcome_pair = (outcomes[0], outcomes[1])
        key = (arb_type.value, event_key, venue_pair, outcome_pair, direction)
        return key, event_key
    outcome_id = event.outcome_id or "UNKNOWN"
    key = (arb_type.value, event_key, outcome_id, direction_raw)
    return key, event_key


def should_emit_signal(event: EventMarkets, arb_type: ArbMode, signal: Dict[str, object]) -> bool:
    key, event_key = signal_fingerprint(event, arb_type, signal)
    edge_bps = signal_edge_bps(signal)
    size = float(signal.get("size") or 0.0)
    now_ts = time.time()
    allowed, reason = SIGNAL_SUPPRESSOR.should_emit(
        arb_type=arb_type,
        key=key,
        event_key=event_key,
        edge_bps=edge_bps,
        size=size,
        now_ts=now_ts,
    )
    if not allowed:
        logger.debug("arb suppressed: %s key=%s edge_bps=%.2f", reason, key, edge_bps)
    return allowed


def post_confirm_decision(
    event: EventMarkets,
    arb_type: ArbMode,
    signal: Dict[str, object],
    decision: Dict[str, object],
    *,
    now_ms: Optional[int] = None,
) -> Dict[str, object]:
    if not POST_CONFIRM_ENABLED:
        return {"ok": True, "reason": "disabled"}
    key = _post_confirm_fingerprint(event, arb_type, signal)
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    detected_ts = signal.get("detected_ts")
    if detected_ts is None:
        return {"ok": False, "reason": "missing_detected_ts"}
    try:
        detected_ts_ms = int(detected_ts)
    except (TypeError, ValueError):
        return {"ok": False, "reason": "invalid_detected_ts"}
    legs = signal.get("legs") or []
    keys: List[Tuple[str, Optional[str], str]] = []
    for leg in legs:
        venue = str(leg.get("venue") or "")
        market_id = str(leg.get("market_id") or "")
        outcome_label = str(leg.get("outcome_label") or "")
        if not venue or not market_id or not outcome_label:
            return {"ok": False, "reason": "missing_book"}
        book = ORDERBOOKS.get((venue, market_id, outcome_label))
        if not book:
            return {"ok": False, "reason": "missing_book"}
        fetched_at = book.get("fetched_at_ts")
        if not isinstance(fetched_at, (int, float)):
            return {"ok": False, "reason": "missing_fetched_at"}
        if float(fetched_at) * 1000 <= detected_ts_ms:
            return {"ok": False, "reason": "not_updated_since_detect"}
        keys.append((venue, market_id, outcome_label))
    if not _books_fresh_by_venue(keys):
        return {"ok": False, "reason": "stale_book"}

    size = float(signal.get("size") or 0.0)
    if size <= 0:
        return {"ok": False, "reason": "invalid_size"}
    eff_prices: List[float] = []
    for leg in legs:
        venue = str(leg.get("venue"))
        market_id = str(leg.get("market_id"))
        outcome_label = str(leg.get("outcome_label"))
        side = str(leg.get("side") or "YES")
        stats = get_fill_stats(venue, market_id, outcome_label, side, size)
        if not stats.get("ok"):
            return {"ok": False, "reason": "insufficient_depth"}
        eff_price = apply_fees(venue, stats.get("vwap_price"), size)
        if eff_price is None:
            return {"ok": False, "reason": "missing_price"}
        eff_price *= 1 + (MAX_SLIPPAGE_BPS / 10_000)
        eff_prices.append(eff_price)
    total_price = sum(eff_prices)
    total_cost = size * total_price
    expected_pnl = (size * 1.0) - total_cost
    edge_bps = (expected_pnl / size) * 10_000 if size > 0 else 0.0
    if edge_bps < MIN_EDGE_BPS:
        return {"ok": False, "reason": "edge_below_min", "edge_bps": edge_bps, "expected_pnl_usd": expected_pnl}
    if expected_pnl <= 0:
        return {"ok": False, "reason": "non_positive_pnl", "edge_bps": edge_bps, "expected_pnl_usd": expected_pnl}

    prev = POST_CONFIRM_CACHE.get(key)
    edge_bps = edge_bps
    if not prev:
        POST_CONFIRM_CACHE[key] = {
            "ts": float(now_ms),
            "edge_bps": edge_bps,
        }
        logger.debug("post-confirm cached key=%s edge_bps=%.2f", key, edge_bps)
        return {
            "ok": False,
            "reason": "first_confirm_cached",
            "edge_bps": edge_bps,
            "expected_pnl_usd": expected_pnl,
            "limit_prices": eff_prices,
        }
    age_ms = now_ms - int(prev["ts"])
    if age_ms > POST_CONFIRM_WINDOW_MS:
        POST_CONFIRM_CACHE[key] = {
            "ts": float(now_ms),
            "edge_bps": edge_bps,
        }
        logger.debug("post-confirm reset key=%s age_ms=%d", key, age_ms)
        return {
            "ok": False,
            "reason": "window_expired",
            "edge_bps": edge_bps,
            "prev_edge_bps": float(prev["edge_bps"]),
            "age_ms": age_ms,
            "expected_pnl_usd": expected_pnl,
            "limit_prices": eff_prices,
        }
    edge_decay = float(prev["edge_bps"]) - edge_bps
    if edge_decay > POST_CONFIRM_MAX_EDGE_DECAY_BPS:
        POST_CONFIRM_CACHE.pop(key, None)
        logger.debug("post-confirm reject key=%s edge_decay=%.2f", key, edge_decay)
        return {
            "ok": False,
            "reason": "edge_decay",
            "edge_bps": edge_bps,
            "prev_edge_bps": float(prev["edge_bps"]),
            "edge_decay_bps": edge_decay,
            "age_ms": age_ms,
            "expected_pnl_usd": expected_pnl,
            "limit_prices": eff_prices,
        }
    POST_CONFIRM_CACHE.pop(key, None)
    logger.debug("post-confirm accept key=%s age_ms=%d", key, age_ms)
    return {
        "ok": True,
        "reason": "second_confirm",
        "edge_bps": edge_bps,
        "prev_edge_bps": float(prev["edge_bps"]),
        "edge_decay_bps": edge_decay,
        "age_ms": age_ms,
        "expected_pnl_usd": expected_pnl,
        "limit_prices": eff_prices,
    }


def confirm_on_trigger(opportunity: Dict[str, object]) -> Dict[str, object]:
    detected_ts = opportunity.get("detected_ts")
    if detected_ts is None:
        return _confirm_result(False, "missing_detected_ts", 0.0, 0.0)
    try:
        detected_ts_ms = int(detected_ts)
    except (TypeError, ValueError):
        return _confirm_result(False, "invalid_detected_ts", 0.0, 0.0)
    now_ms = int(time.time() * 1000)
    if now_ms - detected_ts_ms > TRIGGER_CONFIRM_TTL_MS:
        return _confirm_result(False, "ttl_expired", 0.0, 0.0)

    legs = opportunity.get("legs") or []
    keys: List[Tuple[str, Optional[str], str]] = []
    for leg in legs:
        venue = str(leg.get("venue") or "")
        market_id = str(leg.get("market_id") or "")
        outcome_label = str(leg.get("outcome_label") or "")
        if not venue or not market_id or not outcome_label:
            return _confirm_result(False, "missing_book", 0.0, 0.0)
        keys.append((venue, market_id, outcome_label))
        if (venue, market_id, outcome_label) not in ORDERBOOKS:
            return _confirm_result(False, "missing_book", 0.0, 0.0)
    if not _books_fresh_by_venue(keys):
        return _confirm_result(False, "stale_book", 0.0, 0.0)

    size = float(opportunity.get("size") or 0.0)
    if size <= 0:
        return _confirm_result(False, "invalid_size", 0.0, 0.0)

    eff_prices: List[float] = []
    for leg in legs:
        venue = str(leg.get("venue"))
        market_id = str(leg.get("market_id"))
        outcome_label = str(leg.get("outcome_label"))
        side = str(leg.get("side") or "YES")
        stats = get_fill_stats(venue, market_id, outcome_label, side, size)
        if not stats.get("ok"):
            return _confirm_result(False, "insufficient_depth", 0.0, 0.0)
        eff_price = apply_fees(venue, stats.get("vwap_price"), size)
        if eff_price is None:
            return _confirm_result(False, "missing_price", 0.0, 0.0)
        eff_price *= 1 + (MAX_SLIPPAGE_BPS / 10_000)
        eff_prices.append(eff_price)

    total_price = sum(eff_prices)
    total_cost = size * total_price
    expected_pnl = (size * 1.0) - total_cost
    if size <= 0:
        edge_bps = 0.0
    else:
        edge_bps = (expected_pnl / size) * 10_000
    if edge_bps < MIN_EDGE_BPS:
        return _confirm_result(False, "edge_below_min", edge_bps, expected_pnl)
    if expected_pnl <= 0:
        return _confirm_result(False, "non_positive_pnl", edge_bps, expected_pnl)

    bucket = detected_ts_ms // IDEMPOTENCY_BUCKET_MS
    key = _confirm_idempotency_key(legs, eff_prices, bucket)
    last_ts = CONFIRM_IDEMPOTENCY.get(key)
    if last_ts is not None and (now_ms - last_ts) < RECENT_FIRE_TTL_MS:
        return _confirm_result(False, "duplicate", edge_bps, expected_pnl)
    CONFIRM_IDEMPOTENCY[key] = now_ms

    return _confirm_result(True, "", edge_bps, expected_pnl, limit_prices=eff_prices)


def _confirm_idempotency_key(legs: List[Dict[str, object]], eff_prices: List[float], bucket: int) -> str:
    parts = []
    for leg, eff_price in zip(legs, eff_prices):
        venue = str(leg.get("venue") or "")
        market_id = str(leg.get("market_id") or "")
        side = str(leg.get("side") or "")
        rounded = f"{float(eff_price):.4f}"
        parts.append(f"{venue}:{market_id}:{side}:{rounded}")
    raw = f"{bucket}|" + "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _post_confirm_fingerprint(event: EventMarkets, arb_type: ArbMode, signal: Dict[str, object]) -> str:
    key, _ = signal_fingerprint(event, arb_type, signal)
    raw = repr(key)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _confirm_result(
    ok: bool,
    reason: str,
    recalculated_edge_bps: float,
    expected_pnl_usd: float,
    *,
    limit_prices: Optional[List[float]] = None,
) -> Dict[str, object]:
    result = {
        "ok": ok,
        "reason": reason,
        "recalculated_edge_bps": float(recalculated_edge_bps),
        "expected_pnl_usd": float(expected_pnl_usd),
    }
    if limit_prices is not None:
        result["limit_prices"] = limit_prices
    return result
