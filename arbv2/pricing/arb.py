import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple


ORDERBOOKS: Dict[Tuple[str, str, str], Dict[str, object]] = {}

KALSHI_FEE_BPS = float(os.getenv("ARBV2_KALSHI_FEE_BPS", "100"))
POLY_FEE_BPS = float(os.getenv("ARBV2_POLY_FEE_BPS", "0"))
SLIPPAGE_BPS = float(os.getenv("ARBV2_ARB_SLIPPAGE_BPS", "10"))
MAX_STALENESS_SECONDS = float(os.getenv("ARBV2_ARB_MAX_STALENESS_SECONDS", "3"))
MAX_SYNC_SKEW_SECONDS = float(os.getenv("ARBV2_ARB_MAX_SYNC_SKEW_SECONDS", "2"))
EVENT_MIN_ROI = float(os.getenv("ARBV2_ARB_EVENT_MIN_ROI", "0.008"))
EVENT_MIN_ABS_PROFIT = float(os.getenv("ARBV2_ARB_EVENT_MIN_ABS_PROFIT", "1.0"))
BINARY_MIN_ROI = float(os.getenv("ARBV2_ARB_BINARY_MIN_ROI", "0.008"))
BINARY_MIN_ABS_PROFIT = float(os.getenv("ARBV2_ARB_BINARY_MIN_ABS_PROFIT", "1.0"))
Q_MIN = float(os.getenv("ARBV2_ARB_Q_MIN", "100"))
Q_MAX = float(os.getenv("ARBV2_ARB_Q_MAX", "5000"))


class ArbMode(str, Enum):
    EVENT_OUTCOME = "EVENT_OUTCOME"
    BINARY_MIRROR = "BINARY_MIRROR"


@dataclass
class EventMarkets:
    kalshi_by_outcome: Dict[str, str]
    polymarket_by_outcome: Dict[str, str]
    outcomes: List[str]
    event_id: Optional[str] = None


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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _thresholds(arb_type: ArbMode) -> Tuple[float, float]:
    if arb_type == ArbMode.BINARY_MIRROR:
        return BINARY_MIN_ROI, BINARY_MIN_ABS_PROFIT
    return EVENT_MIN_ROI, EVENT_MIN_ABS_PROFIT
