from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from arbv2.pricing import arb as arb_module


@dataclass
class PersistenceRun:
    fingerprint: str
    arb_type: str
    direction: str
    event_key: str
    legs: List[Dict[str, object]]
    detected_ts_ms: int
    confirmed_at_ts: Optional[int]
    first_seen_ts_utc: datetime
    last_seen_ts_utc: datetime
    ticks_alive: int
    entry_edge_bps: float
    max_edge_bps: float
    min_edge_bps: float
    last_edge_bps: float
    last_expected_pnl_usd: float = 0.0
    last_size: float = 0.0
    saw_execution_intent: bool = False
    missed_updates: int = 0


ACTIVE_RUNS: Dict[str, PersistenceRun] = {}
RUNS_BY_EVENT: Dict[str, set[str]] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _max_missed_ticks() -> int:
    try:
        return max(1, int(os.getenv("ARBV2_PERSISTENCE_MISS_TICKS", "6")))
    except ValueError:
        return 6


def _leg_tuple(leg: Dict[str, object]) -> Tuple[str, str, str, str]:
    return (
        str(leg.get("venue") or ""),
        str(leg.get("market_id") or ""),
        str(leg.get("outcome_label") or ""),
        str(leg.get("side") or ""),
    )


def _fingerprint(signal: Dict[str, object]) -> Tuple[str, List[Dict[str, object]]]:
    arb_type = str(signal.get("arb_type") or "")
    direction = str(signal.get("direction") or "")
    event_key = str(signal.get("event_id") or "")
    legs = signal.get("legs") or []
    leg_tuples = sorted(_leg_tuple(leg) for leg in legs)
    payload = {
        "arb_type": arb_type,
        "direction": direction,
        "event_key": event_key,
        "legs": leg_tuples,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    leg_dicts = [
        {
            "venue": venue,
            "market_id": market_id,
            "outcome_label": outcome_label,
            "side": side,
        }
        for venue, market_id, outcome_label, side in leg_tuples
    ]
    return digest, leg_dicts


def observe_event(
    event: arb_module.EventMarkets,
    arb_mode: arb_module.ArbMode,
    *,
    now_utc: Optional[datetime] = None,
    scan_interval_ms: int = 500,
) -> List[Dict[str, object]]:
    if now_utc is None:
        now_utc = _now_utc()
    event_key = str(event.event_id or "")
    run_ids = list(RUNS_BY_EVENT.get(event_key, set()))
    if not run_ids:
        return []
    results: List[Dict[str, object]] = []
    for run_id in run_ids:
        run = ACTIVE_RUNS.get(run_id)
        if run is None:
            continue
        if run.arb_type != arb_mode.value:
            continue
        update = _compute_edge_for_run(run, now_utc)
        if update is None:
            run.missed_updates += 1
            if run.missed_updates >= _max_missed_ticks():
                _remove_run(run)
            continue
        edge_bps, expected_pnl = update
        run.missed_updates = 0
        _update_run(run, now_utc, edge_bps, expected_pnl, run.last_size or 0.0)
        if edge_bps > 0:
            continue
        duration_ms = int(now_utc.timestamp() * 1000) - run.detected_ts_ms
        _remove_run(run)
        if run.saw_execution_intent and run.confirmed_at_ts is not None:
            results.append(
                {
                    "duration_ms": duration_ms,
                    "event_id": run.event_key,
                    "confirmed_at_ts": run.confirmed_at_ts,
                }
            )
    return results


def drop_run(signal: Optional[Dict[str, object]]) -> None:
    if signal is None:
        return None
    fingerprint, _ = _fingerprint(signal)
    run = ACTIVE_RUNS.get(fingerprint)
    if run:
        _remove_run(run)
    return None


def mark_execution_intent(signal: Optional[Dict[str, object]], confirmed_at_ts: Optional[int] = None) -> None:
    if signal is None:
        return None
    fingerprint, legs = _fingerprint(signal)
    run = ACTIVE_RUNS.get(fingerprint)
    detected_ts = signal.get("detected_ts")
    try:
        detected_ts_ms = int(detected_ts)
    except (TypeError, ValueError):
        detected_ts_ms = int(time.time() * 1000)
    if run is None:
        start_utc = datetime.fromtimestamp(detected_ts_ms / 1000, tz=timezone.utc)
        edge_bps = float(arb_module.signal_edge_bps(signal))
        expected_pnl = float(signal.get("profit") or 0.0)
        size = float(signal.get("size") or 0.0)
        run = PersistenceRun(
            fingerprint=fingerprint,
            arb_type=str(signal.get("arb_type") or ""),
            direction=str(signal.get("direction") or ""),
            event_key=str(signal.get("event_id") or ""),
            legs=legs,
            detected_ts_ms=detected_ts_ms,
            confirmed_at_ts=confirmed_at_ts,
            first_seen_ts_utc=start_utc,
            last_seen_ts_utc=start_utc,
            ticks_alive=1,
            entry_edge_bps=edge_bps,
            max_edge_bps=edge_bps,
            min_edge_bps=edge_bps,
            last_edge_bps=edge_bps,
            last_expected_pnl_usd=expected_pnl,
            last_size=size,
            saw_execution_intent=True,
            missed_updates=0,
        )
        ACTIVE_RUNS[fingerprint] = run
        RUNS_BY_EVENT.setdefault(run.event_key, set()).add(fingerprint)
        return None
    run.saw_execution_intent = True
    if confirmed_at_ts is not None:
        run.confirmed_at_ts = confirmed_at_ts
    return None


def _update_run(
    run: PersistenceRun,
    now_utc: datetime,
    edge_bps: float,
    expected_pnl: float,
    size: float,
) -> None:
    run.last_seen_ts_utc = now_utc
    run.ticks_alive += 1
    run.max_edge_bps = max(run.max_edge_bps, edge_bps)
    run.min_edge_bps = min(run.min_edge_bps, edge_bps)
    run.last_edge_bps = edge_bps
    run.last_expected_pnl_usd = expected_pnl
    run.last_size = size


def _remove_run(run: PersistenceRun) -> None:
    ACTIVE_RUNS.pop(run.fingerprint, None)
    event_runs = RUNS_BY_EVENT.get(run.event_key)
    if event_runs:
        event_runs.discard(run.fingerprint)
        if not event_runs:
            RUNS_BY_EVENT.pop(run.event_key, None)


def _compute_edge_for_run(run: PersistenceRun, now_utc: datetime) -> Optional[Tuple[float, float]]:
    size = float(run.last_size or 0.0)
    if size <= 0:
        return None
    keys = [(leg.get("venue"), leg.get("market_id"), leg.get("outcome_label")) for leg in run.legs]
    if not arb_module._books_fresh_by_venue(keys):
        return None
    if not arb_module._books_fresh(keys, now_utc):
        return None
    eff_prices: List[float] = []
    for leg in run.legs:
        venue = str(leg.get("venue") or "")
        market_id = str(leg.get("market_id") or "")
        outcome_label = str(leg.get("outcome_label") or "")
        side = str(leg.get("side") or "").upper() or "YES"
        stats = arb_module.get_fill_stats(venue, market_id, outcome_label, side, size)
        if not stats.get("ok"):
            return None
        vwap_price = stats.get("vwap_price")
        eff = arb_module.apply_fees(venue, vwap_price, size)
        if eff is None:
            return None
        eff_prices.append(float(eff))
    total_price = sum(eff_prices)
    profit = size * (1.0 - total_price)
    edge_bps = (profit / size) * 10_000 if size > 0 else 0.0
    return edge_bps, profit
