from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from arbv2.pricing import arb as arb_module


@dataclass
class PersistenceRun:
    fingerprint: str
    arb_type: str
    direction: str
    event_key: str
    legs_json: str
    first_seen_ts_utc: datetime
    last_seen_ts_utc: datetime
    ticks_alive: int
    entry_edge_bps: float
    max_edge_bps: float
    min_edge_bps: float
    last_edge_bps: float
    edge_series: List[Tuple[str, float]] = field(default_factory=list)
    last_expected_pnl_usd: float = 0.0
    last_size: float = 0.0


ACTIVE_RUNS: Dict[str, PersistenceRun] = {}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _min_edge_bps() -> float:
    return float(getattr(arb_module, "MIN_EDGE_BPS", 0))


def _leg_tuple(leg: Dict[str, object]) -> Tuple[str, str, str, str]:
    return (
        str(leg.get("venue") or ""),
        str(leg.get("market_id") or ""),
        str(leg.get("outcome_label") or ""),
        str(leg.get("side") or ""),
    )


def _fingerprint(signal: Dict[str, object]) -> Tuple[str, str]:
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
    return digest, json.dumps(leg_tuples, separators=(",", ":"))


def _append_persistence_csv(row: Dict[str, object], csv_path: str) -> None:
    fieldnames = [
        "ts_utc_emitted",
        "fingerprint",
        "arb_type",
        "direction",
        "event_key",
        "legs_json",
        "first_seen_ts_utc",
        "last_seen_ts_utc",
        "duration_ms",
        "ticks_alive",
        "scan_interval_ms",
        "entry_edge_bps",
        "max_edge_bps",
        "min_edge_bps",
        "last_edge_bps",
        "last_expected_pnl_usd",
        "last_size",
        "edge_series_json",
    ]
    write_header = False
    try:
        with open(csv_path, "r", encoding="utf-8") as handle:
            existing = handle.read(1)
            if not existing:
                write_header = True
    except FileNotFoundError:
        write_header = True
    with open(csv_path, "a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def observe(
    signal: Optional[Dict[str, object]],
    *,
    now_utc: Optional[datetime] = None,
    scan_interval_ms: int = 500,
    csv_path: str = "arbv2_persistence.csv",
) -> None:
    if signal is None:
        return
    if now_utc is None:
        now_utc = _now_utc()
    edge_bps = float(arb_module.signal_edge_bps(signal))
    min_edge = _min_edge_bps()
    fingerprint, legs_json = _fingerprint(signal)
    run = ACTIVE_RUNS.get(fingerprint)
    ts_iso = now_utc.isoformat()
    expected_pnl = float(signal.get("profit") or 0.0)
    size = float(signal.get("size") or 0.0)

    if edge_bps >= min_edge:
        if run is None:
            run = PersistenceRun(
                fingerprint=fingerprint,
                arb_type=str(signal.get("arb_type") or ""),
                direction=str(signal.get("direction") or ""),
                event_key=str(signal.get("event_id") or ""),
                legs_json=legs_json,
                first_seen_ts_utc=now_utc,
                last_seen_ts_utc=now_utc,
                ticks_alive=1,
                entry_edge_bps=edge_bps,
                max_edge_bps=edge_bps,
                min_edge_bps=edge_bps,
                last_edge_bps=edge_bps,
                edge_series=[(ts_iso, edge_bps)],
                last_expected_pnl_usd=expected_pnl,
                last_size=size,
            )
            ACTIVE_RUNS[fingerprint] = run
            return
        _update_run(run, now_utc, edge_bps, expected_pnl, size, ts_iso)
        return

    if edge_bps > 0:
        if run is None:
            return
        _update_run(run, now_utc, edge_bps, expected_pnl, size, ts_iso)
        return

    if run is None:
        return
    _update_run(run, now_utc, edge_bps, expected_pnl, size, ts_iso)
    _finalize_run(run, scan_interval_ms, csv_path)
    ACTIVE_RUNS.pop(fingerprint, None)


def end_run(
    signal: Optional[Dict[str, object]],
    *,
    now_utc: Optional[datetime] = None,
    scan_interval_ms: int = 500,
    csv_path: str = "arbv2_persistence.csv",
) -> None:
    if signal is None:
        return
    if now_utc is None:
        now_utc = _now_utc()
    fingerprint, _ = _fingerprint(signal)
    run = ACTIVE_RUNS.get(fingerprint)
    if run is None:
        return
    edge_bps = float(arb_module.signal_edge_bps(signal))
    expected_pnl = float(signal.get("profit") or 0.0)
    size = float(signal.get("size") or 0.0)
    ts_iso = now_utc.isoformat()
    _update_run(run, now_utc, edge_bps, expected_pnl, size, ts_iso)
    _finalize_run(run, scan_interval_ms, csv_path)
    ACTIVE_RUNS.pop(fingerprint, None)


def _update_run(
    run: PersistenceRun,
    now_utc: datetime,
    edge_bps: float,
    expected_pnl: float,
    size: float,
    ts_iso: str,
) -> None:
    run.last_seen_ts_utc = now_utc
    run.ticks_alive += 1
    run.max_edge_bps = max(run.max_edge_bps, edge_bps)
    run.min_edge_bps = min(run.min_edge_bps, edge_bps)
    run.last_edge_bps = edge_bps
    run.edge_series.append((ts_iso, edge_bps))
    run.last_expected_pnl_usd = expected_pnl
    run.last_size = size


def _finalize_run(run: PersistenceRun, scan_interval_ms: int, csv_path: str) -> None:
    duration_ms = int((run.last_seen_ts_utc - run.first_seen_ts_utc).total_seconds() * 1000)
    row = {
        "ts_utc_emitted": _now_utc().isoformat(),
        "fingerprint": run.fingerprint,
        "arb_type": run.arb_type,
        "direction": run.direction,
        "event_key": run.event_key,
        "legs_json": run.legs_json,
        "first_seen_ts_utc": run.first_seen_ts_utc.isoformat(),
        "last_seen_ts_utc": run.last_seen_ts_utc.isoformat(),
        "duration_ms": duration_ms,
        "ticks_alive": run.ticks_alive,
        "scan_interval_ms": scan_interval_ms,
        "entry_edge_bps": run.entry_edge_bps,
        "max_edge_bps": run.max_edge_bps,
        "min_edge_bps": run.min_edge_bps,
        "last_edge_bps": run.last_edge_bps,
        "last_expected_pnl_usd": run.last_expected_pnl_usd,
        "last_size": run.last_size,
        "edge_series_json": json.dumps(run.edge_series, separators=(",", ":")),
    }
    _append_persistence_csv(row, csv_path)
