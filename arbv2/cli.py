import argparse
import asyncio
import csv
import json
import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from arbv2.config import load_config
from arbv2.ingest.kalshi import ingest_kalshi
from arbv2.ingest.polymarket import ingest_polymarket
from arbv2.match.matcher import match_predicates
from arbv2.match.sports_equiv import predicates_equivalent
from arbv2.models import Market, SportsPredicate
from arbv2.teams import canonicalize_team
from arbv2.parse.sports import parse_sports_predicate
from arbv2.pricing.polymarket import (
    build_token_map,
    fetch_book_snapshot,
    stream_books as stream_poly_books,
    validate_clob_auth,
)
from arbv2.pricing.kalshi import stream_books as stream_kalshi_books
from arbv2.pricing.arb import (
    ArbMode,
    CONFIRM_MODE,
    EventMarkets,
    MAX_SLIPPAGE_BPS,
    ORDERBOOKS,
    Q_MIN,
    apply_fees,
    confirm_on_trigger,
    evaluate_paired_metrics,
    evaluate_profit_at_size,
    evaluate_profit_rows,
    find_best_arb,
    get_fill_stats,
    post_confirm_decision,
    signal_edge_bps,
    subscribe_orderbook_updates,
    should_emit_signal,
    unsubscribe_orderbook_updates,
)
from arbv2.storage import (
    delete_pending_signal,
    fetch_pending_signals,
    fetch_markets,
    fetch_match_pairs,
    fetch_predicates,
    fetch_matched_market_ids,
    init_db,
    insert_prices,
    insert_arb_scans,
    log_market_stats,
    prune_orphaned_runtime_rows,
    replace_matches,
    upsert_pending_signal,
    upsert_markets,
    upsert_predicates,
)


logger = logging.getLogger(__name__)
EXECUTION_INTENTS_CSV = "arbv2_execution_intents.csv"
EXECUTION_INTENT_FIELDS = [
    "intent_id",
    "type",
    "event_id",
    "venues",
    "markets",
    "sides",
    "limit_prices",
    "size_usd",
    "edge_bps",
    "expected_pnl_usd",
    "detected_ts",
    "confirmed_at_ts",
    "time_to_confirm_ms",
    "signal_duration_ms",
]
ACTIVE_CONFIRMED_SIGNALS: Dict[str, Dict[str, object]] = {}
PENDING_SIGNALS_HYDRATED_DB: Optional[str] = None


def _purge_db_files(db_path: str) -> None:
    removed: List[str] = []
    for path in [db_path, f"{db_path}-wal", f"{db_path}-shm", f"{db_path}-journal"]:
        if os.path.exists(path):
            os.remove(path)
            removed.append(path)
    if removed:
        logger.info("Purged db files: %s", ", ".join(removed))
    else:
        logger.info("No db files to purge for path: %s", db_path)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(prog="arbv2")
    sub = parser.add_subparsers(dest="cmd", required=True)

    ingest_parser = sub.add_parser("ingest", help="Ingest Kalshi and Polymarket markets")
    ingest_parser.add_argument("--kalshi", action="store_true", help="Ingest Kalshi markets")
    ingest_parser.add_argument("--polymarket", action="store_true", help="Ingest Polymarket markets")
    ingest_parser.add_argument("--limit", type=int, default=None)

    sub.add_parser("match", help="Parse predicates and match markets")
    sub.add_parser("health", help="Show runtime DB/CSV health metrics")

    debug_parser = sub.add_parser("debug", help="Debug a pair")
    debug_parser.add_argument("kalshi_market_id")
    debug_parser.add_argument("polymarket_market_id")

    price_parser = sub.add_parser("price", help="Fetch pricing data")
    price_parser.add_argument("--polymarket", action="store_true", help="Fetch Polymarket prices")
    price_parser.add_argument("--kalshi", action="store_true", help="Fetch Kalshi prices")
    price_parser.add_argument("--snapshot", action="store_true", help="Fetch a one-time snapshot")
    price_parser.add_argument("--stream", action="store_true", help="Stream live prices")
    price_parser.add_argument("--limit", type=int, default=None)
    price_parser.add_argument("--poll-seconds", type=int, default=1, help="Kalshi poll cadence in seconds")

    arb_parser = sub.add_parser("arb", help="Run arbitrage evaluation")
    arb_parser.add_argument(
        "--mode",
        choices=[mode.value for mode in ArbMode],
        default=ArbMode.EVENT_OUTCOME.value,
        help="Arbitrage mode",
    )
    arb_parser.add_argument("--limit", type=int, default=None)

    run_parser = sub.add_parser("run", help="Loop ingest + match on an interval")
    run_parser.add_argument(
        "--interval-seconds",
        type=int,
        default=3600,
        help="Seconds between ingest+match runs",
    )
    live_parser = sub.add_parser(
        "live",
        help="Loop ingest+match and run price streams in the same process",
    )
    live_parser.add_argument("--kalshi", action="store_true", help="Stream Kalshi prices")
    live_parser.add_argument("--polymarket", action="store_true", help="Stream Polymarket prices")
    live_parser.add_argument("--limit", type=int, default=None)
    live_parser.add_argument(
        "--interval-seconds",
        type=int,
        default=3600,
        help="Seconds between ingest+match refreshes",
    )
    live_parser.add_argument("--poll-seconds", type=int, default=1, help="Kalshi poll cadence in seconds")

    args = parser.parse_args()
    config = load_config()
    if args.cmd == "ingest":
        if args.limit:
            config.ingest_limit = args.limit
        return _run_ingest(config, args.kalshi, args.polymarket)
    if args.cmd == "match":
        return _run_match(config)
    if args.cmd == "health":
        return _run_health(config)
    if args.cmd == "debug":
        return _run_debug(config, args.kalshi_market_id, args.polymarket_market_id)
    if args.cmd == "price":
        return _run_price(config, args)
    if args.cmd == "arb":
        return _run_arb(config, args)
    if args.cmd == "run":
        return _run_loop(config, args.interval_seconds)
    if args.cmd == "live":
        return _run_live(config, args)
    return 1


def _run_ingest(config, kalshi_flag: bool, polymarket_flag: bool) -> int:
    init_db(config.db_path)
    if not kalshi_flag and not polymarket_flag:
        kalshi_flag = True
        polymarket_flag = True
    markets: List[Market] = []
    if kalshi_flag:
        kalshi_markets = ingest_kalshi(config)
        logger.info("Kalshi markets: %d", len(kalshi_markets))
        markets.extend(kalshi_markets)
    if polymarket_flag:
        poly_markets = ingest_polymarket(config)
        logger.info("Polymarket markets: %d", len(poly_markets))
        markets.extend(poly_markets)
    upsert_markets(config.db_path, markets)
    log_market_stats(config.db_path)
    return 0


def _run_match(config) -> int:
    init_db(config.db_path)
    kalshi_markets = fetch_markets(config.db_path, venue="kalshi")
    poly_markets = fetch_markets(config.db_path, venue="polymarket")

    kalshi_preds = _parse_all(kalshi_markets)
    poly_preds = _parse_all(poly_markets)

    upsert_predicates(config.db_path, kalshi_preds + poly_preds)

    matches = match_predicates(
        kalshi_markets,
        poly_markets,
        kalshi_preds,
        poly_preds,
        subset_matching=config.subset_matching_enabled,
    )
    replace_matches(config.db_path, matches)
    pruned = prune_orphaned_runtime_rows(config.db_path)
    if pruned["prices_deleted"] or pruned["arb_scans_deleted"]:
        logger.info(
            "Pruned orphaned rows prices=%d arb_scans=%d",
            pruned["prices_deleted"],
            pruned["arb_scans_deleted"],
        )

    logger.info("Predicates parsed: kalshi=%d polymarket=%d", len(kalshi_preds), len(poly_preds))
    logger.info("Matches found: %d", len(matches))
    _log_match_stats(kalshi_markets, poly_markets, kalshi_preds, poly_preds, matches)
    return 0


def _run_health(config) -> int:
    init_db(config.db_path)
    conn = sqlite3.connect(config.db_path)
    try:
        cur = conn.cursor()
        counts = {
            "matches": _fetch_scalar(cur, "SELECT COUNT(*) FROM matches"),
            "kalshi_markets": _fetch_scalar(cur, "SELECT COUNT(*) FROM markets WHERE venue='kalshi'"),
            "poly_markets": _fetch_scalar(cur, "SELECT COUNT(*) FROM markets WHERE venue='polymarket'"),
            "kalshi_prices": _fetch_scalar(cur, "SELECT COUNT(*) FROM prices WHERE venue='kalshi'"),
            "poly_prices": _fetch_scalar(cur, "SELECT COUNT(*) FROM prices WHERE venue='polymarket'"),
            "arb_scans": _fetch_scalar(cur, "SELECT COUNT(*) FROM arb_scans"),
            "pending_signals": _fetch_scalar(cur, "SELECT COUNT(*) FROM pending_signals"),
        }
        latest = {
            "kalshi_prices_latest_ts": _fetch_scalar(cur, "SELECT MAX(ts_utc) FROM prices WHERE venue='kalshi'"),
            "poly_prices_latest_ts": _fetch_scalar(cur, "SELECT MAX(ts_utc) FROM prices WHERE venue='polymarket'"),
            "arb_scans_latest_ts": _fetch_scalar(cur, "SELECT MAX(ts_utc) FROM arb_scans"),
            "pending_signals_latest_ts": _fetch_scalar(cur, "SELECT MAX(updated_ts) FROM pending_signals"),
        }
        orphan_counts = {
            "orphan_prices": _fetch_scalar(
                cur,
                """
                SELECT COUNT(*) FROM prices p
                WHERE
                    (p.venue='kalshi' AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.kalshi_market_id=p.market_id))
                    OR
                    (p.venue='polymarket' AND NOT EXISTS (SELECT 1 FROM matches m WHERE m.polymarket_market_id=p.market_id))
                """,
            ),
            "orphan_arb_scans": _fetch_scalar(
                cur,
                """
                SELECT COUNT(*) FROM arb_scans a
                WHERE NOT EXISTS (
                    SELECT 1 FROM matches m
                    WHERE m.kalshi_market_id=a.kalshi_market_id
                      AND m.polymarket_market_id=a.polymarket_market_id
                )
                """,
            ),
        }
        coverage = {
            "matched_pairs_with_both_prices": _fetch_scalar(
                cur,
                """
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT m.kalshi_market_id, m.polymarket_market_id
                    FROM matches m
                    WHERE EXISTS (SELECT 1 FROM prices pk WHERE pk.venue='kalshi' AND pk.market_id=m.kalshi_market_id)
                      AND EXISTS (SELECT 1 FROM prices pp WHERE pp.venue='polymarket' AND pp.market_id=m.polymarket_market_id)
                )
                """,
            ),
            "scan_pairs_present": _fetch_scalar(
                cur,
                "SELECT COUNT(*) FROM (SELECT DISTINCT kalshi_market_id, polymarket_market_id FROM arb_scans)",
            ),
        }
    finally:
        conn.close()

    durations = _execution_intent_duration_stats(EXECUTION_INTENTS_CSV)
    logger.info("HEALTH counts=%s", counts)
    logger.info("HEALTH latest=%s", latest)
    logger.info("HEALTH orphans=%s", orphan_counts)
    logger.info("HEALTH coverage=%s", coverage)
    logger.info("HEALTH execution_intents=%s", durations)
    return 0


def _fetch_scalar(cur: sqlite3.Cursor, query: str):
    row = cur.execute(query).fetchone()
    if not row:
        return None
    return row[0]


def _execution_intent_duration_stats(csv_path: str) -> Dict[str, object]:
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return {
            "rows_total": 0,
            "durations_nonblank": 0,
            "durations_blank": 0,
        }
    with open(csv_path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    durations: List[float] = []
    for row in rows:
        value = (row.get("signal_duration_ms") or "").strip()
        if not value:
            continue
        try:
            durations.append(float(value))
        except ValueError:
            continue
    out: Dict[str, object] = {
        "rows_total": len(rows),
        "durations_nonblank": len(durations),
        "durations_blank": len(rows) - len(durations),
    }
    if not durations:
        return out
    sorted_values = sorted(durations)

    def percentile(q: float) -> float:
        idx = int((len(sorted_values) - 1) * q)
        return sorted_values[idx]

    out.update(
        {
            "duration_ms_min": min(sorted_values),
            "duration_ms_p50": percentile(0.5),
            "duration_ms_p90": percentile(0.9),
            "duration_ms_p99": percentile(0.99),
            "duration_ms_max": max(sorted_values),
            "duration_ge_100ms": sum(1 for x in sorted_values if x >= 100),
            "duration_ge_500ms": sum(1 for x in sorted_values if x >= 500),
            "duration_ge_1000ms": sum(1 for x in sorted_values if x >= 1000),
            "duration_ge_5000ms": sum(1 for x in sorted_values if x >= 5000),
        }
    )
    return out


def _run_loop(config, interval_seconds: int) -> int:
    if interval_seconds <= 0:
        logger.warning("Interval must be positive; got %d", interval_seconds)
        return 0
    logger.info("Looping ingest+match every %d seconds", interval_seconds)
    try:
        while True:
            _run_ingest(config, True, True)
            _run_match(config)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Run loop stopped")
    return 0


def _run_debug(config, kalshi_id: str, poly_id: str) -> int:
    init_db(config.db_path)
    kalshi = _find_market(config, "kalshi", kalshi_id)
    poly = _find_market(config, "polymarket", poly_id)
    if not kalshi or not poly:
        logger.error("Market not found in DB")
        return 1
    kpred = parse_sports_predicate(kalshi)
    ppred = parse_sports_predicate(poly)
    print("Kalshi predicate:", kpred)
    print("Polymarket predicate:", ppred)
    if not kpred or not ppred:
        print("Unable to parse predicate")
        return 1
    result = predicates_equivalent(kpred, ppred, config.max_event_time_delta_hours)
    print("Match result:", result.equivalent, result.reason)
    return 0


def _run_price(config, args) -> int:
    init_db(config.db_path)
    if not args.polymarket and not args.kalshi:
        args.polymarket = True
        args.kalshi = True
    if not args.snapshot and not args.stream:
        args.snapshot = True
    poly_token_map, kalshi_markets = _build_price_targets(
        config,
        enable_polymarket=args.polymarket,
        enable_kalshi=args.kalshi,
        limit=args.limit,
    )

    if args.stream:
        try:
            asyncio.run(_run_price_streams(config, poly_token_map, kalshi_markets, args.poll_seconds))
        except KeyboardInterrupt:
            logger.info("Price stream stopped")
    elif args.kalshi and kalshi_markets and args.snapshot:
        logger.warning("Kalshi snapshot via polling is disabled; use --stream instead.")
    return 0


def _build_price_targets(
    config,
    *,
    enable_polymarket: bool,
    enable_kalshi: bool,
    limit: Optional[int],
):
    poly_token_map = None
    kalshi_markets = None
    live_poly_ids: Optional[set] = None

    if enable_polymarket:
        validate_clob_auth(config)
        matched_ids = fetch_matched_market_ids(config.db_path, venue="polymarket")
        markets = fetch_markets(config.db_path, venue="polymarket")
        markets = [m for m in markets if m.market_id in matched_ids]
        if markets:
            now_utc = datetime.now(timezone.utc)
            before = len(markets)
            markets = [m for m in markets if _polymarket_market_is_live(m, now_utc)]
            if before and len(markets) != before:
                logger.info("Polymarket live filter kept %d/%d markets", len(markets), before)
            live_poly_ids = {m.market_id for m in markets}
        if markets:
            poly_token_map = build_token_map(markets)
            if limit:
                poly_token_map = dict(list(poly_token_map.items())[: limit])
        else:
            logger.warning("No matched Polymarket markets found; run match first.")

    if enable_kalshi:
        matched_ids = fetch_matched_market_ids(config.db_path, venue="kalshi")
        markets = fetch_markets(config.db_path, venue="kalshi")
        markets = [m for m in markets if m.market_id in matched_ids]
        if markets and live_poly_ids:
            pairs = fetch_match_pairs(config.db_path)
            live_kalshi_ids = {k for k, p in pairs if p in live_poly_ids}
            before = len(markets)
            markets = [m for m in markets if m.market_id in live_kalshi_ids]
            if before and len(markets) != before:
                logger.info("Kalshi live filter kept %d/%d markets", len(markets), before)
        if markets:
            if limit:
                markets = markets[: limit]
            kalshi_markets = markets
        else:
            logger.warning("No matched Kalshi markets found; run match first.")

    return poly_token_map, kalshi_markets


def _parse_iso_dt(value: object) -> Optional[datetime]:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _polymarket_market_is_live(market: Market, now_utc: datetime) -> bool:
    raw = market.raw_json or {}
    if raw.get("archived") is True or raw.get("closed") is True:
        return False
    if raw.get("active") is False:
        return False
    if raw.get("acceptingOrders") is False:
        return False
    start_val = raw.get("gameStartTime") or raw.get("startTime") or raw.get("startDate")
    end_val = raw.get("endDate") or raw.get("endDateIso")
    start_dt = _parse_iso_dt(start_val)
    end_dt = _parse_iso_dt(end_val)
    pre_window = timedelta(hours=6)
    post_window = timedelta(hours=2)
    if start_dt and now_utc < start_dt - pre_window:
        return False
    if end_dt and now_utc > end_dt + post_window:
        return False
    return True


def _run_arb(config, args) -> int:
    init_db(config.db_path)
    kalshi_markets = fetch_markets(config.db_path, venue="kalshi")
    poly_markets = fetch_markets(config.db_path, venue="polymarket")
    kalshi_preds = _parse_all(kalshi_markets)
    poly_preds = _parse_all(poly_markets)

    mode = ArbMode(args.mode)
    signals: List[dict] = []

    if mode == ArbMode.EVENT_OUTCOME:
        events = _build_event_markets(kalshi_markets, poly_markets, kalshi_preds, poly_preds)
        if args.limit:
            events = events[: args.limit]
        for event in events:
            signal = find_best_arb(event, mode)
            if signal and should_emit_signal(event, mode, signal):
                signals.append(signal)
    else:
        events = _build_binary_events(config.db_path)
        if args.limit:
            events = events[: args.limit]
        for event in events:
            signal = find_best_arb(event, mode)
            if signal and should_emit_signal(event, mode, signal):
                signals.append(signal)

    for signal in signals[:10]:
        logger.info(
            "ARB %s size=%s profit=%.4f roi=%.5f direction=%s",
            signal["arb_type"],
            signal["size"],
            signal["profit"],
            signal["roi"],
            signal["direction"],
        )
    logger.info("Arb signals found: %d", len(signals))
    return 0


async def _run_price_streams(
    config,
    poly_token_map,
    kalshi_markets,
    poll_seconds: int,
) -> None:
    tasks = []
    if poly_token_map:
        tasks.append(asyncio.create_task(stream_poly_books(config, poly_token_map, config.db_path)))
    if kalshi_markets:
        tasks.append(asyncio.create_task(stream_kalshi_books(config, kalshi_markets, config.db_path)))
    tasks.append(asyncio.create_task(_run_arb_stream(config)))
    tasks.append(asyncio.create_task(_run_intent_backfill_loop(config.db_path, EXECUTION_INTENTS_CSV)))
    tasks.append(asyncio.create_task(_loop_heartbeat("price_streams")))
    if not tasks:
        return
    await asyncio.gather(*tasks)


def _parse_all(markets: List[Market]) -> List[SportsPredicate]:
    predicates: List[SportsPredicate] = []
    for market in markets:
        pred = parse_sports_predicate(market)
        if pred:
            predicates.append(pred)
    return predicates


def _find_market(config, venue: str, market_id: str) -> Optional[Market]:
    markets = fetch_markets(config.db_path, venue=venue)
    for market in markets:
        if market.market_id == market_id:
            return market
    return None


async def _run_arb_stream(config) -> None:
    kalshi_markets = fetch_markets(config.db_path, venue="kalshi")
    poly_markets = fetch_markets(config.db_path, venue="polymarket")
    matched_pairs = set(fetch_match_pairs(config.db_path))
    title_by_market_id = _title_by_market_id(kalshi_markets + poly_markets)
    kalshi_preds = _parse_all(kalshi_markets)
    poly_preds = _parse_all(poly_markets)
    events_event = _build_event_markets(kalshi_markets, poly_markets, kalshi_preds, poly_preds)
    events_binary = _build_binary_events(config.db_path)
    events_event_by_leg = _index_events_by_leg(events_event)
    events_binary_by_leg = _index_events_by_leg(events_binary)

    # Prime arb_scans with current state once at stream startup.
    await asyncio.to_thread(
        _scan_arb_cycle,
        config.db_path,
        events_event,
        events_binary,
        title_by_market_id,
        matched_pairs,
    )

    loop = asyncio.get_running_loop()
    update_queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue(maxsize=8192)
    pending_legs: Set[Tuple[str, str]] = set()
    full_rescan_requested = False

    def _enqueue_leg_update(venue: str, market_id: str) -> None:
        nonlocal full_rescan_requested
        leg_key = (venue, market_id)
        if leg_key in pending_legs:
            return
        if update_queue.full():
            full_rescan_requested = True
            return
        pending_legs.add(leg_key)
        update_queue.put_nowait(leg_key)

    def _on_orderbook_update(venue: str, market_id: str, outcome_label: str) -> None:
        _ = outcome_label
        loop.call_soon_threadsafe(_enqueue_leg_update, venue, market_id)

    subscriber_token = subscribe_orderbook_updates(_on_orderbook_update)
    try:
        while True:
            first_leg = await update_queue.get()
            changed_legs: Set[Tuple[str, str]] = {first_leg}
            pending_legs.discard(first_leg)
            while True:
                try:
                    leg = update_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                pending_legs.discard(leg)
                changed_legs.add(leg)

            if full_rescan_requested:
                full_rescan_requested = False
                scan_event = events_event
                scan_binary = events_binary
            else:
                scan_event = _events_for_legs(events_event_by_leg, changed_legs)
                scan_binary = _events_for_legs(events_binary_by_leg, changed_legs)

            if not scan_event and not scan_binary:
                continue

            await asyncio.to_thread(
                _scan_arb_cycle,
                config.db_path,
                scan_event,
                scan_binary,
                title_by_market_id,
                matched_pairs,
            )
    finally:
        unsubscribe_orderbook_updates(subscriber_token)


def _scan_arb_cycle(
    db_path: str,
    events_event: List[EventMarkets],
    events_binary: List[EventMarkets],
    title_by_market_id: Dict[str, str],
    matched_pairs: Optional[Set[Tuple[str, str]]] = None,
) -> None:
    _scan_arb_events(db_path, events_event, ArbMode.EVENT_OUTCOME, title_by_market_id, matched_pairs)
    _scan_arb_events(db_path, events_binary, ArbMode.BINARY_MIRROR, title_by_market_id, matched_pairs)


def _index_events_by_leg(events: List[EventMarkets]) -> Dict[Tuple[str, str], List[EventMarkets]]:
    index: Dict[Tuple[str, str], List[EventMarkets]] = {}
    for event in events:
        seen: Set[Tuple[str, str]] = set()
        for market_id in event.kalshi_by_outcome.values():
            if market_id:
                seen.add(("kalshi", market_id))
        for market_id in event.polymarket_by_outcome.values():
            if market_id:
                seen.add(("polymarket", market_id))
        for leg_key in seen:
            index.setdefault(leg_key, []).append(event)
    return index


def _events_for_legs(
    index: Dict[Tuple[str, str], List[EventMarkets]],
    legs: Set[Tuple[str, str]],
) -> List[EventMarkets]:
    selected: List[EventMarkets] = []
    seen_ids: Set[int] = set()
    for leg_key in legs:
        for event in index.get(leg_key, []):
            marker = id(event)
            if marker in seen_ids:
                continue
            seen_ids.add(marker)
            selected.append(event)
    return selected


async def _loop_heartbeat(name: str, *, interval: float = 5.0, warn_threshold: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    next_ts = loop.time() + interval
    while True:
        await asyncio.sleep(interval)
        now = loop.time()
        drift = now - next_ts
        if drift > warn_threshold:
            logger.warning("Event loop lag %s drift=%.3fs", name, drift)
        next_ts = now + interval


async def _run_intent_backfill_loop(db_path: str, csv_path: str, *, interval_seconds: float = 1.0) -> None:
    while True:
        await asyncio.to_thread(_backfill_execution_intent_durations, db_path, csv_path)
        await asyncio.sleep(interval_seconds)


def _run_live(config, args) -> int:
    _purge_db_files(config.db_path)
    init_db(config.db_path)
    if not args.polymarket and not args.kalshi:
        args.polymarket = True
        args.kalshi = True
    if args.interval_seconds <= 0:
        logger.warning("Interval must be positive; got %d", args.interval_seconds)
        return 0
    try:
        asyncio.run(
            _run_live_loop(
                config,
                enable_polymarket=args.polymarket,
                enable_kalshi=args.kalshi,
                interval_seconds=args.interval_seconds,
                poll_seconds=args.poll_seconds,
                limit=args.limit,
            )
        )
    except KeyboardInterrupt:
        logger.info("Live loop stopped")
    return 0


async def _run_live_loop(
    config,
    *,
    enable_polymarket: bool,
    enable_kalshi: bool,
    interval_seconds: int,
    poll_seconds: int,
    limit: Optional[int],
) -> None:
    cycle = 0
    while True:
        cycle += 1
        logger.info("Live cycle %d: ingest+match", cycle)
        _run_ingest(config, True, True)
        _run_match(config)
        poly_token_map, kalshi_markets = _build_price_targets(
            config,
            enable_polymarket=enable_polymarket,
            enable_kalshi=enable_kalshi,
            limit=limit,
        )
        tasks = []
        if enable_polymarket and poly_token_map:
            tasks.append(asyncio.create_task(stream_poly_books(config, poly_token_map, config.db_path)))
        if enable_kalshi and kalshi_markets:
            tasks.append(asyncio.create_task(stream_kalshi_books(config, kalshi_markets, config.db_path)))
        tasks.append(asyncio.create_task(_run_arb_stream(config)))
        tasks.append(asyncio.create_task(_run_intent_backfill_loop(config.db_path, EXECUTION_INTENTS_CSV)))
        if not tasks:
            logger.warning("No price streams to run; sleeping %d seconds", interval_seconds)
            await asyncio.sleep(interval_seconds)
            continue
        try:
            await asyncio.sleep(interval_seconds)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def _scan_arb_events(
    db_path: str,
    events: List[EventMarkets],
    mode: ArbMode,
    title_by_market_id: Dict[str, str],
    matched_pairs: Optional[Set[Tuple[str, str]]] = None,
) -> None:
    _backfill_execution_intent_durations(db_path, EXECUTION_INTENTS_CSV)
    scan_rows: List[tuple] = []
    latest_by_key: Dict[tuple, tuple] = {}
    for event in events:
        metrics = evaluate_paired_metrics(event, mode)
        if metrics:
            legs = metrics.get("legs") or []
            kalshi_market_id = None
            polymarket_market_id = None
            for leg in legs:
                if leg.get("venue") == "kalshi" and not kalshi_market_id:
                    kalshi_market_id = leg.get("market_id")
                if leg.get("venue") == "polymarket" and not polymarket_market_id:
                    polymarket_market_id = leg.get("market_id")
            if kalshi_market_id and polymarket_market_id:
                if matched_pairs is not None and (kalshi_market_id, polymarket_market_id) not in matched_pairs:
                    continue
                event_title = title_by_market_id.get(kalshi_market_id) or title_by_market_id.get(polymarket_market_id)
                key = (metrics["arb_type"], kalshi_market_id, polymarket_market_id)
                latest_by_key[key] = (
                    metrics["arb_type"],
                    kalshi_market_id,
                    polymarket_market_id,
                    float(metrics["profit"]),
                    metrics["ts_utc"],
                    metrics.get("event_id"),
                    metrics.get("size"),
                    event_title,
                )
        signal = find_best_arb(event, mode)
        # Continue to use signal below for emission.
        if not signal:
            continue
        if not should_emit_signal(event, mode, signal):
            continue
        detected_ts = int(time.time() * 1000)
        signal["detected_ts"] = detected_ts
        logger.info(
            "ARB detected ts=%d event_id=%s arb_type=%s direction=%s",
            detected_ts,
            signal.get("event_id"),
            signal.get("arb_type"),
            signal.get("direction"),
        )
        if CONFIRM_MODE == "on_trigger":
            decision = confirm_on_trigger(signal)
            logger.info(
                "ARB confirm %s reason=%s edge_bps=%.2f expected_pnl=%.4f",
                "PASS" if decision["ok"] else "FAIL",
                decision.get("reason") or "",
                decision.get("recalculated_edge_bps") or 0.0,
                decision.get("expected_pnl_usd") or 0.0,
            )
            if not decision["ok"]:
                _append_confirm_rejection_csv(
                    signal,
                    decision,
                    None,
                    "confirm_on_trigger",
                    "arbv2_confirm_rejections.csv",
                )
                continue
            post_decision = post_confirm_decision(event, mode, signal, decision)
            if not post_decision["ok"]:
                _append_confirm_rejection_csv(
                    signal,
                    decision,
                    post_decision,
                    "post_confirm",
                    "arbv2_confirm_rejections.csv",
                )
                continue
            intent = _build_execution_intent(signal, post_decision)
            logger.info("EXECUTION_INTENT %s", json.dumps(intent, separators=(",", ":")))
            _append_execution_intent_csv(intent, EXECUTION_INTENTS_CSV)
            _track_confirmed_signal(db_path, signal, intent, EXECUTION_INTENTS_CSV)
            _log_signal_details(signal)
            continue
        _append_arb_csv(signal, "arbv2_arbs.csv")
        logger.info(
            "ARB signal %s size=%s profit=%.2f roi=%.4f direction=%s",
            signal["arb_type"],
            signal["size"],
            signal["profit"],
            signal["roi"],
            signal["direction"],
        )
        _log_signal_details(signal)
    scan_rows = list(latest_by_key.values())
    if scan_rows:
        insert_arb_scans(db_path, scan_rows)
    return None


def _log_signal_details(signal: Dict[str, object]) -> None:
    for leg in signal.get("legs") or []:
        venue = leg["venue"]
        market_id = leg["market_id"]
        outcome_label = leg["outcome_label"]
        book = _format_book_snapshot(venue, market_id, outcome_label)
        logger.info(
            "  leg %s %s %s vwap=%.4f eff=%.4f worst=%.4f book=%s",
            venue,
            market_id,
            outcome_label,
            leg["raw_vwap"] or 0.0,
            leg["eff_vwap"] or 0.0,
            leg["worst_price"] or 0.0,
            book,
        )


def _build_execution_intent(signal: Dict[str, object], decision: Dict[str, object]) -> Dict[str, object]:
    legs = signal.get("legs") or []
    confirmed_at_ts = int(time.time() * 1000)
    intent_id = uuid.uuid4().hex
    detected_ts = _as_int(signal.get("detected_ts"))
    time_to_confirm_ms = None
    if detected_ts is not None:
        time_to_confirm_ms = confirmed_at_ts - detected_ts
    return {
        "intent_id": intent_id,
        "type": "EXECUTION_INTENT",
        "event_id": signal.get("event_id"),
        "venues": [leg.get("venue") for leg in legs],
        "markets": [leg.get("market_id") for leg in legs],
        "sides": [leg.get("side") for leg in legs],
        "limit_prices": decision.get("limit_prices") or [leg.get("eff_vwap") for leg in legs],
        "size_usd": signal.get("size"),
        "edge_bps": decision.get("recalculated_edge_bps") or decision.get("edge_bps"),
        "expected_pnl_usd": decision.get("expected_pnl_usd"),
        "detected_ts": detected_ts,
        "confirmed_at_ts": confirmed_at_ts,
        "time_to_confirm_ms": time_to_confirm_ms,
        "signal_duration_ms": None,
    }


def _append_execution_intent_csv(intent: Dict[str, object], csv_path: str) -> None:
    rows = _read_execution_intent_rows(csv_path)
    rows.append(_execution_intent_row(intent))
    _write_execution_intent_rows(csv_path, rows)


def _track_confirmed_signal(db_path: str, signal: Dict[str, object], intent: Dict[str, object], csv_path: str) -> None:
    size_usd = _as_float(signal.get("size"))
    detected_ts = _as_int(intent.get("detected_ts"))
    confirmed_at_ts = _as_int(intent.get("confirmed_at_ts"))
    if size_usd is None or size_usd <= 0 or detected_ts is None or confirmed_at_ts is None:
        return
    legs = signal.get("legs") or []
    normalized_legs: List[Dict[str, object]] = []
    for leg in legs:
        venue = str(leg.get("venue") or "")
        market_id = str(leg.get("market_id") or "")
        outcome_label = str(leg.get("outcome_label") or "")
        side = str(leg.get("side") or "YES")
        if not venue or not market_id or not outcome_label:
            continue
        normalized_legs.append(
            {
                "venue": venue,
                "market_id": market_id,
                "outcome_label": outcome_label,
                "side": side,
            }
        )
    if not normalized_legs:
        return
    key = _execution_intent_key(intent)
    ACTIVE_CONFIRMED_SIGNALS[key] = {
        "intent": dict(intent),
        "legs": normalized_legs,
        "size_usd": size_usd,
        "detected_ts": detected_ts,
        "confirmed_at_ts": confirmed_at_ts,
        "csv_path": csv_path,
    }
    upsert_pending_signal(db_path, key, ACTIVE_CONFIRMED_SIGNALS[key])


def _hydrate_pending_signals(db_path: str) -> None:
    global PENDING_SIGNALS_HYDRATED_DB
    if PENDING_SIGNALS_HYDRATED_DB == db_path:
        return
    if PENDING_SIGNALS_HYDRATED_DB != db_path:
        ACTIVE_CONFIRMED_SIGNALS.clear()
    for key, payload in fetch_pending_signals(db_path):
        if key and isinstance(payload, dict):
            ACTIVE_CONFIRMED_SIGNALS[key] = payload
    PENDING_SIGNALS_HYDRATED_DB = db_path


def _backfill_execution_intent_durations(db_path: str, csv_path: str) -> None:
    _hydrate_pending_signals(db_path)
    if not ACTIVE_CONFIRMED_SIGNALS:
        return
    now_ms = int(time.time() * 1000)
    completed_keys: List[str] = []
    for key, tracked in list(ACTIVE_CONFIRMED_SIGNALS.items()):
        legs = tracked.get("legs") or []
        size_usd = _as_float(tracked.get("size_usd"))
        detected_ts = _as_int(tracked.get("detected_ts"))
        if not legs or size_usd is None or size_usd <= 0 or detected_ts is None:
            continue
        pnl = _reprice_signal_pnl(legs, size_usd)
        tracked["last_eval_ts"] = now_ms
        tracked["last_eval_pnl_usd"] = pnl
        upsert_pending_signal(db_path, key, tracked, now_ms=now_ms)
        if pnl is None or pnl > 0:
            continue
        intent = dict(tracked.get("intent") or {})
        intent["signal_duration_ms"] = now_ms - detected_ts
        _replace_execution_intent_csv_row(csv_path, intent)
        logger.info(
            "EXECUTION_INTENT_COMPLETE event_id=%s confirmed_at_ts=%s signal_duration_ms=%s",
            intent.get("event_id"),
            intent.get("confirmed_at_ts"),
            intent.get("signal_duration_ms"),
        )
        completed_keys.append(key)
    for key in completed_keys:
        ACTIVE_CONFIRMED_SIGNALS.pop(key, None)
        delete_pending_signal(db_path, key)


def _reprice_signal_pnl(legs: List[Dict[str, object]], size_usd: float) -> Optional[float]:
    eff_prices: List[float] = []
    for leg in legs:
        venue = str(leg.get("venue") or "")
        market_id = str(leg.get("market_id") or "")
        outcome_label = str(leg.get("outcome_label") or "")
        side = str(leg.get("side") or "YES")
        if not venue or not market_id or not outcome_label:
            return None
        stats = get_fill_stats(venue, market_id, outcome_label, side, size_usd)
        if not stats.get("ok"):
            return None
        eff_price = apply_fees(venue, stats.get("vwap_price"), size_usd)
        if eff_price is None:
            return None
        eff_price *= 1 + (MAX_SLIPPAGE_BPS / 10_000)
        eff_prices.append(eff_price)
    total_cost = size_usd * sum(eff_prices)
    return size_usd - total_cost


def _replace_execution_intent_csv_row(csv_path: str, intent: Dict[str, object]) -> None:
    target = _execution_intent_row(intent)
    rows = _read_execution_intent_rows(csv_path)
    replaced = False
    target_intent_id = str(target.get("intent_id") or "")
    for idx, row in enumerate(rows):
        row_intent_id = str(row.get("intent_id") or "")
        if target_intent_id and row_intent_id and row_intent_id == target_intent_id:
            rows[idx] = target
            replaced = True
            break
        if (
            str(row.get("type") or "") == str(target.get("type") or "")
            and str(row.get("event_id") or "") == str(target.get("event_id") or "")
            and str(row.get("confirmed_at_ts") or "") == str(target.get("confirmed_at_ts") or "")
            and str(row.get("markets") or "") == str(target.get("markets") or "")
            and str(row.get("sides") or "") == str(target.get("sides") or "")
            and str(row.get("signal_duration_ms") or "") == ""
        ):
            rows[idx] = target
            replaced = True
            break
    if not replaced:
        rows.append(target)
    _write_execution_intent_rows(csv_path, rows)


def _read_execution_intent_rows(csv_path: str) -> List[Dict[str, object]]:
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return []
    rows: List[Dict[str, object]] = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            normalized = {field: row.get(field, "") for field in EXECUTION_INTENT_FIELDS}
            rows.append(normalized)
    return rows


def _write_execution_intent_rows(csv_path: str, rows: List[Dict[str, object]]) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EXECUTION_INTENT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in EXECUTION_INTENT_FIELDS})


def _execution_intent_row(intent: Dict[str, object]) -> Dict[str, object]:
    return {
        "intent_id": intent.get("intent_id"),
        "type": intent.get("type"),
        "event_id": intent.get("event_id"),
        "venues": json.dumps(intent.get("venues") or [], separators=(",", ":")),
        "markets": json.dumps(intent.get("markets") or [], separators=(",", ":")),
        "sides": json.dumps(intent.get("sides") or [], separators=(",", ":")),
        "limit_prices": json.dumps(intent.get("limit_prices") or [], separators=(",", ":")),
        "size_usd": intent.get("size_usd"),
        "edge_bps": intent.get("edge_bps"),
        "expected_pnl_usd": intent.get("expected_pnl_usd"),
        "detected_ts": intent.get("detected_ts"),
        "confirmed_at_ts": intent.get("confirmed_at_ts"),
        "time_to_confirm_ms": intent.get("time_to_confirm_ms"),
        "signal_duration_ms": intent.get("signal_duration_ms"),
    }


def _execution_intent_key(intent: Dict[str, object]) -> str:
    intent_id = str(intent.get("intent_id") or "")
    if intent_id:
        return intent_id
    event_id = str(intent.get("event_id") or "")
    confirmed_at_ts = str(intent.get("confirmed_at_ts") or "")
    markets = json.dumps(intent.get("markets") or [], separators=(",", ":"))
    sides = json.dumps(intent.get("sides") or [], separators=(",", ":"))
    return "|".join([event_id, confirmed_at_ts, markets, sides])


def _as_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _append_confirm_rejection_csv(
    signal: Dict[str, object],
    decision: Dict[str, object],
    post_decision: Optional[Dict[str, object]],
    stage: str,
    csv_path: str,
) -> None:
    legs = signal.get("legs") or []
    raw_vwap_map = _legs_to_map(legs, "raw_vwap")
    eff_vwap_map = _legs_to_map(legs, "eff_vwap")
    limit_prices = decision.get("limit_prices") or []
    limit_price_map = _legs_to_map(legs, "eff_vwap", override_values=limit_prices)
    detected_edge_bps = signal_edge_bps(signal)
    confirmed_edge_bps = None
    if post_decision:
        confirmed_edge_bps = post_decision.get("edge_bps")
    else:
        confirmed_edge_bps = decision.get("recalculated_edge_bps")
    detected_ts = signal.get("detected_ts")
    confirm_latency_ms = None
    try:
        if detected_ts is not None:
            confirm_latency_ms = int(time.time() * 1000) - int(detected_ts)
    except (TypeError, ValueError):
        confirm_latency_ms = None
    rejection_reason = None
    if post_decision:
        rejection_reason = post_decision.get("reason")
    else:
        rejection_reason = decision.get("reason")
    sync_skew_seconds = post_decision.get("sync_skew_seconds") if post_decision else None
    kalshi_book_ts_utc = post_decision.get("kalshi_book_ts_utc") if post_decision else None
    polymarket_book_ts_utc = post_decision.get("polymarket_book_ts_utc") if post_decision else None
    reason = decision.get("reason")
    if stage == "post_confirm" and post_decision:
        reason = post_decision.get("reason")
    row = {
        "event_id": signal.get("event_id"),
        "arb_type": signal.get("arb_type"),
        "direction": signal.get("direction"),
        "size_usd": signal.get("size"),
        "detected_ts": detected_ts,
        "stage": stage,
        "reason": reason,
        "expected_pnl_usd": decision.get("expected_pnl_usd"),
        "detected_edge_bps": detected_edge_bps,
        "confirmed_edge_bps": confirmed_edge_bps,
        "confirm_latency_ms": confirm_latency_ms,
        "sync_skew_seconds": sync_skew_seconds,
        "kalshi_book_ts_utc": kalshi_book_ts_utc,
        "polymarket_book_ts_utc": polymarket_book_ts_utc,
        "edge_decay_bps": post_decision.get("edge_decay_bps") if post_decision else None,
        "venues": json.dumps([leg.get("venue") for leg in legs], separators=(",", ":")),
        "markets": json.dumps([leg.get("market_id") for leg in legs], separators=(",", ":")),
    }
    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    with open(csv_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _legs_to_map(legs: List[Dict[str, object]], key: str, *, override_values: Optional[List[object]] = None) -> Dict[str, object]:
    output: Dict[str, object] = {}
    for idx, leg in enumerate(legs):
        venue = str(leg.get("venue") or "")
        if not venue:
            continue
        value = leg.get(key)
        if override_values is not None and idx < len(override_values):
            value = override_values[idx]
        if venue not in output:
            output[venue] = value
        else:
            existing = output[venue]
            if isinstance(existing, list):
                existing.append(value)
            else:
                output[venue] = [existing, value]
    return output


def _format_book_snapshot(venue: str, market_id: str, outcome_label: str) -> str:
    book = ORDERBOOKS.get((venue, market_id, outcome_label))
    if not book:
        return "missing"
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    best_bids = ",".join(f"{price:.3f}@{qty:.0f}" for price, qty in list(bids)[:3])
    best_asks = ",".join(f"{price:.3f}@{qty:.0f}" for price, qty in list(asks)[:3])
    return f"bids[{best_bids}] asks[{best_asks}]"


def _append_arb_csv(signal: Dict[str, object], csv_path: str) -> None:
    row = {
        "arb_type": signal.get("arb_type"),
        "event_id": signal.get("event_id"),
        "market_id": signal.get("market_id"),
        "outcome_label": signal.get("outcome_label"),
        "direction": signal.get("direction"),
        "size": signal.get("size"),
        "profit": signal.get("profit"),
        "roi": signal.get("roi"),
        "ts_utc": signal.get("ts_utc"),
        "legs_json": json.dumps(signal.get("legs", []), separators=(",", ":")),
    }
    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    with open(csv_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _log_match_stats(
    kalshi_markets: List[Market],
    poly_markets: List[Market],
    kalshi_preds: List[SportsPredicate],
    poly_preds: List[SportsPredicate],
    matches: List,
) -> None:
    kalshi_keys = _event_keys(kalshi_markets, kalshi_preds)
    poly_keys = _event_keys(poly_markets, poly_preds)
    matched_keys = kalshi_keys.intersection(poly_keys)
    logger.info(
        "EventKey counts: kalshi=%d polymarket=%d matched=%d",
        len(kalshi_keys),
        len(poly_keys),
        len(matched_keys),
    )
    logger.info(
        "Outcome label counts: kalshi=%s polymarket=%s",
        _count_outcomes(kalshi_markets),
        _count_outcomes(poly_markets),
    )
    pred_by_id = {pred.market_id: pred for pred in kalshi_preds + poly_preds}
    for sample in matches[:3]:
        k = _find_market_by_id(kalshi_markets, sample.kalshi_market_id)
        p = _find_market_by_id(poly_markets, sample.polymarket_market_id)
        if not k or not p:
            continue
        kpred = pred_by_id.get(k.market_id)
        team_a = canonicalize_team(kpred.team_a) if kpred else None
        team_b = canonicalize_team(kpred.team_b) if kpred else None
        logger.info(
            "Matched pair: %s | %s vs %s | %s | %s | %s",
            k.event_date,
            team_a,
            team_b,
            k.outcome_label,
            k.market_id,
            p.market_id,
        )


def _event_keys(markets: List[Market], preds: List[SportsPredicate]) -> set:
    pred_by_id = {pred.market_id: pred for pred in preds}
    keys = set()
    for market in markets:
        pred = pred_by_id.get(market.market_id)
        if not pred or not market.event_date:
            continue
        team_a = canonicalize_team(pred.team_a)
        team_b = canonicalize_team(pred.team_b)
        if not team_a or not team_b:
            continue
        left, right = sorted([team_a, team_b])
        keys.add((left, right, market.event_date, _gender_key(market)))
    return keys


def _count_outcomes(markets: List[Market]) -> dict:
    counts: dict = {}
    for market in markets:
        label = market.outcome_label or "UNKNOWN"
        counts[label] = counts.get(label, 0) + 1
    return counts


def _title_by_market_id(markets: List[Market]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for market in markets:
        title = market.event_title or market.title or ""
        if title and market.market_id not in mapping:
            mapping[market.market_id] = str(title)
    return mapping


def _build_event_markets(
    kalshi_markets: List[Market],
    poly_markets: List[Market],
    kalshi_preds: List[SportsPredicate],
    poly_preds: List[SportsPredicate],
) -> List[EventMarkets]:
    kalshi_by_event = _group_by_event_key(kalshi_markets, kalshi_preds)
    poly_by_event = _group_by_event_key(poly_markets, poly_preds)
    matched_keys = set(kalshi_by_event.keys()).intersection(poly_by_event.keys())
    events: List[EventMarkets] = []
    for key in matched_keys:
        k_group = kalshi_by_event[key]
        p_group = poly_by_event[key]
        if any(m.outcome_label == "DRAW" for m in k_group) or any(m.outcome_label == "DRAW" for m in p_group):
            continue
        k_outcomes = {m.outcome_label for m in k_group if m.outcome_label and m.outcome_label != "DRAW"}
        p_outcomes = {m.outcome_label for m in p_group if m.outcome_label and m.outcome_label != "DRAW"}
        outcomes = sorted(k_outcomes.intersection(p_outcomes))
        if len(outcomes) != 2:
            continue
        k_map = {m.outcome_label: m.market_id for m in k_group if m.outcome_label in outcomes}
        p_map = {m.outcome_label: m.market_id for m in p_group if m.outcome_label in outcomes}
        if set(k_map.keys()) != set(outcomes) or set(p_map.keys()) != set(outcomes):
            continue
        event_key = _event_key_str(key)
        events.append(
            EventMarkets(
                kalshi_by_outcome=k_map,
                polymarket_by_outcome=p_map,
                outcomes=outcomes,
                event_id=event_key,
            )
        )
    return events


def _build_binary_events(db_path: str) -> List[EventMarkets]:
    pairs = fetch_match_pairs(db_path)
    markets = fetch_markets(db_path)
    preds = fetch_predicates(db_path)
    key_map = _market_event_key_map(markets, preds)
    outcome_map = {m.market_id: m.outcome_label for m in markets}
    events: List[EventMarkets] = []
    for kalshi_id, poly_id in pairs:
        event_key = key_map.get(kalshi_id) or key_map.get(poly_id)
        outcome_id = outcome_map.get(kalshi_id) or outcome_map.get(poly_id)
        events.append(
            EventMarkets(
                kalshi_by_outcome={"YES": kalshi_id, "NO": kalshi_id},
                polymarket_by_outcome={"YES": poly_id, "NO": poly_id},
                outcomes=["YES", "NO"],
                event_id=event_key,
                outcome_id=outcome_id,
            )
        )
    return events


def _event_key_tuple(market: Market, pred: SportsPredicate) -> Optional[tuple]:
    if not market.event_date:
        return None
    team_a = canonicalize_team(pred.team_a)
    team_b = canonicalize_team(pred.team_b)
    if not team_a or not team_b:
        return None
    left, right = sorted([team_a, team_b])
    return (left, right, market.event_date, _gender_key(market))


def _event_key_str(key: tuple) -> str:
    return "|".join(str(part) for part in key)


def _group_by_event_key(markets: List[Market], preds: List[SportsPredicate]) -> Dict[tuple, List[Market]]:
    pred_by_id = {pred.market_id: pred for pred in preds}
    grouped: Dict[tuple, List[Market]] = {}
    for market in markets:
        pred = pred_by_id.get(market.market_id)
        if not pred:
            continue
        key = _event_key_tuple(market, pred)
        if not key:
            continue
        grouped.setdefault(key, []).append(market)
    return grouped


def _market_event_key_map(markets: List[Market], preds: List[SportsPredicate]) -> Dict[str, str]:
    pred_by_id = {pred.market_id: pred for pred in preds}
    mapping: Dict[str, str] = {}
    for market in markets:
        pred = pred_by_id.get(market.market_id)
        if not pred:
            continue
        key = _event_key_tuple(market, pred)
        if not key:
            continue
        mapping[market.market_id] = _event_key_str(key)
    return mapping


def _gender_key(market: Market) -> str:
    if market.venue == "kalshi":
        if str(market.series_ticker or "") == "KXNCAAWBGAME":
            return "W"
        return "M"
    if market.venue == "polymarket":
        title = str(market.title or "").strip()
        if title.endswith("(W)"):
            return "W"
        return "M"
    return "M"


def _find_market_by_id(markets: List[Market], market_id: str) -> Optional[Market]:
    for market in markets:
        if market.market_id == market_id:
            return market
    return None
