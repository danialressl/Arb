import argparse
import asyncio
import csv
import json
import logging
import os
import time
from typing import Dict, List, Optional

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
from arbv2.pricing.kalshi import poll_markets
from arbv2.pricing.arb import (
    ArbMode,
    CONFIRM_MODE,
    EventMarkets,
    ORDERBOOKS,
    Q_MIN,
    confirm_on_trigger,
    post_confirm_decision,
    evaluate_profit_at_size,
    evaluate_profit_rows,
    find_best_arb,
    should_emit_signal,
)
from arbv2.storage import (
    fetch_markets,
    fetch_match_pairs,
    fetch_predicates,
    fetch_matched_market_ids,
    init_db,
    insert_prices,
    insert_arb_scans,
    log_market_stats,
    replace_matches,
    upsert_markets,
    upsert_predicates,
)


logger = logging.getLogger(__name__)


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

    logger.info("Predicates parsed: kalshi=%d polymarket=%d", len(kalshi_preds), len(poly_preds))
    logger.info("Matches found: %d", len(matches))
    _log_match_stats(kalshi_markets, poly_markets, kalshi_preds, poly_preds, matches)
    return 0


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
        try:
            asyncio.run(
                poll_markets(
                    config,
                    kalshi_markets,
                    config.db_path,
                    poll_seconds=args.poll_seconds,
                    run_once=True,
                )
            )
        except KeyboardInterrupt:
            logger.info("Kalshi price stream stopped")
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

    if enable_polymarket:
        validate_clob_auth(config)
        matched_ids = fetch_matched_market_ids(config.db_path, venue="polymarket")
        markets = fetch_markets(config.db_path, venue="polymarket")
        markets = [m for m in markets if m.market_id in matched_ids]
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
        if markets:
            if limit:
                markets = markets[: limit]
            kalshi_markets = markets
        else:
            logger.warning("No matched Kalshi markets found; run match first.")

    return poly_token_map, kalshi_markets


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
        tasks.append(
            asyncio.create_task(
                poll_markets(
                    config,
                    kalshi_markets,
                    config.db_path,
                    poll_seconds=poll_seconds,
                    run_once=False,
                )
            )
        )
    tasks.append(asyncio.create_task(_run_arb_stream(config)))
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
    kalshi_preds = _parse_all(kalshi_markets)
    poly_preds = _parse_all(poly_markets)
    events_event = _build_event_markets(kalshi_markets, poly_markets, kalshi_preds, poly_preds)
    events_binary = _build_binary_events(config.db_path)
    while True:
        _scan_arb_events(config.db_path, events_event, ArbMode.EVENT_OUTCOME)
        _scan_arb_events(config.db_path, events_binary, ArbMode.BINARY_MIRROR)
        await asyncio.sleep(2)


def _run_live(config, args) -> int:
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
        ORDERBOOKS.clear()
        tasks = []
        if enable_polymarket and poly_token_map:
            tasks.append(asyncio.create_task(stream_poly_books(config, poly_token_map, config.db_path)))
        if enable_kalshi and kalshi_markets:
            tasks.append(
                asyncio.create_task(
                    poll_markets(
                        config,
                        kalshi_markets,
                        config.db_path,
                        poll_seconds=poll_seconds,
                        run_once=False,
                    )
                )
            )
        tasks.append(asyncio.create_task(_run_arb_stream(config)))
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


def _scan_arb_events(db_path: str, events: List[EventMarkets], mode: ArbMode) -> None:
    scan_rows: List[tuple] = []
    best_by_key: Dict[tuple, tuple] = {}
    for event in events:
        profit_rows = evaluate_profit_rows(event, mode, Q_MIN)
        for row in profit_rows:
            key = (row["arb_type"], row["kalshi_market_id"], row["polymarket_market_id"])
            profit = float(row["profit"])
            prev = best_by_key.get(key)
            if prev is None or profit > prev[3]:
                best_by_key[key] = (
                    row["arb_type"],
                    row["kalshi_market_id"],
                    row["polymarket_market_id"],
                    profit,
                    row["ts_utc"],
                )
        signal = find_best_arb(event, mode)
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
            intent = _build_execution_intent(signal, decision)
            logger.info("EXECUTION_INTENT %s", json.dumps(intent, separators=(",", ":")))
            _append_execution_intent_csv(intent, "arbv2_execution_intents.csv")
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
    scan_rows = list(best_by_key.values())
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
    return {
        "type": "EXECUTION_INTENT",
        "event_id": signal.get("event_id"),
        "venues": [leg.get("venue") for leg in legs],
        "markets": [leg.get("market_id") for leg in legs],
        "sides": [leg.get("side") for leg in legs],
        "limit_prices": decision.get("limit_prices") or [leg.get("eff_vwap") for leg in legs],
        "size_usd": signal.get("size"),
        "edge_bps": decision.get("recalculated_edge_bps"),
        "expected_pnl_usd": decision.get("expected_pnl_usd"),
        "confirmed_at_ts": int(time.time() * 1000),
    }


def _append_execution_intent_csv(intent: Dict[str, object], csv_path: str) -> None:
    row = {
        "type": intent.get("type"),
        "event_id": intent.get("event_id"),
        "venues": json.dumps(intent.get("venues") or [], separators=(",", ":")),
        "markets": json.dumps(intent.get("markets") or [], separators=(",", ":")),
        "sides": json.dumps(intent.get("sides") or [], separators=(",", ":")),
        "limit_prices": json.dumps(intent.get("limit_prices") or [], separators=(",", ":")),
        "size_usd": intent.get("size_usd"),
        "edge_bps": intent.get("edge_bps"),
        "expected_pnl_usd": intent.get("expected_pnl_usd"),
        "confirmed_at_ts": intent.get("confirmed_at_ts"),
    }
    write_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    with open(csv_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


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
    row = {
        "event_id": signal.get("event_id"),
        "arb_type": signal.get("arb_type"),
        "direction": signal.get("direction"),
        "size_usd": signal.get("size"),
        "detected_ts": signal.get("detected_ts"),
        "stage": stage,
        "reason": decision.get("reason"),
        "edge_bps": decision.get("recalculated_edge_bps"),
        "expected_pnl_usd": decision.get("expected_pnl_usd"),
        "raw_vwaps_by_venue": json.dumps(raw_vwap_map, separators=(",", ":")),
        "eff_vwaps_by_venue": json.dumps(eff_vwap_map, separators=(",", ":")),
        "confirm_limit_prices_by_venue": json.dumps(limit_price_map, separators=(",", ":")),
        "first_confirm_edge_bps": post_decision.get("prev_edge_bps") if post_decision else None,
        "second_confirm_edge_bps": post_decision.get("edge_bps") if post_decision else None,
        "confirm_age_ms": post_decision.get("age_ms") if post_decision else None,
        "edge_decay_bps": post_decision.get("edge_decay_bps") if post_decision else None,
        "post_confirm_reason": post_decision.get("reason") if post_decision else None,
        "venues": json.dumps([leg.get("venue") for leg in legs], separators=(",", ":")),
        "markets": json.dumps([leg.get("market_id") for leg in legs], separators=(",", ":")),
        "sides": json.dumps([leg.get("side") for leg in legs], separators=(",", ":")),
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
