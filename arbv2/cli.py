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
from arbv2.pricing.arb import ArbMode, EventMarkets, ORDERBOOKS, Q_MIN, evaluate_profit_at_size, evaluate_profit_rows, find_best_arb
from arbv2.storage import (
    fetch_markets,
    fetch_match_pairs,
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
    poly_token_map = None
    kalshi_markets = None

    if args.polymarket:
        validate_clob_auth(config)
        matched_ids = fetch_matched_market_ids(config.db_path, venue="polymarket")
        markets = fetch_markets(config.db_path, venue="polymarket")
        markets = [m for m in markets if m.market_id in matched_ids]
        if not markets:
            logger.warning("No matched Polymarket markets found; run match first.")
            return 0
        poly_token_map = build_token_map(markets)
        if args.limit:
            poly_token_map = dict(list(poly_token_map.items())[: args.limit])
        if args.snapshot:
            snapshots = []
            total = len(poly_token_map)
            checked = 0
            for token_id, targets in poly_token_map.items():
                checked += 1
                snaps = fetch_book_snapshot(config, token_id, targets)
                if snaps:
                    snapshots.extend(snaps)
                if checked % 50 == 0 or checked == total:
                    logger.info(
                        "Polymarket snapshot progress: checked=%d/%d saved=%d",
                        checked,
                        total,
                        len(snapshots),
                    )
            if snapshots:
                insert_prices(config.db_path, snapshots)
                logger.info("Polymarket price snapshots: %d", len(snapshots))

    if args.kalshi:
        matched_ids = fetch_matched_market_ids(config.db_path, venue="kalshi")
        markets = fetch_markets(config.db_path, venue="kalshi")
        markets = [m for m in markets if m.market_id in matched_ids]
        if not markets:
            logger.warning("No matched Kalshi markets found; run match first.")
            return 0
        if args.limit:
            markets = markets[: args.limit]
        kalshi_markets = markets

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
            if signal:
                signals.append(signal)
    else:
        events = _build_binary_events(config.db_path)
        if args.limit:
            events = events[: args.limit]
        for event in events:
            signal = find_best_arb(event, mode)
            if signal:
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
        _append_arb_csv(signal, "arbv2_arbs.csv")
        logger.info(
            "ARB signal %s size=%s profit=%.2f roi=%.4f direction=%s",
            signal["arb_type"],
            signal["size"],
            signal["profit"],
            signal["roi"],
            signal["direction"],
        )
        for leg in signal["legs"]:
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
    scan_rows = list(best_by_key.values())
    if scan_rows:
        insert_arb_scans(db_path, scan_rows)
    return None


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
        events.append(
            EventMarkets(
                kalshi_by_outcome=k_map,
                polymarket_by_outcome=p_map,
                outcomes=outcomes,
                event_id=None,
            )
        )
    return events


def _build_binary_events(db_path: str) -> List[EventMarkets]:
    pairs = fetch_match_pairs(db_path)
    events: List[EventMarkets] = []
    for kalshi_id, poly_id in pairs:
        events.append(
            EventMarkets(
                kalshi_by_outcome={"YES": kalshi_id, "NO": kalshi_id},
                polymarket_by_outcome={"YES": poly_id, "NO": poly_id},
                outcomes=["YES", "NO"],
                event_id=None,
            )
        )
    return events


def _group_by_event_key(
    markets: List[Market], preds: List[SportsPredicate]
) -> Dict[tuple, List[Market]]:
    pred_by_id = {pred.market_id: pred for pred in preds}
    grouped: Dict[tuple, List[Market]] = {}
    for market in markets:
        pred = pred_by_id.get(market.market_id)
        if not pred or not market.event_date:
            continue
        team_a = canonicalize_team(pred.team_a)
        team_b = canonicalize_team(pred.team_b)
        if not team_a or not team_b:
            continue
        left, right = sorted([team_a, team_b])
        key = (left, right, market.event_date, _gender_key(market))
        grouped.setdefault(key, []).append(market)
    return grouped


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
