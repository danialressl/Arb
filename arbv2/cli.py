import argparse
import logging
from typing import List, Optional

from arbv2.config import load_config
from arbv2.ingest.kalshi import ingest_kalshi
from arbv2.ingest.polymarket import ingest_polymarket
from arbv2.match.matcher import match_predicates
from arbv2.match.sports_equiv import predicates_equivalent
from arbv2.models import Market, SportsPredicate
from arbv2.teams import canonicalize_team
from arbv2.parse.sports import parse_sports_predicate
from arbv2.storage import (
    fetch_markets,
    init_db,
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

    matches = match_predicates(kalshi_markets, poly_markets, kalshi_preds, poly_preds)
    replace_matches(config.db_path, matches)

    logger.info("Predicates parsed: kalshi=%d polymarket=%d", len(kalshi_preds), len(poly_preds))
    logger.info("Matches found: %d", len(matches))
    _log_match_stats(kalshi_markets, poly_markets, kalshi_preds, poly_preds, matches)
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
        keys.add((left, right, market.event_date))
    return keys


def _count_outcomes(markets: List[Market]) -> dict:
    counts: dict = {}
    for market in markets:
        label = market.outcome_label or "UNKNOWN"
        counts[label] = counts.get(label, 0) + 1
    return counts


def _find_market_by_id(markets: List[Market], market_id: str) -> Optional[Market]:
    for market in markets:
        if market.market_id == market_id:
            return market
    return None
