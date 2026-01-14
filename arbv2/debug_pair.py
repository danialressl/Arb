import argparse
import sqlite3
from pprint import pprint

from arbv2.config import load_config
from arbv2.match.matcher import _event_key_equivalent, _tokens_equivalent
from arbv2.match.matcher import _event_key_from_predicate, _normalize_outcome_label
from arbv2.models import Market, SportsPredicate


def _fetch_market(cur: sqlite3.Cursor, market_id: str) -> Market | None:
    row = cur.execute(
        """
        SELECT venue, market_id, event_id, series_ticker, title, event_title,
               event_date, market_type, status, outcome_label, raw_json
        FROM markets
        WHERE market_id = ?
        """,
        (market_id,),
    ).fetchone()
    if not row:
        return None
    return Market(**dict(row))


def _fetch_predicate(cur: sqlite3.Cursor, market_id: str) -> SportsPredicate | None:
    row = cur.execute(
        """
        SELECT venue, market_id, event_id, team_a, team_b, winner
        FROM predicates
        WHERE market_id = ?
        """,
        (market_id,),
    ).fetchone()
    if not row:
        return None
    return SportsPredicate(**dict(row))


def main() -> int:
    parser = argparse.ArgumentParser(description="Debug match decisions for two market IDs")
    parser.add_argument("kalshi_market_id")
    parser.add_argument("polymarket_market_id")
    args = parser.parse_args()

    config = load_config()
    conn = sqlite3.connect(config.db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    k_market = _fetch_market(cur, args.kalshi_market_id)
    p_market = _fetch_market(cur, args.polymarket_market_id)
    if not k_market or not p_market:
        print("Market not found in DB")
        return 1

    k_pred = _fetch_predicate(cur, args.kalshi_market_id)
    p_pred = _fetch_predicate(cur, args.polymarket_market_id)
    if not k_pred or not p_pred:
        print("Predicate not found in DB")
        return 1

    k_key = _event_key_from_predicate(k_pred, k_market)
    p_key = _event_key_from_predicate(p_pred, p_market)

    print("subset_matching_enabled:", config.subset_matching_enabled)
    print("kalshi market:")
    pprint({"market_id": k_market.market_id, "event_title": k_market.event_title, "event_date": k_market.event_date})
    print("polymarket market:")
    pprint({"market_id": p_market.market_id, "event_title": p_market.event_title, "event_date": p_market.event_date})
    print("kalshi predicate:", {"team_a": k_pred.team_a, "team_b": k_pred.team_b})
    print("polymarket predicate:", {"team_a": p_pred.team_a, "team_b": p_pred.team_b})
    print("kalshi event_key:", k_key)
    print("polymarket event_key:", p_key)

    if k_key and p_key:
        print("event_date_equal:", k_key[2] == p_key[2])
        print(
            "event_key_subset_equivalent:",
            _event_key_equivalent(k_key, p_key),
        )
    else:
        print("event_key_subset_equivalent: False (missing event_key)")

    k_outcome = _normalize_outcome_label(k_market.outcome_label)
    p_outcome = _normalize_outcome_label(p_market.outcome_label)
    print("kalshi outcome_label:", k_outcome)
    print("polymarket outcome_label:", p_outcome)
    print("outcome_subset_equivalent:", _tokens_equivalent(k_outcome or "", p_outcome or ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
