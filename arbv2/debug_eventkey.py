import argparse
import sqlite3
import re
from typing import Dict, List, Optional, Tuple


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _query_markets(conn: sqlite3.Connection, team_a: str, team_b: str, venue: str) -> List[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT m.market_id, m.event_title, m.event_date, m.outcome_label, p.team_a, p.team_b
        FROM markets m
        LEFT JOIN predicates p ON p.venue=m.venue AND p.market_id=m.market_id
        WHERE m.venue=? AND m.outcome_label IN (?, ?, 'DRAW')
        ORDER BY m.event_date
        """,
        (venue, team_a, team_b),
    ).fetchall()
    return _filter_draws_for_event(rows, team_a, team_b)


def main() -> int:
    parser = argparse.ArgumentParser(prog="arbv2.debug_eventkey")
    parser.add_argument("team_a")
    parser.add_argument("team_b")
    parser.add_argument("--db", default="arbv2.db")
    args = parser.parse_args()

    conn = _connect(args.db)
    try:
        rows_by_venue: Dict[str, List[sqlite3.Row]] = {}
        for venue in ("kalshi", "polymarket"):
            rows = _query_markets(conn, args.team_a, args.team_b, venue)
            rows_by_venue[venue] = rows
            print(f"{venue} rows={len(rows)}")
            for row in rows[:5]:
                print(dict(row))
        print("canonical_event_keys")
        for venue in ("kalshi", "polymarket"):
            keys = _event_keys(rows_by_venue[venue])
            print(venue, sorted(keys))
        kalshi_dates = {row["event_date"] for row in rows_by_venue["kalshi"]}
        poly_dates = {row["event_date"] for row in rows_by_venue["polymarket"]}
        print("kalshi_dates", sorted(d for d in kalshi_dates if d))
        print("polymarket_dates", sorted(d for d in poly_dates if d))
        print("matched_dates", sorted(d for d in kalshi_dates.intersection(poly_dates) if d))
        _print_match_rows(conn, rows_by_venue)
    finally:
        conn.close()
    return 0


def _event_keys(rows: List[sqlite3.Row]) -> List[Tuple[str, str, str]]:
    keys = []
    for row in rows:
        team_a = _normalize_team(row["team_a"])
        team_b = _normalize_team(row["team_b"])
        event_date = row["event_date"]
        if not team_a or not team_b or not event_date:
            continue
        left, right = sorted([team_a, team_b])
        keys.append((left, right, event_date))
    return sorted(set(keys))


def _filter_draws_for_event(rows: List[sqlite3.Row], team_a: str, team_b: str) -> List[sqlite3.Row]:
    if not rows:
        return rows
    event_dates = set()
    for row in rows:
        if row["outcome_label"] != "DRAW":
            event_dates.add(row["event_date"])
    if not event_dates:
        return rows
    filtered = []
    for row in rows:
        if row["outcome_label"] != "DRAW":
            filtered.append(row)
            continue
        if row["event_date"] in event_dates:
            filtered.append(row)
    return filtered


def _print_match_rows(conn: sqlite3.Connection, rows_by_venue: Dict[str, List[sqlite3.Row]]) -> None:
    kalshi_ids = {row["market_id"] for row in rows_by_venue.get("kalshi", [])}
    poly_ids = {row["market_id"] for row in rows_by_venue.get("polymarket", [])}
    if not kalshi_ids or not poly_ids:
        print("matches_for_event 0")
        return
    placeholders_k = ",".join("?" for _ in kalshi_ids)
    placeholders_p = ",".join("?" for _ in poly_ids)
    query = (
        f"SELECT kalshi_market_id, polymarket_market_id, reason FROM matches "
        f"WHERE kalshi_market_id IN ({placeholders_k}) AND polymarket_market_id IN ({placeholders_p})"
    )
    rows = conn.execute(query, tuple(kalshi_ids) + tuple(poly_ids)).fetchall()
    print("matches_for_event", len(rows))
    for row in rows:
        print(dict(row))


def _normalize_team(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"[^\w\s]", " ", value.upper())
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


if __name__ == "__main__":
    raise SystemExit(main())
