import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

from arbv2.models import Market, MatchResult, PriceSnapshot, SportsPredicate
from arbv2.teams import canonicalize_team, league_hint_from_series_ticker, league_hint_from_text, league_hint_from_url


logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    venue TEXT NOT NULL,
    market_id TEXT NOT NULL,
    event_id TEXT,
    series_ticker TEXT,
    title TEXT,
    event_title TEXT,
    market_type TEXT,
    status TEXT,
    event_date DATE,
    outcome_label TEXT,
    raw_json TEXT,
    PRIMARY KEY (venue, market_id)
);

CREATE TABLE IF NOT EXISTS predicates (
    venue TEXT NOT NULL,
    market_id TEXT NOT NULL,
    event_id TEXT,
    team_a TEXT,
    team_b TEXT,
    winner TEXT,
    PRIMARY KEY (venue, market_id)
);

CREATE TABLE IF NOT EXISTS matches (
    kalshi_market_id TEXT NOT NULL,
    polymarket_market_id TEXT NOT NULL,
    reason TEXT,
    kalshi_title TEXT,
    polymarket_title TEXT,
    PRIMARY KEY (kalshi_market_id, polymarket_market_id)
);

CREATE TABLE IF NOT EXISTS prices (
    venue TEXT NOT NULL,
    market_id TEXT NOT NULL,
    outcome_label TEXT NOT NULL,
    best_bid REAL,
    best_ask REAL,
    last_trade REAL,
    ts_utc TEXT NOT NULL,
    raw_json TEXT,
    PRIMARY KEY (venue, market_id, outcome_label)
);

CREATE TABLE IF NOT EXISTS arb_scans (
    arb_type TEXT NOT NULL,
    kalshi_market_id TEXT NOT NULL,
    polymarket_market_id TEXT NOT NULL,
    expected_profit REAL,
    ts_utc TEXT NOT NULL,
    event_id TEXT,
    size REAL,
    event_title TEXT
);
"""


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str) -> None:
    conn = _connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate_markets_schema(conn)
        _migrate_predicates_schema(conn)
        _migrate_matches_schema(conn)
        _migrate_prices_schema(conn)
        _migrate_arb_scans_schema(conn)
        conn.commit()
    finally:
        conn.close()
    backfill_market_fields(db_path)


def upsert_markets(db_path: str, markets: Iterable[Market]) -> None:
    conn = _connect(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO markets (
                venue, market_id, event_id, series_ticker, title, event_title,
                market_type, status, event_date, outcome_label, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, market_id) DO UPDATE SET
                event_id=excluded.event_id,
                series_ticker=excluded.series_ticker,
                title=excluded.title,
                event_title=excluded.event_title,
                market_type=excluded.market_type,
                status=excluded.status,
                event_date=excluded.event_date,
                outcome_label=excluded.outcome_label,
                raw_json=excluded.raw_json
            """,
            [
                (
                    m.venue,
                    m.market_id,
                    m.event_id,
                    m.series_ticker,
                    m.title,
                    m.event_title,
                    m.market_type,
                    m.status,
                    m.event_date,
                    m.outcome_label,
                    json.dumps(m.raw_json),
                )
                for m in markets
            ],
        )
        conn.commit()
    finally:
        conn.close()


def upsert_predicates(db_path: str, predicates: Iterable[SportsPredicate]) -> None:
    conn = _connect(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO predicates (
                venue, market_id, event_id, team_a, team_b, winner
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, market_id) DO UPDATE SET
                event_id=excluded.event_id,
                team_a=excluded.team_a,
                team_b=excluded.team_b,
                winner=excluded.winner
            """,
            [
                (
                    p.venue,
                    p.market_id,
                    p.event_id,
                    p.team_a,
                    p.team_b,
                    p.winner,
                )
                for p in predicates
            ],
        )
        conn.commit()
    finally:
        conn.close()


def replace_matches(db_path: str, matches: Iterable[MatchResult]) -> None:
    conn = _connect(db_path)
    try:
        conn.execute("DELETE FROM matches")
        conn.executemany(
            """
            INSERT INTO matches (
                kalshi_market_id, polymarket_market_id, reason, kalshi_title, polymarket_title
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    m.kalshi_market_id,
                    m.polymarket_market_id,
                    m.reason,
                    m.kalshi_title,
                    m.polymarket_title,
                )
                for m in matches
            ],
        )
        conn.commit()
    finally:
        conn.close()


def insert_prices(db_path: str, snapshots: Iterable[PriceSnapshot]) -> None:
    conn = _connect(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO prices (
                venue, market_id, outcome_label, best_bid, best_ask, last_trade, ts_utc, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, market_id, outcome_label) DO UPDATE SET
                best_bid=excluded.best_bid,
                best_ask=excluded.best_ask,
                last_trade=excluded.last_trade,
                ts_utc=excluded.ts_utc,
                raw_json=excluded.raw_json
            """,
            [
                (
                    snap.venue,
                    snap.market_id,
                    snap.outcome_label,
                    snap.best_bid,
                    snap.best_ask,
                    snap.last_trade,
                    snap.ts_utc,
                    json.dumps(snap.raw_json) if snap.raw_json is not None else None,
                )
                for snap in snapshots
            ],
        )
        conn.commit()
    finally:
        conn.close()


def insert_arb_scans(db_path: str, rows: Iterable[tuple]) -> None:
    conn = _connect(db_path)
    try:
        rows_list = list(rows)
        if not rows_list:
            return
        conn.executemany(
            """
            DELETE FROM arb_scans
            WHERE arb_type=? AND kalshi_market_id=? AND polymarket_market_id=?
            """,
            [(r[0], r[1], r[2]) for r in rows_list],
        )
        conn.executemany(
            """
            INSERT INTO arb_scans (
                arb_type, kalshi_market_id, polymarket_market_id, expected_profit, ts_utc, event_id, size, event_title
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_list,
        )
        conn.commit()
    finally:
        conn.close()


def fetch_markets(db_path: str, venue: Optional[str] = None) -> List[Market]:
    conn = _connect(db_path)
    try:
        if venue:
            rows = conn.execute("SELECT * FROM markets WHERE venue=?", (venue,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM markets").fetchall()
        markets: List[Market] = []
        for row in rows:
            raw_json = json.loads(row["raw_json"]) if row["raw_json"] else {}
            markets.append(
                Market(
                    venue=row["venue"],
                    market_id=row["market_id"],
                    event_id=row["event_id"],
                    series_ticker=row["series_ticker"],
                    title=row["title"],
                    event_title=row["event_title"],
                    market_type=row["market_type"],
                    status=row["status"],
                    event_date=row["event_date"],
                    outcome_label=row["outcome_label"],
                    raw_json=raw_json,
                )
            )
        return markets
    finally:
        conn.close()


def fetch_matched_market_ids(db_path: str, venue: str) -> set:
    conn = _connect(db_path)
    try:
        if venue == "kalshi":
            rows = conn.execute("SELECT kalshi_market_id AS market_id FROM matches").fetchall()
        elif venue == "polymarket":
            rows = conn.execute("SELECT polymarket_market_id AS market_id FROM matches").fetchall()
        else:
            return set()
        return {row["market_id"] for row in rows if row["market_id"]}
    finally:
        conn.close()


def fetch_match_pairs(db_path: str) -> List[tuple]:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            "SELECT kalshi_market_id, polymarket_market_id FROM matches"
        ).fetchall()
        return [
            (row["kalshi_market_id"], row["polymarket_market_id"])
            for row in rows
            if row["kalshi_market_id"] and row["polymarket_market_id"]
        ]
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row["name"] for row in rows}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def _migrate_markets_schema(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(markets)").fetchall()
    if not rows:
        return
    existing = {row["name"] for row in rows}
    needs_rebuild = (
        "event_time" in existing
        or "event_date" not in existing
        or "outcome_label" not in existing
        or "close_time" in existing
    )
    if not needs_rebuild:
        _ensure_column(conn, "markets", "series_ticker", "TEXT")
        _ensure_column(conn, "markets", "market_type", "TEXT")
        _ensure_column(conn, "markets", "status", "TEXT")
        _ensure_column(conn, "markets", "event_date", "DATE")
        _ensure_column(conn, "markets", "outcome_label", "TEXT")
        return
    conn.execute("ALTER TABLE markets RENAME TO markets_old")
    conn.execute(
        """
        CREATE TABLE markets (
            venue TEXT NOT NULL,
            market_id TEXT NOT NULL,
            event_id TEXT,
            series_ticker TEXT,
            title TEXT,
            event_title TEXT,
            market_type TEXT,
            status TEXT,
            event_date DATE,
            outcome_label TEXT,
            raw_json TEXT,
            PRIMARY KEY (venue, market_id)
        )
        """
    )
    select_cols = {
        "venue": "venue",
        "market_id": "market_id",
        "event_id": "event_id",
        "series_ticker": "series_ticker",
        "title": "title",
        "event_title": "event_title",
        "market_type": "market_type",
        "status": "status",
        "raw_json": "raw_json",
    }
    for col in list(select_cols.keys()):
        if col not in existing:
            select_cols[col] = f"NULL AS {col}"
    conn.execute(
        """
        INSERT INTO markets (
            venue, market_id, event_id, series_ticker, title, event_title,
            market_type, status, event_date, outcome_label, raw_json
        )
        SELECT
            {venue}, {market_id}, {event_id}, {series_ticker}, {title}, {event_title},
            {market_type}, {status}, NULL, NULL, {raw_json}
        FROM markets_old
        """
        .format(**select_cols)
    )
    conn.execute("DROP TABLE markets_old")


def backfill_market_fields(db_path: str) -> None:
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT venue, market_id, raw_json, market_type, status, event_date, outcome_label
            FROM markets
            WHERE market_type IS NULL OR status IS NULL OR event_date IS NULL OR outcome_label IS NULL
            """
        ).fetchall()
        updates: List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], str, str]] = []
        for row in rows:
            raw_json = json.loads(row["raw_json"]) if row["raw_json"] else {}
            market_type = row["market_type"]
            status = row["status"]
            event_date = row["event_date"]
            outcome_label = row["outcome_label"]
            if not market_type:
                market_type = "sports_moneyline"
            if not status:
                status = _normalize_status(row["venue"], raw_json)
            if not event_date:
                event_date = _extract_event_date(row["venue"], raw_json)
            if not outcome_label:
                outcome_label = _extract_outcome_label(row["venue"], raw_json)
            updates.append((market_type, status, event_date, outcome_label, row["venue"], row["market_id"]))
        if updates:
            conn.executemany(
                """
                UPDATE markets
                SET market_type=?, status=?, event_date=?, outcome_label=?
                WHERE venue=? AND market_id=?
                """,
                updates,
            )
            conn.commit()
    finally:
        conn.close()


def log_market_stats(db_path: str) -> None:
    conn = _connect(db_path)
    try:
        by_venue = conn.execute(
            "SELECT venue, COUNT(*) AS count FROM markets GROUP BY venue"
        ).fetchall()
        by_type = conn.execute(
            "SELECT market_type, COUNT(*) AS count FROM markets GROUP BY market_type"
        ).fetchall()
        by_status = conn.execute(
            "SELECT status, COUNT(*) AS count FROM markets GROUP BY status"
        ).fetchall()
        null_event = conn.execute(
            """
            SELECT
                SUM(CASE WHEN event_date IS NULL THEN 1 ELSE 0 END) AS null_count,
                SUM(CASE WHEN event_date IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count
            FROM markets
            """
        ).fetchone()
        logger.info(
            "Market counts by venue: %s",
            [(row["venue"], row["count"]) for row in by_venue],
        )
        logger.info(
            "Market counts by type: %s",
            [(row["market_type"], row["count"]) for row in by_type],
        )
        logger.info(
            "Market counts by status: %s",
            [(row["status"], row["count"]) for row in by_status],
        )
        if null_event:
            logger.info(
                "Market event_date null=%s non_null=%s",
                null_event["null_count"],
                null_event["non_null_count"],
            )
        venues = [row["venue"] for row in by_venue]
        for venue in venues:
            samples = conn.execute(
                """
                SELECT title, event_date, status
                FROM markets
                WHERE venue=?
                LIMIT 3
                """,
                (venue,),
            ).fetchall()
            logger.info(
                "Market samples %s: %s",
                venue,
                [(row["title"], row["event_date"], row["status"]) for row in samples],
            )
    finally:
        conn.close()


def _normalize_status(venue: str, raw_json: dict) -> Optional[str]:
    if venue == "kalshi":
        result = str(raw_json.get("result") or "").strip()
        if result:
            return "resolved"
        status = str(raw_json.get("status") or "").strip().lower()
        if status == "active":
            return "open"
        if status in {"closed", "expired"}:
            return "closed"
        return None
    if venue == "polymarket":
        resolved = raw_json.get("resolved")
        archived = raw_json.get("archived")
        closed = raw_json.get("closed")
        active = raw_json.get("active")
        if resolved is True or archived is True:
            return "resolved"
        if closed is True:
            return "closed"
        if active is True and closed is False:
            return "open"
        return None
    return None




def _extract_event_date(venue: str, raw_json: dict) -> Optional[str]:
    if venue == "kalshi":
        value = raw_json.get("expected_expiration_time")
        return _to_utc_date(value)
    if venue == "polymarket":
        value = raw_json.get("gameStartTime") or raw_json.get("endDate") or raw_json.get("endDateIso")
        return _to_utc_date(value)
    return None


def _extract_outcome_label(venue: str, raw_json: dict) -> Optional[str]:
    if venue == "kalshi":
        value = raw_json.get("yes_sub_title") or raw_json.get("yes_subtitle")
        league_hint = raw_json.get("series_league") or league_hint_from_series_ticker(raw_json.get("series_ticker"))
        return _normalize_outcome(value, league_hint)
    if venue == "polymarket":
        question = str(raw_json.get("question") or "")
        group_title = str(raw_json.get("groupItemTitle") or "")
        league_hint = league_hint_from_url(raw_json.get("resolutionSource"))
        if not league_hint:
            league_hint = league_hint_from_text(str(raw_json.get("description") or "")) or league_hint_from_text(
                " ".join([question, group_title])
            )
        if "draw" in question.lower() or "draw" in group_title.lower():
            return "DRAW"
        if "tie" in question.lower() or "tie" in group_title.lower():
            return "DRAW"
        match = re.search(r"will\s+(.+?)\s+win", question, re.IGNORECASE)
        if match:
            return _normalize_outcome(match.group(1), league_hint)
    return None


def _normalize_outcome(value: Optional[str], league_hint: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"[^\w\s]", " ", str(value).upper())
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None
    if text in {"TIE", "DRAW"}:
        return "DRAW"
    return canonicalize_team(text, league_hint)


def _to_utc_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, str):
        try:
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date().isoformat()


def fetch_predicates(db_path: str, venue: Optional[str] = None) -> List[SportsPredicate]:
    conn = _connect(db_path)
    try:
        if venue:
            rows = conn.execute("SELECT * FROM predicates WHERE venue=?", (venue,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM predicates").fetchall()
        predicates: List[SportsPredicate] = []
        for row in rows:
            predicates.append(
                SportsPredicate(
                    venue=row["venue"],
                    market_id=row["market_id"],
                    event_id=row["event_id"],
                    team_a=row["team_a"],
                    team_b=row["team_b"],
                    winner=row["winner"],
                )
            )
        return predicates
    finally:
        conn.close()


def _migrate_predicates_schema(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(predicates)").fetchall()
    if not rows:
        return
    existing = {row["name"] for row in rows}
    if "event_time" not in existing:
        return
    conn.execute("ALTER TABLE predicates RENAME TO predicates_old")
    conn.execute(
        """
        CREATE TABLE predicates (
            venue TEXT NOT NULL,
            market_id TEXT NOT NULL,
            event_id TEXT,
            team_a TEXT,
            team_b TEXT,
            winner TEXT,
            PRIMARY KEY (venue, market_id)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO predicates (venue, market_id, event_id, team_a, team_b, winner)
        SELECT venue, market_id, event_id, team_a, team_b, winner
        FROM predicates_old
        """
    )
    conn.execute("DROP TABLE predicates_old")


def _migrate_matches_schema(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(matches)").fetchall()
    if not rows:
        return
    _ensure_column(conn, "matches", "kalshi_title", "TEXT")
    _ensure_column(conn, "matches", "polymarket_title", "TEXT")


def _migrate_prices_schema(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(prices)").fetchall()
    if not rows:
        return
    pk_cols = {row["name"] for row in rows if row["pk"]}
    required_pk = {"venue", "market_id", "outcome_label"}
    needs_rebuild = not required_pk.issubset(pk_cols)
    if not needs_rebuild:
        _ensure_column(conn, "prices", "best_bid", "REAL")
        _ensure_column(conn, "prices", "best_ask", "REAL")
        _ensure_column(conn, "prices", "last_trade", "REAL")
        _ensure_column(conn, "prices", "ts_utc", "TEXT")
        _ensure_column(conn, "prices", "raw_json", "TEXT")
        return

    conn.execute("ALTER TABLE prices RENAME TO prices_old")
    conn.execute(
        """
        CREATE TABLE prices (
            venue TEXT NOT NULL,
            market_id TEXT NOT NULL,
            outcome_label TEXT NOT NULL,
            best_bid REAL,
            best_ask REAL,
            last_trade REAL,
            ts_utc TEXT NOT NULL,
            raw_json TEXT,
            PRIMARY KEY (venue, market_id, outcome_label)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO prices (
            venue, market_id, outcome_label, best_bid, best_ask, last_trade, ts_utc, raw_json
        )
        SELECT venue, market_id, outcome_label, best_bid, best_ask, last_trade, ts_utc, raw_json
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY venue, market_id, outcome_label
                       ORDER BY ts_utc DESC
                   ) AS rn
            FROM prices_old
        )
        WHERE rn = 1
        """
    )
    conn.execute("DROP TABLE prices_old")


def _migrate_arb_scans_schema(conn: sqlite3.Connection) -> None:
    rows = conn.execute("PRAGMA table_info(arb_scans)").fetchall()
    if not rows:
        return
    _ensure_column(conn, "arb_scans", "event_id", "TEXT")
    _ensure_column(conn, "arb_scans", "size", "REAL")
    _ensure_column(conn, "arb_scans", "event_title", "TEXT")
