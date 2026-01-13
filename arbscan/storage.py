from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Iterable, Optional


def init_db(path: str) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS markets (
                venue TEXT NOT NULL,
                market_id TEXT NOT NULL,
                event_id TEXT,
                title TEXT,
                domain TEXT,
                canonical_predicate TEXT,
                close_time TEXT,
                status TEXT,
                yes_token_id TEXT,
                no_token_id TEXT,
                rules_text TEXT,
                raw_json TEXT,
                updated_ts TEXT,
                PRIMARY KEY (venue, market_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS specs (
                venue TEXT NOT NULL,
                market_id TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                updated_ts TEXT,
                PRIMARY KEY (venue, market_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS matches (
                kalshi_ticker TEXT NOT NULL,
                polymarket_id TEXT NOT NULL,
                score REAL NOT NULL,
                status TEXT NOT NULL,
                reasons_json TEXT NOT NULL,
                updated_ts TEXT,
                PRIMARY KEY (kalshi_ticker, polymarket_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quotes (
                venue TEXT NOT NULL,
                market_id TEXT NOT NULL,
                yes_bid REAL,
                yes_ask REAL,
                no_bid REAL,
                no_ask REAL,
                yes_bid_size REAL,
                yes_ask_size REAL,
                no_bid_size REAL,
                no_ask_size REAL,
                ts TEXT,
                PRIMARY KEY (venue, market_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                match_key TEXT NOT NULL,
                direction TEXT NOT NULL,
                yes_leg TEXT NOT NULL,
                no_leg TEXT NOT NULL,
                margin REAL NOT NULL,
                size REAL NOT NULL,
                ts TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_matches (
                match_id TEXT NOT NULL,
                kalshi_market_id TEXT NOT NULL,
                polymarket_market_id TEXT NOT NULL,
                domain TEXT,
                canonical_predicate TEXT,
                active INTEGER NOT NULL,
                created_at TEXT,
                updated_ts TEXT,
                PRIMARY KEY (match_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_state (
                market_id TEXT NOT NULL,
                venue TEXT NOT NULL,
                yes_bid REAL,
                yes_ask REAL,
                no_bid REAL,
                no_ask REAL,
                liquidity REAL,
                last_updated TEXT,
                PRIMARY KEY (venue, market_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS event_groups (
                venue TEXT NOT NULL,
                event_id TEXT NOT NULL,
                title TEXT,
                close_time TEXT,
                rules_text TEXT,
                event_domain TEXT,
                raw_json TEXT,
                updated_ts TEXT,
                PRIMARY KEY (venue, event_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS outcomes (
                venue TEXT NOT NULL,
                event_id TEXT NOT NULL,
                outcome_key TEXT NOT NULL,
                display_label TEXT,
                market_id TEXT NOT NULL,
                rules_text TEXT,
                event_domain TEXT,
                predicate_json TEXT,
                yes_token_id TEXT,
                no_token_id TEXT,
                updated_ts TEXT,
                PRIMARY KEY (venue, market_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS event_matches (
                kalshi_event_id TEXT NOT NULL,
                polymarket_event_id TEXT NOT NULL,
                score REAL NOT NULL,
                status TEXT NOT NULL,
                reasons_json TEXT NOT NULL,
                updated_ts TEXT,
                PRIMARY KEY (kalshi_event_id, polymarket_event_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS outcome_matches (
                kalshi_market_id TEXT NOT NULL,
                polymarket_market_id TEXT NOT NULL,
                outcome_key TEXT NOT NULL,
                score REAL NOT NULL,
                status TEXT NOT NULL,
                reasons_json TEXT NOT NULL,
                updated_ts TEXT,
                PRIMARY KEY (kalshi_market_id, polymarket_market_id)
            )
            """
        )
        _ensure_column(cur, "markets", "event_id", "TEXT")
        _ensure_column(cur, "markets", "domain", "TEXT")
        _ensure_column(cur, "markets", "canonical_predicate", "TEXT")
        _ensure_column(cur, "markets", "status", "TEXT")
        _ensure_column(cur, "markets", "yes_token_id", "TEXT")
        _ensure_column(cur, "markets", "no_token_id", "TEXT")
        _ensure_column(cur, "event_groups", "event_domain", "TEXT")
        _ensure_column(cur, "outcomes", "event_domain", "TEXT")
        _ensure_column(cur, "outcomes", "predicate_json", "TEXT")
        conn.commit()
    finally:
        conn.close()


def _ensure_column(cur: sqlite3.Cursor, table: str, column: str, col_type: str) -> None:
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
    except sqlite3.OperationalError:
        return


def upsert_market(
    path: str,
    venue: str,
    market_id: str,
    event_id: Optional[str],
    title: str,
    domain: Optional[str],
    canonical_predicate: Optional[str],
    close_time: Optional[str],
    status: Optional[str],
    rules_text: str,
    yes_token_id: Optional[str],
    no_token_id: Optional[str],
    raw_json: dict,
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO markets (
                venue, market_id, event_id, title, domain, canonical_predicate,
                close_time, status, yes_token_id, no_token_id, rules_text, raw_json, updated_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, market_id) DO UPDATE SET
                event_id=excluded.event_id,
                title=excluded.title,
                domain=excluded.domain,
                canonical_predicate=excluded.canonical_predicate,
                close_time=excluded.close_time,
                status=excluded.status,
                yes_token_id=excluded.yes_token_id,
                no_token_id=excluded.no_token_id,
                rules_text=excluded.rules_text,
                raw_json=excluded.raw_json,
                updated_ts=excluded.updated_ts
            """,
            (
                venue,
                market_id,
                event_id,
                title,
                domain,
                canonical_predicate,
                close_time,
                status,
                yes_token_id,
                no_token_id,
                rules_text,
                json.dumps(raw_json),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_spec(path: str, venue: str, market_id: str, spec: dict) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO specs (venue, market_id, spec_json, updated_ts)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(venue, market_id) DO UPDATE SET
                spec_json=excluded.spec_json,
                updated_ts=excluded.updated_ts
            """,
            (venue, market_id, json.dumps(spec), datetime.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_match(
    path: str,
    kalshi_ticker: str,
    polymarket_id: str,
    score: float,
    status: str,
    reasons: dict,
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO matches (kalshi_ticker, polymarket_id, score, status, reasons_json, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(kalshi_ticker, polymarket_id) DO UPDATE SET
                score=excluded.score,
                status=excluded.status,
                reasons_json=excluded.reasons_json,
                updated_ts=excluded.updated_ts
            """,
            (
                kalshi_ticker,
                polymarket_id,
                score,
                status,
                json.dumps(reasons),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_market_match(
    path: str,
    match_id: str,
    kalshi_market_id: str,
    polymarket_market_id: str,
    domain: Optional[str],
    canonical_predicate: Optional[str],
    active: bool,
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO market_matches (
                match_id, kalshi_market_id, polymarket_market_id, domain,
                canonical_predicate, active, created_at, updated_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                kalshi_market_id=excluded.kalshi_market_id,
                polymarket_market_id=excluded.polymarket_market_id,
                domain=excluded.domain,
                canonical_predicate=excluded.canonical_predicate,
                active=excluded.active,
                updated_ts=excluded.updated_ts
            """,
            (
                match_id,
                kalshi_market_id,
                polymarket_market_id,
                domain,
                canonical_predicate,
                1 if active else 0,
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_active_market_matches(path: str, domain: Optional[str] = None) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if domain:
            cur.execute(
                """
                SELECT
                    mm.match_id,
                    mm.kalshi_market_id,
                    mm.polymarket_market_id,
                    mm.domain,
                    mm.canonical_predicate,
                    km.status as kalshi_status,
                    pm.status as polymarket_status,
                    pm.yes_token_id,
                    pm.no_token_id
                FROM market_matches mm
                LEFT JOIN markets km
                    ON km.venue='kalshi' AND km.market_id=mm.kalshi_market_id
                LEFT JOIN markets pm
                    ON pm.venue='polymarket' AND pm.market_id=mm.polymarket_market_id
                WHERE mm.active=1 AND mm.domain=?
                """,
                (domain,),
            )
        else:
            cur.execute(
                """
                SELECT
                    mm.match_id,
                    mm.kalshi_market_id,
                    mm.polymarket_market_id,
                    mm.domain,
                    mm.canonical_predicate,
                    km.status as kalshi_status,
                    pm.status as polymarket_status,
                    pm.yes_token_id,
                    pm.no_token_id
                FROM market_matches mm
                LEFT JOIN markets km
                    ON km.venue='kalshi' AND km.market_id=mm.kalshi_market_id
                LEFT JOIN markets pm
                    ON pm.venue='polymarket' AND pm.market_id=mm.polymarket_market_id
                WHERE mm.active=1
                """
            )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "match_id": row[0],
                    "kalshi_market_id": row[1],
                    "polymarket_market_id": row[2],
                    "domain": row[3],
                    "canonical_predicate": row[4],
                    "kalshi_status": row[5],
                    "polymarket_status": row[6],
                    "yes_token_id": row[7],
                    "no_token_id": row[8],
                }
            )
        return results
    finally:
        conn.close()


def upsert_market_state(
    path: str,
    venue: str,
    market_id: str,
    yes_bid: Optional[float],
    yes_ask: Optional[float],
    no_bid: Optional[float],
    no_ask: Optional[float],
    liquidity: Optional[float],
    last_updated: Optional[str],
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO market_state (
                market_id, venue, yes_bid, yes_ask, no_bid, no_ask, liquidity, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, market_id) DO UPDATE SET
                yes_bid=excluded.yes_bid,
                yes_ask=excluded.yes_ask,
                no_bid=excluded.no_bid,
                no_ask=excluded.no_ask,
                liquidity=excluded.liquidity,
                last_updated=excluded.last_updated
            """,
            (
                market_id,
                venue,
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                liquidity,
                last_updated,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def mark_markets_closed(path: str, venue: str) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE markets SET status='closed' WHERE venue=?", (venue,))
        conn.commit()
    finally:
        conn.close()


def upsert_event_group(
    path: str,
    venue: str,
    event_id: str,
    title: str,
    close_time: Optional[str],
    rules_text: str,
    event_domain: str,
    raw_json: dict,
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO event_groups (venue, event_id, title, close_time, rules_text, event_domain, raw_json, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, event_id) DO UPDATE SET
                title=excluded.title,
                close_time=excluded.close_time,
                rules_text=excluded.rules_text,
                event_domain=excluded.event_domain,
                raw_json=excluded.raw_json,
                updated_ts=excluded.updated_ts
            """,
            (
                venue,
                event_id,
                title,
                close_time,
                rules_text,
                event_domain,
                json.dumps(raw_json),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_outcome(
    path: str,
    venue: str,
    event_id: str,
    outcome_key: str,
    display_label: str,
    market_id: str,
    rules_text: str,
    event_domain: str,
    predicate_json: Optional[str],
    yes_token_id: Optional[str],
    no_token_id: Optional[str],
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO outcomes (
                venue, event_id, outcome_key, display_label, market_id, rules_text,
                event_domain, predicate_json, yes_token_id, no_token_id, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, market_id) DO UPDATE SET
                event_id=excluded.event_id,
                outcome_key=excluded.outcome_key,
                display_label=excluded.display_label,
                rules_text=excluded.rules_text,
                event_domain=excluded.event_domain,
                predicate_json=excluded.predicate_json,
                yes_token_id=excluded.yes_token_id,
                no_token_id=excluded.no_token_id,
                updated_ts=excluded.updated_ts
            """,
            (
                venue,
                event_id,
                outcome_key,
                display_label,
                market_id,
                rules_text,
                event_domain,
                predicate_json,
                yes_token_id,
                no_token_id,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_event_match(
    path: str,
    kalshi_event_id: str,
    polymarket_event_id: str,
    score: float,
    status: str,
    reasons: dict,
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO event_matches (kalshi_event_id, polymarket_event_id, score, status, reasons_json, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(kalshi_event_id, polymarket_event_id) DO UPDATE SET
                score=excluded.score,
                status=excluded.status,
                reasons_json=excluded.reasons_json,
                updated_ts=excluded.updated_ts
            """,
            (
                kalshi_event_id,
                polymarket_event_id,
                score,
                status,
                json.dumps(reasons),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_outcome_match(
    path: str,
    kalshi_market_id: str,
    polymarket_market_id: str,
    outcome_key: str,
    score: float,
    status: str,
    reasons: dict,
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO outcome_matches (kalshi_market_id, polymarket_market_id, outcome_key, score, status, reasons_json, updated_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(kalshi_market_id, polymarket_market_id) DO UPDATE SET
                outcome_key=excluded.outcome_key,
                score=excluded.score,
                status=excluded.status,
                reasons_json=excluded.reasons_json,
                updated_ts=excluded.updated_ts
            """,
            (
                kalshi_market_id,
                polymarket_market_id,
                outcome_key,
                score,
                status,
                json.dumps(reasons),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_specs(path: str, venue: Optional[str] = None) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if venue:
            cur.execute("SELECT spec_json FROM specs WHERE venue=?", (venue,))
        else:
            cur.execute("SELECT spec_json FROM specs")
        rows = cur.fetchall()
        return [json.loads(row[0]) for row in rows]
    finally:
        conn.close()


def list_matches(path: str, statuses: Optional[list[str]] = None) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            cur.execute(
                f"SELECT kalshi_ticker, polymarket_id, score, status, reasons_json FROM matches WHERE status IN ({placeholders})",
                tuple(statuses),
            )
        else:
            cur.execute(
                "SELECT kalshi_ticker, polymarket_id, score, status, reasons_json FROM matches"
            )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "kalshi_ticker": row[0],
                    "polymarket_id": row[1],
                    "score": row[2],
                    "status": row[3],
                    "reasons": json.loads(row[4]),
                }
            )
        return results
    finally:
        conn.close()


def list_markets_for_event(path: str, venue: str, event_id: str) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT market_id, title, domain, canonical_predicate, close_time, status
            FROM markets WHERE venue=? AND event_id=?
            """,
            (venue, event_id),
        )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "market_id": row[0],
                    "title": row[1],
                    "domain": row[2],
                    "canonical_predicate": row[3],
                    "close_time": row[4],
                    "status": row[5],
                }
            )
        return results
    finally:
        conn.close()


def list_market_matches(path: str, active_only: bool = False) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if active_only:
            cur.execute(
                """
                SELECT match_id, kalshi_market_id, polymarket_market_id, domain, canonical_predicate, active
                FROM market_matches WHERE active=1
                """
            )
        else:
            cur.execute(
                """
                SELECT match_id, kalshi_market_id, polymarket_market_id, domain, canonical_predicate, active
                FROM market_matches
                """
            )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "match_id": row[0],
                    "kalshi_market_id": row[1],
                    "polymarket_market_id": row[2],
                    "domain": row[3],
                    "canonical_predicate": row[4],
                    "active": bool(row[5]),
                }
            )
        return results
    finally:
        conn.close()


def list_markets_by_venue(path: str, venue: str, status: Optional[str] = None) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if status:
            cur.execute(
                """
            SELECT market_id, title, domain, canonical_predicate, status, close_time
                FROM markets WHERE venue=? AND status=?
                """,
                (venue, status),
            )
        else:
            cur.execute(
                """
            SELECT market_id, title, domain, canonical_predicate, status, close_time
                FROM markets WHERE venue=?
                """,
                (venue,),
            )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "market_id": row[0],
                    "title": row[1],
                    "domain": row[2],
                    "canonical_predicate": row[3],
                    "status": row[4],
                    "close_time": row[5],
                }
            )
        return results
    finally:
        conn.close()


def list_event_matches(path: str, statuses: Optional[list[str]] = None) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            cur.execute(
                f"SELECT kalshi_event_id, polymarket_event_id, score, status, reasons_json FROM event_matches WHERE status IN ({placeholders})",
                tuple(statuses),
            )
        else:
            cur.execute(
                "SELECT kalshi_event_id, polymarket_event_id, score, status, reasons_json FROM event_matches"
            )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "kalshi_event_id": row[0],
                    "polymarket_event_id": row[1],
                    "score": row[2],
                    "status": row[3],
                    "reasons": json.loads(row[4]),
                }
            )
        return results
    finally:
        conn.close()


def list_outcome_matches(path: str, statuses: Optional[list[str]] = None) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            cur.execute(
                f"SELECT kalshi_market_id, polymarket_market_id, outcome_key, score, status, reasons_json FROM outcome_matches WHERE status IN ({placeholders})",
                tuple(statuses),
            )
        else:
            cur.execute(
                "SELECT kalshi_market_id, polymarket_market_id, outcome_key, score, status, reasons_json FROM outcome_matches"
            )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "kalshi_market_id": row[0],
                    "polymarket_market_id": row[1],
                    "outcome_key": row[2],
                    "score": row[3],
                    "status": row[4],
                    "reasons": json.loads(row[5]),
                }
            )
        return results
    finally:
        conn.close()


def get_match(path: str, kalshi_ticker: str, polymarket_id: str) -> Optional[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT kalshi_ticker, polymarket_id, score, status, reasons_json FROM matches WHERE kalshi_ticker=? AND polymarket_id=?",
            (kalshi_ticker, polymarket_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "kalshi_ticker": row[0],
            "polymarket_id": row[1],
            "score": row[2],
            "status": row[3],
            "reasons": json.loads(row[4]),
        }
    finally:
        conn.close()


def get_event_group(path: str, venue: str, event_id: str) -> Optional[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT raw_json, event_domain FROM event_groups WHERE venue=? AND event_id=?",
            (venue, event_id),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        payload = json.loads(row[0])
        if row[1]:
            payload["event_domain"] = row[1]
        return payload
    finally:
        conn.close()


def get_outcomes_for_event(path: str, venue: str, event_id: str) -> list[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT outcome_key, display_label, market_id, rules_text, event_domain, predicate_json, yes_token_id, no_token_id FROM outcomes WHERE venue=? AND event_id=?",
            (venue, event_id),
        )
        rows = cur.fetchall()
        results = []
        for row in rows:
            results.append(
                {
                    "outcome_key": row[0],
                    "display_label": row[1],
                    "market_id": row[2],
                    "rules_text": row[3],
                    "event_domain": row[4],
                    "predicate_json": row[5],
                    "yes_token_id": row[6],
                    "no_token_id": row[7],
                }
            )
        return results
    finally:
        conn.close()


def get_outcome_by_market(path: str, venue: str, market_id: str) -> Optional[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT outcome_key, display_label, event_id, rules_text, event_domain, predicate_json, yes_token_id, no_token_id FROM outcomes WHERE venue=? AND market_id=?",
            (venue, market_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "outcome_key": row[0],
            "display_label": row[1],
            "event_id": row[2],
            "rules_text": row[3],
            "event_domain": row[4],
            "predicate_json": row[5],
            "yes_token_id": row[6],
            "no_token_id": row[7],
        }
    finally:
        conn.close()


def get_spec(path: str, venue: str, market_id: str) -> Optional[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT spec_json FROM specs WHERE venue=? AND market_id=?", (venue, market_id)
        )
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])
    finally:
        conn.close()


def get_market_raw(path: str, venue: str, market_id: str) -> Optional[dict]:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT raw_json FROM markets WHERE venue=? AND market_id=?", (venue, market_id)
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        return json.loads(row[0])
    finally:
        conn.close()


def upsert_quote(path: str, venue: str, market_id: str, quote: dict) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO quotes (
                venue, market_id, yes_bid, yes_ask, no_bid, no_ask,
                yes_bid_size, yes_ask_size, no_bid_size, no_ask_size, ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, market_id) DO UPDATE SET
                yes_bid=excluded.yes_bid,
                yes_ask=excluded.yes_ask,
                no_bid=excluded.no_bid,
                no_ask=excluded.no_ask,
                yes_bid_size=excluded.yes_bid_size,
                yes_ask_size=excluded.yes_ask_size,
                no_bid_size=excluded.no_bid_size,
                no_ask_size=excluded.no_ask_size,
                ts=excluded.ts
            """,
            (
                venue,
                market_id,
                quote.get("yes_bid"),
                quote.get("yes_ask"),
                quote.get("no_bid"),
                quote.get("no_ask"),
                quote.get("yes_bid_size"),
                quote.get("yes_ask_size"),
                quote.get("no_bid_size"),
                quote.get("no_ask_size"),
                quote.get("ts"),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def record_signal(
    path: str,
    match_key: str,
    direction: str,
    yes_leg: str,
    no_leg: str,
    margin: float,
    size: float,
) -> None:
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO signals (match_key, direction, yes_leg, no_leg, margin, size, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match_key,
                direction,
                yes_leg,
                no_leg,
                margin,
                size,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
