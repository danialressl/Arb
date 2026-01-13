from __future__ import annotations

import logging
import sqlite3

from arbscan.config import AppConfig

logger = logging.getLogger(__name__)


def debug_db(config: AppConfig) -> None:
    conn = sqlite3.connect(config.db_path)
    try:
        cur = conn.cursor()
        counts = {}
        for venue in ("kalshi", "polymarket"):
            cur.execute("SELECT COUNT(*) FROM markets WHERE venue=?", (venue,))
            counts[f"markets_{venue}"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM specs WHERE venue=?", (venue,))
            counts[f"specs_{venue}"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM event_groups WHERE venue=?", (venue,))
            counts[f"event_groups_{venue}"] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM outcomes WHERE venue=?", (venue,))
            counts[f"outcomes_{venue}"] = cur.fetchone()[0]

        cur.execute("SELECT status, COUNT(*) FROM matches GROUP BY status")
        matches = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT status, COUNT(*) FROM event_matches GROUP BY status")
        event_matches = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT status, COUNT(*) FROM outcome_matches GROUP BY status")
        outcome_matches = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute("SELECT COUNT(*) FROM market_matches")
        market_matches = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM market_matches WHERE active=1")
        market_matches_active = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM market_state")
        market_state = cur.fetchone()[0]

        print("DB stats:")
        for key, value in counts.items():
            print(f"  {key}: {value}")
        print(f"  matches: {matches}")
        print(f"  event_matches: {event_matches}")
        print(f"  outcome_matches: {outcome_matches}")
        print(f"  market_matches: {market_matches} (active={market_matches_active})")
        print(f"  market_state: {market_state}")
    finally:
        conn.close()
