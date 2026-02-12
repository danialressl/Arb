import os
import sqlite3
import tempfile
import unittest

from arbv2.models import MatchResult, PriceSnapshot
from arbv2.storage import (
    init_db,
    insert_arb_scans,
    insert_prices,
    prune_orphaned_runtime_rows,
    replace_matches,
)


class StoragePruneOrphansTests(unittest.TestCase):
    def test_prune_removes_unmatched_prices_and_scans(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "arbv2.db")
            init_db(db_path)

            replace_matches(
                db_path,
                [
                    MatchResult(
                        kalshi_market_id="K1",
                        polymarket_market_id="P1",
                        equivalent=True,
                        reason="test",
                    )
                ],
            )

            insert_prices(
                db_path,
                [
                    PriceSnapshot("kalshi", "K1", "A", 0.4, 0.5, None, "2026-02-11T00:00:00+00:00", None),
                    PriceSnapshot("polymarket", "P1", "A", 0.4, 0.5, None, "2026-02-11T00:00:00+00:00", None),
                    PriceSnapshot("kalshi", "KX", "A", 0.4, 0.5, None, "2026-02-11T00:00:00+00:00", None),
                    PriceSnapshot("polymarket", "PX", "A", 0.4, 0.5, None, "2026-02-11T00:00:00+00:00", None),
                ],
            )

            insert_arb_scans(
                db_path,
                [
                    ("EVENT_OUTCOME", "K1", "P1", 1.0, "2026-02-11T00:00:00+00:00", "E1", 1.0, "Matched"),
                    ("EVENT_OUTCOME", "KX", "PX", 1.0, "2026-02-11T00:00:00+00:00", "EX", 1.0, "Orphan"),
                ],
            )

            pruned = prune_orphaned_runtime_rows(db_path)
            self.assertEqual(pruned["prices_deleted"], 2)
            self.assertEqual(pruned["arb_scans_deleted"], 1)

            conn = sqlite3.connect(db_path)
            try:
                prices = conn.execute("SELECT venue, market_id FROM prices ORDER BY venue, market_id").fetchall()
                scans = conn.execute(
                    "SELECT kalshi_market_id, polymarket_market_id FROM arb_scans ORDER BY kalshi_market_id, polymarket_market_id"
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(prices, [("kalshi", "K1"), ("polymarket", "P1")])
            self.assertEqual(scans, [("K1", "P1")])


if __name__ == "__main__":
    unittest.main()
