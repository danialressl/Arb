import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from arbv2.pricing import arb as arb_module
from arbv2.pricing.arb import ORDERBOOKS
from arbv2.pricing.kalshi import _apply_delta, _build_books, _levels_from_snapshot, _publish_books


class KalshiWsBookTests(unittest.TestCase):
    def setUp(self) -> None:
        ORDERBOOKS.clear()

    def test_snapshot_builds_books(self) -> None:
        yes_levels = _levels_from_snapshot([["0.080", 300], ["0.220", 333]])
        no_levels = _levels_from_snapshot([["0.540", 20], ["0.560", 146]])
        bids_yes, asks_yes, bids_no, asks_no = _build_books(yes_levels, no_levels)
        self.assertEqual(bids_yes[0][0], 0.22)
        self.assertEqual(bids_no[0][0], 0.56)
        self.assertAlmostEqual(asks_yes[0][0], 0.44, places=6)
        self.assertAlmostEqual(asks_no[0][0], 0.78, places=6)

    def test_delta_removes_level(self) -> None:
        levels = {0.5: 10.0}
        _apply_delta(levels, 0.5, -10.0)
        self.assertNotIn(0.5, levels)

    def test_publish_books_updates_orderbooks(self) -> None:
        yes_levels = {0.3: 100.0}
        no_levels = {0.6: 50.0}
        ts_dt = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with patch.object(arb_module.time, "time", return_value=1234.0):
            _publish_books("K-TEST", "TEAM_A", yes_levels, no_levels, ts_dt)
        yes_book = ORDERBOOKS.get(("kalshi", "K-TEST", "YES"))
        no_book = ORDERBOOKS.get(("kalshi", "K-TEST", "NO"))
        self.assertIsNotNone(yes_book)
        self.assertIsNotNone(no_book)
        self.assertEqual(yes_book["bids"][0][0], 0.3)
        self.assertEqual(yes_book["asks"][0][0], 0.4)
        self.assertEqual(no_book["bids"][0][0], 0.6)
        self.assertEqual(no_book["asks"][0][0], 0.7)


if __name__ == "__main__":
    unittest.main()
