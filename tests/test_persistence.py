import csv
import json
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from arbv2.pricing import arb as arb_module
from arbv2.persistence import ACTIVE_RUNS, observe, end_run


class PersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        ACTIVE_RUNS.clear()

    def _signal(self, profit: float, size: float, legs_order: str = "kp") -> dict:
        legs = [
            {"venue": "kalshi", "market_id": "K-A", "outcome_label": "A", "side": "YES"},
            {"venue": "polymarket", "market_id": "P-B", "outcome_label": "B", "side": "YES"},
        ]
        if legs_order == "pk":
            legs = list(reversed(legs))
        return {
            "arb_type": "EVENT_OUTCOME",
            "direction": "KALSHI:A + POLY:B",
            "event_id": "EVT-1",
            "size": size,
            "profit": profit,
            "legs": legs,
        }

    def test_lifetime_until_break_even(self) -> None:
        with patch.object(arb_module, "MIN_EDGE_BPS", 40):
            with tempfile.TemporaryDirectory() as tmpdir:
                csv_path = f"{tmpdir}/arbv2_persistence.csv"
                now = datetime(2026, 1, 1, tzinfo=timezone.utc)
                observe(
                    self._signal(profit=0.05, size=10),
                    now_utc=now,
                    scan_interval_ms=500,
                    csv_path=csv_path,
                )
                observe(
                    self._signal(profit=0.01, size=10),
                    now_utc=now + timedelta(milliseconds=500),
                    scan_interval_ms=500,
                    csv_path=csv_path,
                )
                observe(
                    self._signal(profit=-0.01, size=10),
                    now_utc=now + timedelta(milliseconds=1000),
                    scan_interval_ms=500,
                    csv_path=csv_path,
                )
                self.assertEqual(len(ACTIVE_RUNS), 0)
                with open(csv_path, "r", encoding="utf-8") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(len(rows), 1)
                row = rows[0]
                self.assertEqual(int(row["ticks_alive"]), 3)
                edge_series = json.loads(row["edge_series_json"])
                self.assertEqual(len(edge_series), 3)
                self.assertLessEqual(float(row["last_edge_bps"]), 0.0)

    def test_fingerprint_stable_with_leg_order(self) -> None:
        with patch.object(arb_module, "MIN_EDGE_BPS", 40):
            with tempfile.TemporaryDirectory() as tmpdir:
                csv_path = f"{tmpdir}/arbv2_persistence.csv"
                now = datetime(2026, 1, 1, tzinfo=timezone.utc)
                observe(
                    self._signal(profit=0.05, size=10, legs_order="kp"),
                    now_utc=now,
                    scan_interval_ms=500,
                    csv_path=csv_path,
                )
                observe(
                    self._signal(profit=0.06, size=10, legs_order="pk"),
                    now_utc=now + timedelta(milliseconds=500),
                    scan_interval_ms=500,
                    csv_path=csv_path,
                )
                self.assertEqual(len(ACTIVE_RUNS), 1)

    def test_end_run_emits_on_reject(self) -> None:
        with patch.object(arb_module, "MIN_EDGE_BPS", 40):
            with tempfile.TemporaryDirectory() as tmpdir:
                csv_path = f"{tmpdir}/arbv2_persistence.csv"
                now = datetime(2026, 1, 1, tzinfo=timezone.utc)
                observe(
                    self._signal(profit=0.05, size=10),
                    now_utc=now,
                    scan_interval_ms=500,
                    csv_path=csv_path,
                )
                end_run(
                    self._signal(profit=0.04, size=10),
                    now_utc=now + timedelta(milliseconds=500),
                    scan_interval_ms=500,
                    csv_path=csv_path,
                )
                self.assertEqual(len(ACTIVE_RUNS), 0)
                with open(csv_path, "r", encoding="utf-8") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(len(rows), 1)
