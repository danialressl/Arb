import csv
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from arbv2 import cli as cli_module
from arbv2.pricing.arb import ORDERBOOKS, update_orderbook
from arbv2.storage import fetch_pending_signals, init_db


class ExecutionIntentLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        ORDERBOOKS.clear()
        cli_module.ACTIVE_CONFIRMED_SIGNALS.clear()
        cli_module.PENDING_SIGNALS_HYDRATED = False

    def _seed_books(self, kalshi_ask: float, polymarket_ask: float) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(kalshi_ask, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(polymarket_ask, 500)], last_update_ts_utc=now)

    def test_backfill_replaces_row_with_signal_duration(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "arbv2_execution_intents.csv")
            db_path = os.path.join(tmpdir, "arbv2.db")
            init_db(db_path)
            signal = {
                "event_id": "E1",
                "size": 100.0,
                "detected_ts": 1000,
                "legs": [
                    {"venue": "kalshi", "market_id": "K-A", "outcome_label": "A", "side": "YES"},
                    {"venue": "polymarket", "market_id": "P-B", "outcome_label": "B", "side": "YES"},
                ],
            }
            decision = {
                "edge_bps": 100.0,
                "expected_pnl_usd": 1.0,
                "limit_prices": [0.4, 0.5],
            }
            self._seed_books(0.4, 0.5)
            with patch.object(cli_module.time, "time", return_value=2.0):
                intent = cli_module._build_execution_intent(signal, decision)
            cli_module._append_execution_intent_csv(intent, csv_path)
            cli_module._track_confirmed_signal(db_path, signal, intent, csv_path)
            self.assertEqual(len(fetch_pending_signals(db_path)), 1)
            # Simulate process restart.
            cli_module.ACTIVE_CONFIRMED_SIGNALS.clear()
            cli_module.PENDING_SIGNALS_HYDRATED = False

            with open(csv_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["time_to_confirm_ms"], "1000")
            self.assertEqual(rows[0]["signal_duration_ms"], "")

            self._seed_books(0.6, 0.7)
            with patch.object(cli_module.time, "time", return_value=3.0):
                cli_module._backfill_execution_intent_durations(db_path, csv_path)

            with open(csv_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["confirmed_at_ts"], "2000")
            self.assertEqual(rows[0]["time_to_confirm_ms"], "1000")
            self.assertEqual(rows[0]["signal_duration_ms"], "2000")
            self.assertEqual(fetch_pending_signals(db_path), [])


if __name__ == "__main__":
    unittest.main()
