import unittest
from unittest.mock import patch

from arbv2.cli import _scan_arb_events
from arbv2.pricing.arb import ArbMode, EventMarkets


class CliArbScanFilterTests(unittest.TestCase):
    def _event(self) -> EventMarkets:
        return EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
            event_id="E1",
        )

    def _metrics(self, kalshi_id: str, poly_id: str) -> dict:
        return {
            "arb_type": ArbMode.EVENT_OUTCOME.value,
            "profit": -0.01,
            "ts_utc": "2026-02-12T00:00:00+00:00",
            "event_id": "E1",
            "size": 1.0,
            "legs": [
                {"venue": "kalshi", "market_id": kalshi_id},
                {"venue": "polymarket", "market_id": poly_id},
            ],
        }

    @patch("arbv2.cli._backfill_execution_intent_durations")
    @patch("arbv2.cli.find_best_arb", return_value=None)
    @patch("arbv2.cli.insert_arb_scans")
    @patch("arbv2.cli.evaluate_paired_metrics")
    def test_unmatched_scan_pair_is_filtered(
        self,
        mock_metrics,
        mock_insert,
        _mock_find_best,
        _mock_backfill,
    ) -> None:
        mock_metrics.return_value = self._metrics("K-X", "P-X")
        _scan_arb_events(
            "arbv2.db",
            [self._event()],
            ArbMode.EVENT_OUTCOME,
            {},
            matched_pairs={("K-A", "P-A")},
        )
        mock_insert.assert_not_called()

    @patch("arbv2.cli._backfill_execution_intent_durations")
    @patch("arbv2.cli.find_best_arb", return_value=None)
    @patch("arbv2.cli.insert_arb_scans")
    @patch("arbv2.cli.evaluate_paired_metrics")
    def test_matched_scan_pair_is_written(
        self,
        mock_metrics,
        mock_insert,
        _mock_find_best,
        _mock_backfill,
    ) -> None:
        mock_metrics.return_value = self._metrics("K-A", "P-A")
        _scan_arb_events(
            "arbv2.db",
            [self._event()],
            ArbMode.EVENT_OUTCOME,
            {},
            matched_pairs={("K-A", "P-A")},
        )
        mock_insert.assert_called_once()


if __name__ == "__main__":
    unittest.main()
