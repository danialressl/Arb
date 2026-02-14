import unittest
from unittest.mock import patch

from arbv2.cli import _scan_arb_events
from arbv2.pricing.arb import ArbMode, EventMarkets


class CliOrderExecutionTests(unittest.TestCase):
    def _event(self) -> EventMarkets:
        return EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
            event_id="E1",
        )

    def _signal(self) -> dict:
        return {
            "event_id": "E1",
            "arb_type": ArbMode.EVENT_OUTCOME.value,
            "direction": "KALSHI:A + POLY:B",
            "size": 100.0,
            "profit": 1.0,
            "roi": 0.01,
            "legs": [
                {
                    "venue": "kalshi",
                    "market_id": "K-A",
                    "outcome_label": "A",
                    "side": "YES",
                    "raw_vwap": 0.4,
                    "eff_vwap": 0.41,
                    "worst_price": 0.41,
                },
                {
                    "venue": "polymarket",
                    "market_id": "P-B",
                    "outcome_label": "B",
                    "side": "YES",
                    "raw_vwap": 0.5,
                    "eff_vwap": 0.51,
                    "worst_price": 0.51,
                },
            ],
            "ts_utc": "2026-02-14T00:00:00+00:00",
        }

    def _decision(self) -> dict:
        return {
            "ok": True,
            "recalculated_edge_bps": 120.0,
            "edge_bps": 120.0,
            "expected_pnl_usd": 1.2,
            "limit_prices": [0.41, 0.51],
        }

    def _intent(self) -> dict:
        return {
            "intent_id": "intent-1",
            "type": "EXECUTION_INTENT",
            "event_id": "E1",
            "venues": ["kalshi", "polymarket"],
            "markets": ["K-A", "P-B"],
            "sides": ["YES", "YES"],
            "limit_prices": [0.41, 0.51],
            "size_usd": 100.0,
            "edge_bps": 120.0,
            "expected_pnl_usd": 1.2,
            "detected_ts": 1000,
            "confirmed_at_ts": 1200,
            "time_to_confirm_ms": 200,
            "signal_duration_ms": None,
        }

    @patch("arbv2.cli._backfill_execution_intent_durations")
    @patch("arbv2.cli.evaluate_paired_metrics", return_value=None)
    @patch("arbv2.cli.find_best_arb")
    @patch("arbv2.cli.should_emit_signal", return_value=True)
    @patch("arbv2.cli.confirm_on_trigger")
    @patch("arbv2.cli.post_confirm_decision")
    @patch("arbv2.cli._build_execution_intent")
    @patch("arbv2.cli._append_execution_intent_csv")
    @patch("arbv2.cli._track_confirmed_signal")
    @patch("arbv2.cli._log_signal_details")
    @patch("arbv2.cli.execute_confirmed_signal")
    @patch("arbv2.cli._append_order_execution_csv")
    def test_orders_not_sent_when_execution_disabled(
        self,
        mock_append_order_csv,
        mock_execute_orders,
        _mock_log_signal,
        _mock_track_signal,
        _mock_append_intent_csv,
        mock_build_intent,
        mock_post_confirm,
        mock_confirm,
        _mock_emit,
        mock_find_best,
        _mock_metrics,
        _mock_backfill,
    ) -> None:
        mock_find_best.return_value = self._signal()
        mock_confirm.return_value = self._decision()
        mock_post_confirm.return_value = dict(self._decision(), ok=True, reason="confirmed")
        mock_build_intent.return_value = self._intent()

        _scan_arb_events(
            "arbv2.db",
            [self._event()],
            ArbMode.EVENT_OUTCOME,
            {},
            config=object(),
            execute_orders=False,
            max_order_usd=5.0,
        )

        mock_execute_orders.assert_not_called()
        mock_append_order_csv.assert_not_called()

    @patch("arbv2.cli._backfill_execution_intent_durations")
    @patch("arbv2.cli.evaluate_paired_metrics", return_value=None)
    @patch("arbv2.cli.find_best_arb")
    @patch("arbv2.cli.should_emit_signal", return_value=True)
    @patch("arbv2.cli.confirm_on_trigger")
    @patch("arbv2.cli.post_confirm_decision")
    @patch("arbv2.cli._build_execution_intent")
    @patch("arbv2.cli._append_execution_intent_csv")
    @patch("arbv2.cli._track_confirmed_signal")
    @patch("arbv2.cli._log_signal_details")
    @patch("arbv2.cli.execute_confirmed_signal")
    @patch("arbv2.cli._append_order_execution_csv")
    def test_orders_sent_when_execution_enabled(
        self,
        mock_append_order_csv,
        mock_execute_orders,
        _mock_log_signal,
        _mock_track_signal,
        _mock_append_intent_csv,
        mock_build_intent,
        mock_post_confirm,
        mock_confirm,
        _mock_emit,
        mock_find_best,
        _mock_metrics,
        _mock_backfill,
    ) -> None:
        mock_find_best.return_value = self._signal()
        mock_confirm.return_value = self._decision()
        mock_post_confirm.return_value = dict(self._decision(), ok=True, reason="confirmed")
        mock_build_intent.return_value = self._intent()
        mock_execute_orders.return_value = {
            "intent_id": "intent-1",
            "status": "confirmed",
            "order_confirm_ms": 25,
            "contracts": 5,
            "actual_spend_usd": 4.6,
        }

        _scan_arb_events(
            "arbv2.db",
            [self._event()],
            ArbMode.EVENT_OUTCOME,
            {},
            config=object(),
            execute_orders=True,
            max_order_usd=3.5,
        )

        mock_execute_orders.assert_called_once()
        self.assertEqual(mock_execute_orders.call_args.kwargs["max_order_usd"], 3.5)
        mock_append_order_csv.assert_called_once()


if __name__ == "__main__":
    unittest.main()
