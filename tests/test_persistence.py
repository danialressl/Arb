import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from arbv2.persistence import ACTIVE_RUNS, RUNS_BY_EVENT, mark_execution_intent, observe_event
from arbv2.pricing.arb import ArbMode, EventMarkets
import arbv2.persistence as persistence_module


class PersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        ACTIVE_RUNS.clear()
        RUNS_BY_EVENT.clear()

    def _signal(self, *, detected_ts: int, legs_order: str = "kp") -> dict:
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
            "size": 10.0,
            "profit": 0.1,
            "detected_ts": detected_ts,
            "legs": legs,
        }

    def _event(self) -> EventMarkets:
        return EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
            event_id="EVT-1",
        )

    def test_execution_intent_run_ends_on_zero_edge(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        detected_ts = int(now.timestamp() * 1000)
        signal = self._signal(detected_ts=detected_ts)
        mark_execution_intent(signal, confirmed_at_ts=detected_ts + 250)
        event = self._event()
        with patch.object(persistence_module, "_compute_edge_for_run", side_effect=[(50.0, 1.0), (10.0, 1.0), (0.0, 0.0)]):
            observe_event(event, ArbMode.EVENT_OUTCOME, now_utc=now)
            observe_event(event, ArbMode.EVENT_OUTCOME, now_utc=now + timedelta(milliseconds=500))
            results = observe_event(event, ArbMode.EVENT_OUTCOME, now_utc=now + timedelta(milliseconds=1000))
        self.assertEqual(len(ACTIVE_RUNS), 0)
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEqual(result.get("event_id"), "EVT-1")
        self.assertEqual(result.get("confirmed_at_ts"), detected_ts + 250)
        expected_duration = int((now + timedelta(milliseconds=1000)).timestamp() * 1000) - detected_ts
        self.assertEqual(result.get("duration_ms"), expected_duration)

    def test_fingerprint_stable_with_leg_order(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        detected_ts = int(now.timestamp() * 1000)
        mark_execution_intent(self._signal(detected_ts=detected_ts, legs_order="kp"), confirmed_at_ts=detected_ts + 1)
        mark_execution_intent(self._signal(detected_ts=detected_ts, legs_order="pk"), confirmed_at_ts=detected_ts + 1)
        self.assertEqual(len(ACTIVE_RUNS), 1)

    def test_missed_update_timeout_drops_run(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        detected_ts = int(now.timestamp() * 1000)
        signal = self._signal(detected_ts=detected_ts)
        mark_execution_intent(signal, confirmed_at_ts=detected_ts + 1)
        event = self._event()
        with patch.dict(os.environ, {"ARBV2_PERSISTENCE_MISS_TICKS": "2"}):
            with patch.object(persistence_module, "_compute_edge_for_run", return_value=None):
                observe_event(event, ArbMode.EVENT_OUTCOME, now_utc=now)
                observe_event(event, ArbMode.EVENT_OUTCOME, now_utc=now + timedelta(milliseconds=500))
        self.assertEqual(len(ACTIVE_RUNS), 0)


if __name__ == "__main__":
    unittest.main()
