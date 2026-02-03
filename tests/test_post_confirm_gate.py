import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from arbv2.pricing import arb as arb_module
from arbv2.pricing.arb import ArbMode, EventMarkets, ORDERBOOKS, post_confirm_decision, update_orderbook


class PostConfirmGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_enabled = arb_module.POST_CONFIRM_ENABLED
        self._old_window = arb_module.POST_CONFIRM_WINDOW_MS
        self._old_decay = arb_module.POST_CONFIRM_MAX_EDGE_DECAY_BPS
        arb_module.POST_CONFIRM_ENABLED = True
        arb_module.POST_CONFIRM_WINDOW_MS = 400
        arb_module.POST_CONFIRM_MAX_EDGE_DECAY_BPS = 30
        arb_module.POST_CONFIRM_CACHE.clear()
        ORDERBOOKS.clear()

    def tearDown(self) -> None:
        arb_module.POST_CONFIRM_ENABLED = self._old_enabled
        arb_module.POST_CONFIRM_WINDOW_MS = self._old_window
        arb_module.POST_CONFIRM_MAX_EDGE_DECAY_BPS = self._old_decay
        arb_module.POST_CONFIRM_CACHE.clear()

    def _event(self) -> EventMarkets:
        return EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
            event_id="E1",
        )

    def _signal(self, direction: str) -> dict:
        return {
            "direction": direction,
            "size": 100.0,
            "profit": 1.0,
            "legs": [
                {"venue": "kalshi", "market_id": "K-A", "outcome_label": "A", "side": "YES"},
                {"venue": "polymarket", "market_id": "P-B", "outcome_label": "B", "side": "YES"},
            ],
        }

    def _seed_books(self, now: datetime) -> None:
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(0.4, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(0.5, 500)], last_update_ts_utc=now)

    def test_second_confirm_emits(self) -> None:
        event = self._event()
        signal = self._signal("KALSHI:A + POLY:B")
        decision = {"recalculated_edge_bps": 100.0}
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with patch.object(arb_module.time, "time", return_value=2.0):
            self._seed_books(now)
        signal["detected_ts"] = 1000
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=2000)
        self.assertFalse(result["ok"])
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=2200)
        self.assertTrue(result["ok"])

    def test_second_confirm_outside_window(self) -> None:
        event = self._event()
        signal = self._signal("KALSHI:A + POLY:B")
        decision = {"recalculated_edge_bps": 100.0}
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with patch.object(arb_module.time, "time", return_value=2.0):
            self._seed_books(now)
        signal["detected_ts"] = 1000
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=1000)
        self.assertFalse(result["ok"])
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=2000)
        self.assertFalse(result["ok"])

    def test_edge_decay_rejects(self) -> None:
        event = self._event()
        signal = self._signal("KALSHI:A + POLY:B")
        first = {"recalculated_edge_bps": 100.0}
        second = {"recalculated_edge_bps": 50.0}
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with patch.object(arb_module.time, "time", return_value=2.0):
            self._seed_books(now)
        signal["detected_ts"] = 1000
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, first, now_ms=1000)
        self.assertFalse(result["ok"])
        # Worsen the books to force edge decay on the second confirm.
        worse = datetime(2026, 1, 1, tzinfo=timezone.utc)
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(0.6, 500)], last_update_ts_utc=worse)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(0.7, 500)], last_update_ts_utc=worse)
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, second, now_ms=1100)
        self.assertFalse(result["ok"])

    def test_different_fingerprint_is_independent(self) -> None:
        event = self._event()
        signal_a = self._signal("KALSHI:A + POLY:B")
        signal_b = self._signal("POLY:A + KALSHI:B")
        decision = {"recalculated_edge_bps": 100.0}
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with patch.object(arb_module.time, "time", return_value=2.0):
            self._seed_books(now)
        signal_a["detected_ts"] = 1000
        signal_b["detected_ts"] = 1000
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal_a, decision, now_ms=1000)
        self.assertFalse(result["ok"])
        with patch.object(arb_module.time, "time", return_value=2.0):
            result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal_b, decision, now_ms=1100)
        self.assertFalse(result["ok"])
