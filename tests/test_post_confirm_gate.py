import unittest

from arbv2.pricing import arb as arb_module
from arbv2.pricing.arb import ArbMode, EventMarkets, post_confirm_decision


class PostConfirmGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_enabled = arb_module.POST_CONFIRM_ENABLED
        self._old_window = arb_module.POST_CONFIRM_WINDOW_MS
        self._old_decay = arb_module.POST_CONFIRM_MAX_EDGE_DECAY_BPS
        arb_module.POST_CONFIRM_ENABLED = True
        arb_module.POST_CONFIRM_WINDOW_MS = 400
        arb_module.POST_CONFIRM_MAX_EDGE_DECAY_BPS = 30
        arb_module.POST_CONFIRM_CACHE.clear()

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
            "legs": [],
        }

    def test_second_confirm_emits(self) -> None:
        event = self._event()
        signal = self._signal("KALSHI:A + POLY:B")
        decision = {"recalculated_edge_bps": 100.0}
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=1000)
        self.assertFalse(result["ok"])
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=1200)
        self.assertTrue(result["ok"])

    def test_second_confirm_outside_window(self) -> None:
        event = self._event()
        signal = self._signal("KALSHI:A + POLY:B")
        decision = {"recalculated_edge_bps": 100.0}
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=1000)
        self.assertFalse(result["ok"])
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, decision, now_ms=2000)
        self.assertFalse(result["ok"])

    def test_edge_decay_rejects(self) -> None:
        event = self._event()
        signal = self._signal("KALSHI:A + POLY:B")
        first = {"recalculated_edge_bps": 100.0}
        second = {"recalculated_edge_bps": 50.0}
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, first, now_ms=1000)
        self.assertFalse(result["ok"])
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal, second, now_ms=1100)
        self.assertFalse(result["ok"])

    def test_different_fingerprint_is_independent(self) -> None:
        event = self._event()
        signal_a = self._signal("KALSHI:A + POLY:B")
        signal_b = self._signal("POLY:A + KALSHI:B")
        decision = {"recalculated_edge_bps": 100.0}
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal_a, decision, now_ms=1000)
        self.assertFalse(result["ok"])
        result = post_confirm_decision(event, ArbMode.EVENT_OUTCOME, signal_b, decision, now_ms=1100)
        self.assertFalse(result["ok"])
