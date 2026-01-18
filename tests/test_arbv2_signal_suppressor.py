import unittest
from arbv2.pricing.arb import ArbMode, EventMarkets, SignalSuppressor, signal_fingerprint


class SignalSuppressorTests(unittest.TestCase):
    def test_cooldown_suppresses_same_key(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=10, hysteresis_bps=200, event_lock_seconds=0, improvement_bps=0)
        key = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("A", "B"), "KALSHI:A+POLY:B")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=100, size=3200, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=250, size=3200, now_ts=5
        )
        self.assertFalse(ok)

    def test_hysteresis_allows_improvement(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=10, hysteresis_bps=200, event_lock_seconds=0, improvement_bps=0)
        key = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("A", "B"), "KALSHI:A+POLY:B")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=100, size=3200, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=350, size=3200, now_ts=5
        )
        self.assertTrue(ok)

    def test_cooldown_expires(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=5, hysteresis_bps=200, event_lock_seconds=0, improvement_bps=0)
        key = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("A", "B"), "KALSHI:A+POLY:B")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=100, size=3200, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=120, size=3200, now_ts=6
        )
        self.assertTrue(ok)

    def test_event_lock_blocks_other_outcomes(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=0, hysteresis_bps=0, event_lock_seconds=20, improvement_bps=300)
        key1 = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("A", "B"), "KALSHI:A+POLY:B")
        key2 = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("B", "A"), "POLY:A+KALSHI:B")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key1, event_key="E1", edge_bps=100, size=3200, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key2, event_key="E1", edge_bps=200, size=3200, now_ts=5
        )
        self.assertFalse(ok)

    def test_event_lock_allows_improvement(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=0, hysteresis_bps=0, event_lock_seconds=20, improvement_bps=300)
        key1 = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("A", "B"), "KALSHI:A+POLY:B")
        key2 = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("B", "A"), "POLY:A+KALSHI:B")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key1, event_key="E1", edge_bps=100, size=3200, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key2, event_key="E1", edge_bps=450, size=3200, now_ts=5
        )
        self.assertTrue(ok)

    def test_binary_mirror_uses_event_lock(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=0, hysteresis_bps=0, event_lock_seconds=20, improvement_bps=300)
        key1 = ("BINARY_MIRROR", "E1", "OUTCOME_A", "POLY:YES+KALSHI:NO")
        key2 = ("BINARY_MIRROR", "E1", "OUTCOME_B", "POLY:YES+KALSHI:NO")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.BINARY_MIRROR, key=key1, event_key="E1", edge_bps=100, size=100, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.BINARY_MIRROR, key=key2, event_key="E1", edge_bps=200, size=100, now_ts=5
        )
        self.assertFalse(ok)

    def test_smaller_size_same_edge_is_suppressed(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=10, hysteresis_bps=200, event_lock_seconds=0, improvement_bps=0)
        key = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("A", "B"), "KALSHI:A+POLY:B")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=300, size=3200, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=300, size=1600, now_ts=5
        )
        self.assertFalse(ok)

    def test_smaller_size_with_hysteresis_improvement_is_allowed(self) -> None:
        sup = SignalSuppressor(cooldown_seconds=10, hysteresis_bps=200, event_lock_seconds=0, improvement_bps=0)
        key = ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("A", "B"), "KALSHI:A+POLY:B")
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=100, size=3200, now_ts=0
        )
        self.assertTrue(ok)
        ok, _ = sup.should_emit(
            arb_type=ArbMode.EVENT_OUTCOME, key=key, event_key="E1", edge_bps=350, size=1600, now_ts=5
        )
        self.assertTrue(ok)

    def test_fingerprint_stable_across_price_noise(self) -> None:
        event = EventMarkets(
            kalshi_by_outcome={},
            polymarket_by_outcome={},
            outcomes=["ALPHA", "BETA"],
            event_id="E1",
        )
        signal_a = {
            "direction": "KALSHI:A + POLY:B",
            "size": 3200,
            "profit": 10.0,
            "legs": [{"eff_vwap": 0.45}, {"eff_vwap": 0.44}],
        }
        signal_b = {
            "direction": "KALSHI:A + POLY:B",
            "size": 800,
            "profit": 12.0,
            "legs": [{"eff_vwap": 0.42}, {"eff_vwap": 0.47}],
        }
        key_a, _ = signal_fingerprint(event, ArbMode.EVENT_OUTCOME, signal_a)
        key_b, _ = signal_fingerprint(event, ArbMode.EVENT_OUTCOME, signal_b)
        self.assertEqual(key_a, key_b)
        self.assertEqual(
            key_a,
            ("EVENT_OUTCOME", "E1", ("kalshi", "polymarket"), ("ALPHA", "BETA"), "KALSHI:A+POLY:B"),
        )


if __name__ == "__main__":
    unittest.main()
