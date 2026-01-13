import unittest
from datetime import datetime, timedelta

from arbscan.dedupe import DedupeCache
from arbscan.models import ArbSignal, VenueMarketRef


class DedupeTests(unittest.TestCase):
    def test_dedupe_resend_on_margin_step(self):
        cache = DedupeCache(dedupe_seconds=60, margin_step_resend=0.02, size_step_resend=10)
        now = datetime.utcnow()
        signal = ArbSignal(
            canonical_event_id="event-1",
            yes_ref=VenueMarketRef("polymarket", "poly-1"),
            no_ref=VenueMarketRef("kalshi", "kalshi-1"),
            yes_price=0.5,
            no_price=0.49,
            yes_size=100,
            no_size=100,
            executable_size=100,
            margin_abs=0.02,
            margin_pct=0.02,
            total_cost=0.98,
            fees={},
            slippage={},
            timestamp=now,
            alert_id="a",
            outcome_mapping={"polymarket": "YES", "kalshi": "NO"},
        )
        self.assertTrue(cache.should_send(signal, now))
        self.assertFalse(cache.should_send(signal, now + timedelta(seconds=10)))

        improved = ArbSignal(
            **{
                **signal.__dict__,
                "margin_abs": 0.05,
                "alert_id": "b",
                "total_cost": 0.95,
            }
        )
        self.assertTrue(cache.should_send(improved, now + timedelta(seconds=10)))

        size_improved = ArbSignal(
            **{
                **signal.__dict__,
                "executable_size": 120,
                "alert_id": "c",
            }
        )
        self.assertTrue(cache.should_send(size_improved, now + timedelta(seconds=10)))


if __name__ == "__main__":
    unittest.main()
