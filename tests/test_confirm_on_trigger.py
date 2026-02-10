import copy
import time
import unittest
from datetime import datetime, timezone

from arbv2.pricing.arb import (
    CONFIRM_IDEMPOTENCY,
    ORDERBOOKS,
    STREAM_HEALTH,
    STREAM_STATUS,
    TRIGGER_CONFIRM_TTL_MS,
    confirm_on_trigger,
    set_stream_connected,
    set_stream_subscribed,
    touch_stream_heartbeat,
    update_orderbook,
)


class ConfirmOnTriggerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_stream_health = dict(STREAM_HEALTH)
        self._old_stream_status = copy.deepcopy(STREAM_STATUS)
        ORDERBOOKS.clear()
        CONFIRM_IDEMPOTENCY.clear()
        set_stream_connected("kalshi", True)
        set_stream_subscribed("kalshi", True)
        set_stream_connected("polymarket", True)
        set_stream_subscribed("polymarket", True)
        touch_stream_heartbeat("kalshi", ts=time.time())
        touch_stream_heartbeat("polymarket", ts=time.time())

    def tearDown(self) -> None:
        STREAM_HEALTH.clear()
        STREAM_HEALTH.update(self._old_stream_health)
        STREAM_STATUS.clear()
        STREAM_STATUS.update(self._old_stream_status)

    def _build_signal(self, detected_ts: int) -> dict:
        return {
            "size": 100,
            "detected_ts": detected_ts,
            "legs": [
                {
                    "venue": "kalshi",
                    "market_id": "K-A",
                    "outcome_label": "A",
                    "side": "YES",
                },
                {
                    "venue": "polymarket",
                    "market_id": "P-B",
                    "outcome_label": "B",
                    "side": "YES",
                },
            ],
        }

    def _seed_books(self) -> None:
        now = datetime.now(timezone.utc)
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(0.40, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(0.50, 500)], last_update_ts_utc=now)

    def test_confirm_passes_with_fresh_books(self) -> None:
        self._seed_books()
        now_ms = int(time.time() * 1000)
        signal = self._build_signal(now_ms)
        decision = confirm_on_trigger(signal)
        self.assertTrue(decision["ok"])
        self.assertGreater(decision["recalculated_edge_bps"], 0)

    def test_confirm_rejects_stream_unhealthy(self) -> None:
        self._seed_books()
        set_stream_connected("kalshi", False)
        now_ms = int(time.time() * 1000)
        signal = self._build_signal(now_ms)
        decision = confirm_on_trigger(signal)
        self.assertFalse(decision["ok"])
        self.assertEqual(decision["reason"], "stream_unhealthy")
        self.assertEqual(decision["stream_reasons"]["kalshi"], "stream_disconnected")

    def test_confirm_rejects_ttl_expired(self) -> None:
        self._seed_books()
        expired_ts = int(time.time() * 1000) - (TRIGGER_CONFIRM_TTL_MS + 1)
        signal = self._build_signal(expired_ts)
        decision = confirm_on_trigger(signal)
        self.assertFalse(decision["ok"])
        self.assertEqual(decision["reason"], "ttl_expired")

    def test_confirm_rejects_duplicate_idempotency(self) -> None:
        self._seed_books()
        now_ms = int(time.time() * 1000)
        signal = self._build_signal(now_ms)
        decision = confirm_on_trigger(signal)
        self.assertTrue(decision["ok"])
        decision_again = confirm_on_trigger(signal)
        self.assertFalse(decision_again["ok"])
        self.assertEqual(decision_again["reason"], "duplicate")


if __name__ == "__main__":
    unittest.main()
