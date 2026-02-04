import time
import unittest
from unittest.mock import patch
from datetime import datetime, timezone

from arbv2.pricing import arb as arb_module
from arbv2.pricing.arb import (
    CONFIRM_IDEMPOTENCY,
    KALSHI_MAX_AGE_SECONDS,
    ORDERBOOKS,
    POLY_MAX_AGE_SECONDS,
    TRIGGER_CONFIRM_TTL_MS,
    confirm_on_trigger,
    update_orderbook,
)


class ConfirmOnTriggerTests(unittest.TestCase):
    def setUp(self) -> None:
        ORDERBOOKS.clear()
        CONFIRM_IDEMPOTENCY.clear()

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

    def test_confirm_rejects_stale_books(self) -> None:
        self._seed_books()
        now = time.time()
        with patch.object(arb_module, "POLY_MAX_AGE_SECONDS", 1.0), patch.object(
            arb_module, "KALSHI_MAX_AGE_SECONDS", 1.0
        ):
            for (venue, _, _), book in list(ORDERBOOKS.items()):
                if venue == "polymarket":
                    book["fetched_at_ts"] = now - (arb_module.POLY_MAX_AGE_SECONDS + 1)
                else:
                    book["fetched_at_ts"] = now - (arb_module.KALSHI_MAX_AGE_SECONDS + 1)
            now_ms = int(time.time() * 1000)
            signal = self._build_signal(now_ms)
            decision = confirm_on_trigger(signal)
            self.assertFalse(decision["ok"])
            self.assertEqual(decision["reason"], "stale_book")

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
