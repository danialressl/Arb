import unittest
from datetime import datetime, timedelta, timezone

from arbv2.pricing.arb import (
    ArbMode,
    MAX_STALENESS_SECONDS,
    ORDERBOOKS,
    EventMarkets,
    find_best_arb,
    get_fill_stats,
    update_orderbook,
    evaluate_arb_at_size,
    _kalshi_fee_total,
)


class ArbV2ArbitrageTests(unittest.TestCase):
    def setUp(self) -> None:
        ORDERBOOKS.clear()

    def test_fill_stats_vwap_and_worst(self) -> None:
        now = datetime.now(timezone.utc)
        update_orderbook(
            "kalshi",
            "K1",
            "YES",
            bids=[],
            asks=[(0.40, 5), (0.45, 5)],
            last_update_ts_utc=now,
        )
        stats = get_fill_stats("kalshi", "K1", "YES", "YES", 8)
        self.assertTrue(stats["ok"])
        self.assertAlmostEqual(stats["vwap_price"], (0.40 * 5 + 0.45 * 3) / 8, places=6)
        self.assertAlmostEqual(stats["worst_price"], 0.45, places=6)

    def test_fill_stats_insufficient_depth(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        update_orderbook("kalshi", "K2", "YES", bids=[], asks=[(0.4, 3)], last_update_ts_utc=now)
        stats = get_fill_stats("kalshi", "K2", "YES", "YES", 10)
        self.assertFalse(stats["ok"])

    def test_stale_book_rejected_when_enabled(self) -> None:
        if MAX_STALENESS_SECONDS <= 0:
            self.skipTest("Staleness guard disabled")
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        stale = now - timedelta(seconds=MAX_STALENESS_SECONDS + 1)
        for venue in ("kalshi", "polymarket"):
            update_orderbook(venue, f"{venue}-A", "A", bids=[], asks=[(0.4, 200)], last_update_ts_utc=stale)
            update_orderbook(venue, f"{venue}-B", "B", bids=[], asks=[(0.55, 200)], last_update_ts_utc=stale)
        event = EventMarkets(
            kalshi_by_outcome={"A": "kalshi-A", "B": "kalshi-B"},
            polymarket_by_outcome={"A": "polymarket-A", "B": "polymarket-B"},
            outcomes=["A", "B"],
        )
        self.assertIsNone(evaluate_arb_at_size(event, ArbMode.EVENT_OUTCOME, 200, now_utc=now))

    def test_event_outcome_arb(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(0.40, 500)], last_update_ts_utc=now)
        update_orderbook("kalshi", "K-B", "B", bids=[], asks=[(0.60, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-A", "A", bids=[], asks=[(0.45, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(0.55, 500)], last_update_ts_utc=now)
        event = EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
        )
        result = evaluate_arb_at_size(event, ArbMode.EVENT_OUTCOME, 200, now_utc=now)
        self.assertIsNotNone(result)
        self.assertGreater(result["profit"], 0)

    def test_binary_mirror_arb(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        update_orderbook("kalshi", "K-YES", "YES", bids=[], asks=[(0.42, 500)], last_update_ts_utc=now)
        update_orderbook("kalshi", "K-NO", "NO", bids=[], asks=[(0.60, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-YES", "YES", bids=[], asks=[(0.44, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-NO", "NO", bids=[], asks=[(0.54, 500)], last_update_ts_utc=now)
        event = EventMarkets(
            kalshi_by_outcome={"YES": "K-YES", "NO": "K-NO"},
            polymarket_by_outcome={"YES": "P-YES", "NO": "P-NO"},
            outcomes=["YES", "NO"],
        )
        result = evaluate_arb_at_size(event, ArbMode.BINARY_MIRROR, 200, now_utc=now)
        self.assertIsNotNone(result)
        self.assertGreater(result["profit"], 0)

    def test_find_best_ladder(self) -> None:
        now = datetime.now(timezone.utc)
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(0.40, 500), (0.60, 500)], last_update_ts_utc=now)
        update_orderbook("kalshi", "K-B", "B", bids=[], asks=[(0.60, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-A", "A", bids=[], asks=[(0.45, 500)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(0.55, 500), (0.70, 500)], last_update_ts_utc=now)
        event = EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
        )
        result = find_best_arb(event, ArbMode.EVENT_OUTCOME, Q_min=200, Q_max=800)
        self.assertIsNotNone(result)
        self.assertEqual(result["size"], 500)

    def test_kalshi_fee_rounds_up_cent(self) -> None:
        # fee = ceil(0.07 * C * P * (1 - P)) to cents
        fee = _kalshi_fee_total(100, 0.50)
        self.assertAlmostEqual(fee, 1.76, places=6)

    def test_paired_sizing_stops_on_marginal_cost(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(0.40, 100), (0.90, 100)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(0.50, 200)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-A", "A", bids=[], asks=[(0.99, 1)], last_update_ts_utc=now)
        update_orderbook("kalshi", "K-B", "B", bids=[], asks=[(0.99, 1)], last_update_ts_utc=now)
        event = EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
        )
        result = find_best_arb(event, ArbMode.EVENT_OUTCOME, Q_min=1, Q_max=1000)
        self.assertIsNotNone(result)
        self.assertEqual(result["size"], 100)

    def test_paired_sizing_uses_deeper_levels(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        update_orderbook("kalshi", "K-A", "A", bids=[], asks=[(0.30, 150), (0.40, 150)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-B", "B", bids=[], asks=[(0.50, 300)], last_update_ts_utc=now)
        update_orderbook("polymarket", "P-A", "A", bids=[], asks=[(0.99, 1)], last_update_ts_utc=now)
        update_orderbook("kalshi", "K-B", "B", bids=[], asks=[(0.99, 1)], last_update_ts_utc=now)
        event = EventMarkets(
            kalshi_by_outcome={"A": "K-A", "B": "K-B"},
            polymarket_by_outcome={"A": "P-A", "B": "P-B"},
            outcomes=["A", "B"],
        )
        result = find_best_arb(event, ArbMode.EVENT_OUTCOME, Q_min=1, Q_max=1000)
        self.assertIsNotNone(result)
        self.assertEqual(result["size"], 300)


if __name__ == "__main__":
    unittest.main()
