import unittest
from datetime import datetime

from arbscan.arb import compute_arb_signals
from arbscan.models import MarketQuote, MappedEvent, VenueMarketRef


class ArbMathTests(unittest.TestCase):
    def test_margin_with_fees_and_slippage(self):
        event = MappedEvent(
            canonical_event_id="event-1",
            refs=[
                VenueMarketRef(venue="polymarket", market_id="poly-1"),
                VenueMarketRef(venue="kalshi", market_id="kalshi-1"),
            ],
        )
        quotes = {
            ("polymarket", "poly-1", "YES"): MarketQuote(
                best_bid=0.48,
                best_ask=0.5,
                bid_size=100,
                ask_size=100,
                timestamp=datetime.utcnow(),
            ),
            ("kalshi", "kalshi-1", "NO"): MarketQuote(
                best_bid=0.48,
                best_ask=0.49,
                bid_size=100,
                ask_size=100,
                timestamp=datetime.utcnow(),
            ),
        }
        fees = {"polymarket": 0.02, "kalshi": 0.01}
        slippage = {"polymarket": 0.005, "kalshi": 0.005}
        signals = compute_arb_signals(
            event=event,
            quotes=quotes,
            fees=fees,
            slippages=slippage,
            threshold=0.0,
            now=datetime.utcnow(),
        )
        self.assertEqual(len(signals), 1)
        signal = signals[0]
        expected_total = 0.5 + 0.49 + 0.02 + 0.01 + 0.005 + 0.005
        expected_margin = 1.0 - expected_total
        self.assertAlmostEqual(signal.margin_abs, expected_margin, places=6)
        self.assertEqual(signal.executable_size, 100)


if __name__ == "__main__":
    unittest.main()
