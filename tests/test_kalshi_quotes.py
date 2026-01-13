import unittest

from arbscan.config import AppConfig
from arbscan.connectors.kalshi import KalshiConnector
from arbscan.models import VenueMarketRef


def _config() -> AppConfig:
    return AppConfig(
        scan_interval_seconds=1.0,
        arb_threshold=0.01,
        dedupe_seconds=60,
        margin_step_resend=0.01,
        size_step_resend=10.0,
        min_size=1.0,
        match_threshold=0.85,
        include_review=False,
        time_window_days=7,
        min_review_score=0.5,
        polymarket_l1_private_key=None,
        polymarket_l2_api_key=None,
        polymarket_l2_api_secret=None,
        polymarket_l2_api_passphrase=None,
        kalshi_api_key=None,
        kalshi_private_key_path=None,
        slack_webhook_url=None,
        fee_polymarket=0.0,
        fee_kalshi=0.0,
        fee_limitless=0.0,
        slippage_polymarket=0.005,
        slippage_kalshi=0.005,
        slippage_limitless=0.005,
        polymarket_rest_url="https://clob.polymarket.com",
        polymarket_ws_url="wss://ws-subscriptions-clob.polymarket.com",
        polymarket_gamma_url="https://gamma-api.polymarket.com",
        kalshi_base_url="https://api.elections.kalshi.com/trade-api/v2",
        kalshi_timeout_seconds=10.0,
        kalshi_rate_limit_per_second=5.0,
        kalshi_signing_mode="none",
        limitless_adapter="stub",
        limitless_http_url=None,
        limitless_http_path_template="/markets/{market_id}/book",
        limitless_timeout_seconds=10.0,
        mock_mode=False,
        db_path=":memory:",
    )


class KalshiQuoteMathTests(unittest.TestCase):
    def test_implied_asks_from_bids(self):
        connector = KalshiConnector(_config(), lambda *_: None, [VenueMarketRef("kalshi", "TEST")])
        data = {
            "orderbook": {
                "yes": {"bids": [{"price": 55, "quantity": 10}]},
                "no": {"bids": [{"price": 40, "quantity": 12}]},
            }
        }
        quotes = connector.compute_normalized_quotes(data)
        self.assertAlmostEqual(quotes["yes_bid"], 0.55)
        self.assertAlmostEqual(quotes["no_bid"], 0.40)
        self.assertAlmostEqual(quotes["yes_ask"], 0.60)
        self.assertAlmostEqual(quotes["no_ask"], 0.45)
        self.assertEqual(quotes["yes_bid_size"], 10)
        self.assertEqual(quotes["no_bid_size"], 12)

    def test_missing_side(self):
        connector = KalshiConnector(_config(), lambda *_: None, [VenueMarketRef("kalshi", "TEST")])
        data = {"orderbook": {"yes": {"bids": []}, "no": {"bids": []}}}
        quotes = connector.compute_normalized_quotes(data)
        self.assertIsNone(quotes["yes_ask"])
        self.assertIsNone(quotes["no_ask"])


if __name__ == "__main__":
    unittest.main()
