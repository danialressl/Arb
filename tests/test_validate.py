import tempfile
import unittest
from unittest.mock import patch

from arbscan.config import AppConfig
from arbscan.validate import validate_mappings


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


class ValidateMappingsTests(unittest.TestCase):
    @patch("arbscan.validate.get_json")
    def test_kalshi_404(self, mock_get):
        mock_get.return_value = ({}, 404)
        config = _config()
        content = """
        events:
          - canonical_event_id: "event-1"
            venues:
              kalshi:
                ticker: "BAD-TICKER"
              polymarket:
                market_id: "1"
                yes_token_id: "y"
                no_token_id: "n"
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as handle:
            handle.write(content)
            path = handle.name
        errors = validate_mappings(config, path)
        self.assertTrue(any("Kalshi ticker not found" in e.message for e in errors))

    @patch("arbscan.validate.get_json")
    def test_polymarket_missing_tokens(self, mock_get):
        def _side_effect(url, timeout=10):
            return {"tokens": [{"token_id": "y", "outcome": "YES"}]}, 200

        mock_get.side_effect = _side_effect
        config = _config()
        content = """
        events:
          - canonical_event_id: "event-1"
            venues:
              polymarket:
                market_id: "1"
                yes_token_id: "y"
                no_token_id: "missing"
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as handle:
            handle.write(content)
            path = handle.name
        errors = validate_mappings(config, path)
        self.assertTrue(any("no_token_id" in e.message for e in errors))


if __name__ == "__main__":
    unittest.main()
