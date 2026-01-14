import json
import unittest
from pathlib import Path

from arbv2.match.sports_equiv import predicates_equivalent
from arbv2.models import Market
from arbv2.parse.sports import parse_sports_predicate


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


class FixtureTests(unittest.TestCase):
    def test_kalshi_polymarket_match(self) -> None:
        kalshi_raw = json.loads((FIXTURES / "kalshi_sample.json").read_text())
        poly_raw = json.loads((FIXTURES / "polymarket_sample.json").read_text())

        kalshi_market = Market(
            venue="kalshi",
            market_id=kalshi_raw["market"]["ticker"],
            event_id=kalshi_raw["event"]["event_ticker"],
            title=kalshi_raw["market"]["title"],
            event_title=kalshi_raw["event"]["title"],
            raw_json=kalshi_raw["market"],
        )
        poly_market = Market(
            venue="polymarket",
            market_id=poly_raw["market"]["id"],
            event_id=poly_raw["event"]["id"],
            title=poly_raw["market"]["question"],
            event_title=poly_raw["event"]["title"],
            raw_json=poly_raw["market"],
        )

        kalshi_pred = parse_sports_predicate(kalshi_market)
        poly_pred = parse_sports_predicate(poly_market)

        self.assertIsNotNone(kalshi_pred)
        self.assertIsNotNone(poly_pred)
        result = predicates_equivalent(kalshi_pred, poly_pred, max_hours=24)
        self.assertTrue(result.equivalent, msg=result.reason)


if __name__ == "__main__":
    unittest.main()
