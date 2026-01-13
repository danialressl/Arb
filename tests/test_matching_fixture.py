import unittest
from datetime import datetime

from arbscan.matching import EmbeddingScorer, evaluate_pair
from arbscan.models import ResolutionSpec


class MatchingFixtureTests(unittest.TestCase):
    def test_known_equivalent_pair_is_review_or_match(self):
        kalshi = ResolutionSpec(
            venue="kalshi",
            market_id="TEST-KALSHI",
            title="Will CPI be above 3% in June 2025?",
            close_time=datetime(2025, 6, 30, 0, 0, 0),
            end_time=datetime(2025, 6, 30, 0, 0, 0),
            resolution_source="BLS",
            rules_text="CPI above 3 percent in June 2025 per BLS.",
            is_binary=True,
            event_type="macro",
            entities=["CPI", "BLS"],
            predicate="above",
            threshold=3.0,
            threshold_units="percent",
            measurement_time="2025-06",
            timezone="UTC",
            void_conditions=[],
        )
        polymarket = ResolutionSpec(
            venue="polymarket",
            market_id="TEST-POLY",
            title="CPI above 3% in June 2025",
            close_time=datetime(2025, 6, 30, 0, 0, 0),
            end_time=datetime(2025, 6, 30, 0, 0, 0),
            resolution_source="BLS",
            rules_text="Market resolves to YES if CPI exceeds 3% in June 2025.",
            is_binary=True,
            event_type="macro",
            entities=["CPI", "BLS"],
            predicate="above",
            threshold=3.0,
            threshold_units="percent",
            measurement_time="2025-06",
            timezone="UTC",
            void_conditions=[],
        )
        decision = evaluate_pair(
            kalshi,
            polymarket,
            EmbeddingScorer(),
            time_window_days=30,
            min_review_score=0.5,
        )
        self.assertIsNone(decision["rejection_reason"], msg=f"Rejected: {decision}")


if __name__ == "__main__":
    unittest.main()
