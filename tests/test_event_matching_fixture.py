import unittest
from datetime import datetime

from arbscan.canonical import parse_rate_delta_outcome
from arbscan.event_matching import analyze_event_pair, map_outcomes
from arbscan.models import EventDomain, EventGroup, OutcomeMarket


class EventMatchingFixtureTests(unittest.TestCase):
    def test_known_event_pair_maps_outcomes(self):
        kalshi = EventGroup(
            venue="kalshi",
            event_id="KXFEDDECISION-26JAN",
            title="Fed decision in January",
            close_time=datetime(2026, 1, 26, 0, 0, 0),
            rules_text="Federal Reserve decision for January meeting.",
            event_domain=EventDomain.RATE_DELTA,
            outcomes=[
                OutcomeMarket(
                    outcome_key="NO_CHANGE",
                    display_label="No change (0 bps)",
                    market_id="KXFEDDECISION-26JAN-NOCHANGE",
                    rules_text="",
                    event_domain=EventDomain.RATE_DELTA,
                    canonical_predicate=parse_rate_delta_outcome("No change (0 bps)", ""),
                ),
                OutcomeMarket(
                    outcome_key="CUT_25",
                    display_label="Cut 25 bps",
                    market_id="KXFEDDECISION-26JAN-CUT25",
                    rules_text="",
                    event_domain=EventDomain.RATE_DELTA,
                    canonical_predicate=parse_rate_delta_outcome("Cut 25 bps", ""),
                ),
                OutcomeMarket(
                    outcome_key="HIKE_25_PLUS",
                    display_label="Hike 25+ bps",
                    market_id="KXFEDDECISION-26JAN-HIKE",
                    rules_text="",
                    event_domain=EventDomain.RATE_DELTA,
                    canonical_predicate=parse_rate_delta_outcome("Hike 25+ bps", ""),
                ),
            ],
        )
        poly = EventGroup(
            venue="polymarket",
            event_id="fed-decision-in-january",
            title="Fed decision in January",
            close_time=datetime(2026, 1, 26, 0, 0, 0),
            rules_text="Federal Reserve rate decision in January meeting.",
            event_domain=EventDomain.RATE_DELTA,
            outcomes=[
                OutcomeMarket(
                    outcome_key="NO_CHANGE",
                    display_label="No change",
                    market_id="POLY-NOCHANGE",
                    rules_text="",
                    event_domain=EventDomain.RATE_DELTA,
                    canonical_predicate=parse_rate_delta_outcome("No change", ""),
                ),
                OutcomeMarket(
                    outcome_key="CUT_25",
                    display_label="25 bps cut",
                    market_id="POLY-CUT25",
                    rules_text="",
                    event_domain=EventDomain.RATE_DELTA,
                    canonical_predicate=parse_rate_delta_outcome("25 bps cut", ""),
                ),
                OutcomeMarket(
                    outcome_key="HIKE_25_PLUS",
                    display_label="25 bps or more hike",
                    market_id="POLY-HIKE",
                    rules_text="",
                    event_domain=EventDomain.RATE_DELTA,
                    canonical_predicate=parse_rate_delta_outcome("25 bps or more hike", ""),
                ),
            ],
        )
        decision = analyze_event_pair(kalshi, poly, scorer=_DummyScorer(), time_window_days=30, min_review_score=0.5)
        self.assertIsNone(decision["rejection_reason"], msg=f"Rejected: {decision}")
        pairs = map_outcomes(kalshi, poly)
        self.assertGreaterEqual(len(pairs), 3, msg=f"Expected >=3 outcome pairs, got {pairs}")


class _DummyScorer:
    def available(self):
        return False

    def similarity(self, text_a, text_b):
        return 0.9


if __name__ == "__main__":
    unittest.main()
