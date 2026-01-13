from __future__ import annotations

import csv
import json
import logging
from datetime import datetime
from typing import Optional

from arbscan.config import AppConfig
from arbscan.canonical import predicate_from_dict
from arbscan.event_matching import EmbeddingScorer, analyze_event_pair, extract_event_features, map_outcomes
from arbscan.models import EventGroup, OutcomeMarket
from arbscan.storage import (
    get_event_group,
    get_outcome_by_market,
    get_outcomes_for_event,
    list_event_matches,
    list_outcome_matches,
)

logger = logging.getLogger(__name__)


def export_review(config: AppConfig, min_score: float, out_path: str) -> None:
    matches = list_event_matches(config.db_path, statuses=["REVIEW", "MATCH"])
    filtered = [m for m in matches if m["score"] >= min_score]

    with open(out_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "kalshi_ticker",
            "polymarket_id",
            "score",
            "status",
            "hard_fail_reasons",
            "soft_reasons",
        ])
        for match in filtered:
            writer.writerow(
                [
                    match["kalshi_ticker"],
                    match["polymarket_id"],
                    match["score"],
                    match["status"],
                    ",".join(match["reasons"].get("hard_fail_reasons", [])),
                    ",".join(match["reasons"].get("soft_reasons", [])),
                ]
            )

    logger.info("Wrote %d review rows to %s", len(filtered), out_path)


def explain_match(config: AppConfig, kalshi_ticker: str, polymarket_id: str) -> None:
    kalshi_raw = get_spec(config.db_path, "kalshi", kalshi_ticker)
    poly_raw = get_spec(config.db_path, "polymarket", polymarket_id)
    match = get_match(config.db_path, kalshi_ticker, polymarket_id)
    raise SystemExit("Use explain-event-match or explain-outcome-match.")


def explain_event_match(config: AppConfig, kalshi_event_id: str, polymarket_event_id: str) -> None:
    kalshi_event = get_event_group(config.db_path, "kalshi", kalshi_event_id)
    poly_event = get_event_group(config.db_path, "polymarket", polymarket_event_id)
    if not kalshi_event or not poly_event:
        raise SystemExit("Missing event groups in DB. Run scan-catalog first.")

    kalshi_group = _event_group_from_db("kalshi", kalshi_event_id, kalshi_event, config)
    poly_group = _event_group_from_db("polymarket", polymarket_event_id, poly_event, config)
    decision = analyze_event_pair(
        kalshi_group,
        poly_group,
        EmbeddingScorer(),
        time_window_days=config.time_window_days,
        min_review_score=config.min_review_score,
    )
    kalshi_features = extract_event_features(kalshi_group.rules_text)
    poly_features = extract_event_features(poly_group.rules_text)
    outcome_pairs = map_outcomes(kalshi_group, poly_group)
    output = {
        "kalshi_event": kalshi_event,
        "polymarket_event": poly_event,
        "comparison": {
            "event_type": (kalshi_features["event_type"], poly_features["event_type"]),
            "predicate": (kalshi_features["predicate"], poly_features["predicate"]),
            "units": (kalshi_features["units"], poly_features["units"]),
            "entities": (kalshi_features["entities"], poly_features["entities"]),
            "close_time": (kalshi_group.close_time, poly_group.close_time),
            "resolution_source": (
                kalshi_event.get("resolutionSource"),
                poly_event.get("resolutionSource"),
            ),
        },
        "scores": decision,
        "mapped_outcomes": [pair.__dict__ for pair in outcome_pairs],
    }
    print(json.dumps(output, indent=2, sort_keys=True))


def explain_outcome_match(config: AppConfig, kalshi_market_id: str, polymarket_market_id: str) -> None:
    matches = list_outcome_matches(config.db_path, statuses=None)
    match = None
    for row in matches:
        if row["kalshi_market_id"] == kalshi_market_id and row["polymarket_market_id"] == polymarket_market_id:
            match = row
            break
    if not match:
        raise SystemExit("Outcome match not found in DB.")
    kalshi_outcome = get_outcome_by_market(config.db_path, "kalshi", kalshi_market_id)
    poly_outcome = get_outcome_by_market(config.db_path, "polymarket", polymarket_market_id)
    output = {
        "kalshi_outcome": kalshi_outcome,
        "polymarket_outcome": poly_outcome,
        "match": match,
    }
    print(json.dumps(output, indent=2, sort_keys=True))


def _event_group_from_db(venue: str, event_id: str, raw_event: dict, config: AppConfig):
    close_time = _parse_time(raw_event.get("close_time") or raw_event.get("endDate") or raw_event.get("closeTime"))
    rules_text = raw_event.get("rules_text")
    if not rules_text:
        rules_text = "\n".join(
            part
            for part in [
                raw_event.get("title") or raw_event.get("question") or "",
                raw_event.get("description") or "",
                raw_event.get("rules_primary") or raw_event.get("rulesPrimary") or "",
                raw_event.get("rules_secondary") or raw_event.get("rulesSecondary") or "",
            ]
            if part
        )
    outcomes = get_outcomes_for_event(config.db_path, venue, event_id)
    outcome_objs = []
    for outcome in outcomes:
        outcome_objs.append(
            OutcomeMarket(
                outcome_key=outcome["outcome_key"],
                display_label=outcome["display_label"],
                market_id=outcome["market_id"],
                rules_text=outcome["rules_text"],
                event_domain=outcome.get("event_domain") or raw_event.get("event_domain") or "",
                canonical_predicate=predicate_from_dict(
                    json.loads(outcome.get("predicate_json") or "null")
                )
                if outcome.get("predicate_json")
                else None,
                yes_token_id=outcome.get("yes_token_id"),
                no_token_id=outcome.get("no_token_id"),
            )
        )
    return EventGroup(
        venue=venue,
        event_id=event_id,
        title=raw_event.get("title") or raw_event.get("question") or "",
        close_time=close_time,
        rules_text=rules_text or "",
        event_domain=raw_event.get("event_domain") or "",
        outcomes=outcome_objs,
    )


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
