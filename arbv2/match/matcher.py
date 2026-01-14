import logging
import re
from typing import Dict, Iterable, List, Optional, Tuple

from arbv2.models import Market, MatchResult, SportsPredicate
from arbv2.teams import canonicalize_team


logger = logging.getLogger(__name__)

EventKey = Tuple[str, str, str]


def match_predicates(
    kalshi_markets: List[Market],
    polymarket_markets: List[Market],
    kalshi_preds: List[SportsPredicate],
    poly_preds: List[SportsPredicate],
) -> List[MatchResult]:
    kalshi_by_market = {m.market_id: m for m in kalshi_markets}
    poly_by_market = {m.market_id: m for m in polymarket_markets}
    kalshi_by_event = _group_by_event_key(kalshi_markets, kalshi_preds)
    poly_by_event = _group_by_event_key(polymarket_markets, poly_preds)

    kalshi_keys = set(kalshi_by_event.keys())
    poly_keys = set(poly_by_event.keys())
    matched_keys = kalshi_keys.intersection(poly_keys)
    logger.info("Event keys: kalshi=%d polymarket=%d matched=%d", len(kalshi_keys), len(poly_keys), len(matched_keys))
    logger.info(
        "Rejects event_key_mismatch: kalshi_only=%d polymarket_only=%d",
        len(kalshi_keys - poly_keys),
        len(poly_keys - kalshi_keys),
    )

    matches: List[MatchResult] = []
    outcome_mismatches = 0
    for key in matched_keys:
        kalshi_group = kalshi_by_event[key]
        poly_group = poly_by_event[key]
        k_by_outcome = _group_by_outcome(kalshi_group)
        p_by_outcome = _group_by_outcome(poly_group)
        for outcome_label, k_markets in k_by_outcome.items():
            p_markets = p_by_outcome.get(outcome_label)
            if not p_markets:
                outcome_mismatches += len(k_markets)
                continue
            for km in k_markets:
                for pm in p_markets:
                    if km.market_type and pm.market_type and km.market_type != pm.market_type:
                        continue
                    matches.append(
                        MatchResult(
                            kalshi_market_id=km.market_id,
                            polymarket_market_id=pm.market_id,
                            equivalent=True,
                            reason="event_key_match",
                        )
                    )
        missing = set(p_by_outcome.keys()) - set(k_by_outcome.keys())
        if missing:
            for missing_outcome in missing:
                outcome_mismatches += len(p_by_outcome.get(missing_outcome, []))

    if outcome_mismatches:
        logger.info("Rejects outcome_label_mismatch=%d", outcome_mismatches)
    return matches


def _group_by_event_key(
    markets: Iterable[Market], preds: Iterable[SportsPredicate]
) -> Dict[EventKey, List[Market]]:
    pred_by_market = {pred.market_id: pred for pred in preds}
    grouped: Dict[EventKey, List[Market]] = {}
    for market in markets:
        pred = pred_by_market.get(market.market_id)
        if not pred:
            continue
        event_key = _event_key_from_predicate(pred, market)
        if not event_key:
            continue
        grouped.setdefault(event_key, []).append(market)
    return grouped


def _event_key_from_predicate(pred: SportsPredicate, market: Market) -> Optional[EventKey]:
    if not market.event_date:
        return None
    team_a = canonicalize_team(pred.team_a)
    team_b = canonicalize_team(pred.team_b)
    if not team_a or not team_b:
        return None
    left, right = sorted([team_a, team_b])
    return (left, right, market.event_date)


def _group_by_outcome(markets: Iterable[Market]) -> Dict[str, List[Market]]:
    grouped: Dict[str, List[Market]] = {}
    for market in markets:
        label = _normalize_outcome_label(market.outcome_label)
        if not label:
            continue
        grouped.setdefault(label, []).append(market)
    return grouped




def _normalize_outcome_label(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"[^\w\s]", " ", value.upper())
    text = re.sub(r"\s+", " ", text).strip()
    if text in {"TIE", "DRAW"}:
        return "DRAW"
    return text or None
