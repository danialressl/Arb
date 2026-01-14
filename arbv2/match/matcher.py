import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from arbv2.models import Market, MatchResult, SportsPredicate
from arbv2.teams import canonicalize_team


logger = logging.getLogger(__name__)

EventKey = Tuple[str, str, str]


@dataclass
class EventMatchStats:
    exact_used: int = 0
    subset_used: int = 0


def match_predicates(
    kalshi_markets: List[Market],
    polymarket_markets: List[Market],
    kalshi_preds: List[SportsPredicate],
    poly_preds: List[SportsPredicate],
    subset_matching: bool = False,
) -> List[MatchResult]:
    kalshi_by_market = {m.market_id: m for m in kalshi_markets}
    poly_by_market = {m.market_id: m for m in polymarket_markets}
    kalshi_by_event = _group_by_event_key(kalshi_markets, kalshi_preds)
    poly_by_event = _group_by_event_key(polymarket_markets, poly_preds)

    kalshi_keys = list(kalshi_by_event.keys())
    poly_keys = list(poly_by_event.keys())
    matched_pairs, stats = _match_event_keys(kalshi_keys, poly_keys, subset_matching=subset_matching)
    logger.info(
        "Event keys: kalshi=%d polymarket=%d matched=%d", len(kalshi_keys), len(poly_keys), len(matched_pairs)
    )
    if subset_matching:
        logger.info("Event key matches: exact=%d subset=%d", stats.exact_used, stats.subset_used)

    matches: List[MatchResult] = []
    outcome_mismatches = 0
    for kalshi_key, poly_key in matched_pairs:
        kalshi_group = kalshi_by_event[kalshi_key]
        poly_group = poly_by_event[poly_key]
        if subset_matching:
            outcome_mismatches += _match_outcomes_by_subset(kalshi_group, poly_group, matches)
        else:
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
                                kalshi_title=km.title,
                                polymarket_title=pm.title,
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


def _match_event_keys(
    kalshi_keys: List[EventKey],
    poly_keys: List[EventKey],
    *,
    subset_matching: bool,
) -> Tuple[List[Tuple[EventKey, EventKey]], EventMatchStats]:
    stats = EventMatchStats()
    if not subset_matching:
        matched = [(key, key) for key in set(kalshi_keys).intersection(poly_keys)]
        stats.exact_used = len(matched)
        return matched, stats

    matched_pairs: List[Tuple[EventKey, EventKey]] = []
    for k_key in kalshi_keys:
        for p_key in poly_keys:
            if k_key[2] != p_key[2]:
                continue
            if k_key == p_key:
                matched_pairs.append((k_key, p_key))
                stats.exact_used += 1
                continue
            if _event_key_equivalent(k_key, p_key):
                matched_pairs.append((k_key, p_key))
                stats.subset_used += 1
    return matched_pairs, stats


def _event_key_equivalent(left: EventKey, right: EventKey) -> bool:
    if left[2] != right[2]:
        return False
    l1, l2 = left[0], left[1]
    r1, r2 = right[0], right[1]
    direct = _tokens_equivalent(l1, r1) and _tokens_equivalent(l2, r2)
    swapped = _tokens_equivalent(l1, r2) and _tokens_equivalent(l2, r1)
    if direct:
        _log_subset_match("EVENTKEY", l1, r1, left[2])
        _log_subset_match("EVENTKEY", l2, r2, left[2])
    if swapped:
        _log_subset_match("EVENTKEY", l1, r2, left[2])
        _log_subset_match("EVENTKEY", l2, r1, left[2])
    return direct or swapped


def _match_outcomes_by_subset(
    kalshi_group: List[Market],
    poly_group: List[Market],
    matches: List[MatchResult],
) -> int:
    mismatches = 0
    for km in kalshi_group:
        k_label = _normalize_outcome_label(km.outcome_label)
        if not k_label:
            mismatches += 1
            continue
        matched = False
        for pm in poly_group:
            p_label = _normalize_outcome_label(pm.outcome_label)
            if not p_label:
                continue
            if not _tokens_equivalent(k_label, p_label):
                continue
            _log_subset_match("OUTCOME", k_label, p_label, km.event_date or "")
            if km.market_type and pm.market_type and km.market_type != pm.market_type:
                continue
            matches.append(
                MatchResult(
                    kalshi_market_id=km.market_id,
                    polymarket_market_id=pm.market_id,
                    equivalent=True,
                    reason="event_key_match",
                    kalshi_title=km.title,
                    polymarket_title=pm.title,
                )
            )
            matched = True
        if not matched:
            mismatches += 1
    return mismatches


def _tokens_equivalent(left: str, right: str) -> bool:
    if left == right:
        return True
    left_tokens = _tokenize(left)
    right_tokens = _tokenize(right)
    if not left_tokens or not right_tokens:
        return False
    return left_tokens.issubset(right_tokens) or right_tokens.issubset(left_tokens)


def _log_subset_match(kind: str, left: str, right: str, event_date: str) -> None:
    if left == right:
        return
    logger.debug(
        "%s_FALLBACK_MATCH left=%s right=%s left_tokens=%s right_tokens=%s event_date=%s",
        kind,
        left,
        right,
        sorted(_tokenize(left)),
        sorted(_tokenize(right)),
        event_date,
    )


def _tokenize(value: str) -> Set[str]:
    text = re.sub(r"[^\w\s]", " ", value.upper())
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return set()
    return set(text.split())

def _normalize_outcome_label(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = re.sub(r"[^\w\s]", " ", value.upper())
    text = re.sub(r"\s+", " ", text).strip()
    if text in {"TIE", "DRAW"}:
        return "DRAW"
    return text or None
