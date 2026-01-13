# Semantic core: market equivalence logic. DO NOT RUN IN LIVE LOOP.
from __future__ import annotations

import math
from datetime import datetime
from difflib import SequenceMatcher
from typing import List, Optional, Tuple

from arbscan.canonical import predicate_key
from arbscan.models import EventGroup, OutcomePair, Predicate

REJECTION_REASONS = [
    "binary_mismatch",
    "event_type_mismatch",
    "predicate_conflict",
    "unit_conflict",
    "non_overlapping_time_window",
    "entity_disjoint",
    "text_similarity_too_low",
    "bug_or_missing_field",
]

HARD_REASONS = {
    "binary_mismatch",
    "event_type_mismatch",
    "predicate_conflict",
    "unit_conflict",
    "non_overlapping_time_window",
    "bug_or_missing_field",
}


class EmbeddingScorer:
    def __init__(self) -> None:
        self._model = None

    def available(self) -> bool:
        try:
            import sentence_transformers  # noqa: F401
            return True
        except ModuleNotFoundError:
            return False

    def _load(self) -> None:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer("all-MiniLM-L6-v2")

    def similarity(self, text_a: str, text_b: str) -> float:
        self._load()
        embeddings = self._model.encode([text_a, text_b])
        return _cosine_similarity(embeddings[0], embeddings[1])


def match_event_groups(
    kalshi_events: List[EventGroup],
    poly_events: List[EventGroup],
    top_k: int,
    time_window_days: int,
    min_review_score: float,
) -> tuple[list[dict], dict]:
    scorer = EmbeddingScorer()
    total_comparisons = 0
    rejection_counts = {}
    score_samples: List[float] = []
    top_pairs: List[dict] = []
    matches = []

    for kalshi in kalshi_events:
        scored = []
        for poly in poly_events:
            total_comparisons += 1
            decision = analyze_event_pair(kalshi, poly, scorer, time_window_days, min_review_score)
            score_samples.append(decision["event_score"])
            _track_reject(rejection_counts, decision["rejection_reason"])
            top_pairs = _update_top_pairs(
                top_pairs,
                decision["event_score"],
                decision["text_score"],
                kalshi,
                poly,
                limit=10,
            )
            scored.append((poly, decision))

        scored.sort(key=lambda item: item[1]["event_score"], reverse=True)
        for poly, decision in scored[:top_k]:
            matches.append(
                {
                    "kalshi_event_id": kalshi.event_id,
                    "polymarket_event_id": poly.event_id,
                    "score": round(decision["event_score"], 4),
                    "rejection_reason": decision["rejection_reason"],
                    "rejection_type": decision["rejection_type"],
                    "reasons": decision,
                }
            )

    stats = {
        "total_comparisons": total_comparisons,
        "rejection_counts": rejection_counts,
        "score_samples": score_samples,
        "top_pairs": top_pairs,
    }
    return matches, stats


def analyze_event_pair(
    kalshi: EventGroup,
    poly: EventGroup,
    scorer: EmbeddingScorer,
    time_window_days: int,
    min_review_score: float,
) -> dict:
    if not kalshi.rules_text or not poly.rules_text:
        return _decision(0.0, 0.0, 0.0, 0.0, "bug_or_missing_field", "HARD")
    if not kalshi.outcomes or not poly.outcomes:
        return _decision(0.0, 0.0, 0.0, 0.0, "bug_or_missing_field", "HARD")

    if _binary_mismatch(kalshi, poly):
        return _decision(0.0, 0.0, 0.0, 0.0, "binary_mismatch", "HARD")

    if _event_type_mismatch(kalshi.rules_text, poly.rules_text):
        return _decision(0.0, 0.0, 0.0, 0.0, "event_type_mismatch", "HARD")

    if _predicate_conflict(kalshi.rules_text, poly.rules_text):
        return _decision(0.0, 0.0, 0.0, 0.0, "predicate_conflict", "HARD")

    if _unit_conflict(kalshi.rules_text, poly.rules_text):
        return _decision(0.0, 0.0, 0.0, 0.0, "unit_conflict", "HARD")

    text_score = _text_similarity(kalshi.rules_text, poly.rules_text, scorer)
    title_score = _text_similarity(kalshi.title, poly.title, scorer)
    time_score = _time_score(kalshi.close_time, poly.close_time, time_window_days)
    event_score = 0.5 * text_score + 0.3 * title_score + 0.2 * time_score

    rejection_reason = None
    rejection_type = None
    if time_score == 0.0:
        rejection_reason = "non_overlapping_time_window"
        rejection_type = "HARD"
    elif event_score < min_review_score:
        rejection_reason = _soft_reason(kalshi, poly, text_score, time_score)
        rejection_type = "SOFT"

    return _decision(event_score, text_score, title_score, time_score, rejection_reason, rejection_type)


def map_outcomes(
    kalshi: EventGroup,
    poly: EventGroup,
) -> list[OutcomePair]:
    poly_by_key = {}
    for outcome in poly.outcomes:
        key = _predicate_key(outcome.canonical_predicate)
        if key:
            poly_by_key[key] = outcome

    pairs: list[OutcomePair] = []
    for outcome in kalshi.outcomes:
        key = _predicate_key(outcome.canonical_predicate)
        if key and key in poly_by_key:
            pairs.append(
                OutcomePair(
                    kalshi_market_ticker=outcome.market_id,
                    polymarket_market_id=poly_by_key[key].market_id,
                    outcome_key=key,
                    mapping_confidence=1.0,
                    mapping_reason="predicate_match",
                )
            )

    return pairs


def _predicate_key(predicate: Optional[Predicate]) -> Optional[str]:
    if not predicate:
        return None
    return predicate_key(predicate)


def _binary_mismatch(kalshi: EventGroup, poly: EventGroup) -> bool:
    kalshi_binary = len(kalshi.outcomes) == 2
    poly_binary = len(poly.outcomes) == 2
    return kalshi_binary != poly_binary


def _event_type_mismatch(kalshi_text: str, poly_text: str) -> bool:
    kalshi_type = _infer_event_type(kalshi_text)
    poly_type = _infer_event_type(poly_text)
    return bool(kalshi_type and poly_type and kalshi_type != poly_type)


def _predicate_conflict(kalshi_text: str, poly_text: str) -> bool:
    kalshi_pred = _infer_predicate(kalshi_text)
    poly_pred = _infer_predicate(poly_text)
    return bool(kalshi_pred and poly_pred and kalshi_pred != poly_pred)


def _unit_conflict(kalshi_text: str, poly_text: str) -> bool:
    kalshi_unit = _infer_units(kalshi_text)
    poly_unit = _infer_units(poly_text)
    return bool(kalshi_unit and poly_unit and kalshi_unit != poly_unit)


def _soft_reason(
    kalshi: EventGroup,
    poly: EventGroup,
    text_score: float,
    time_score: float,
) -> str:
    if text_score < 0.2:
        return "text_similarity_too_low"
    if time_score < 0.2:
        return "non_overlapping_time_window"
    if _entities_disjoint(kalshi.rules_text, poly.rules_text):
        return "entity_disjoint"
    return "text_similarity_too_low"


def _text_similarity(text_a: str, text_b: str, scorer: EmbeddingScorer) -> float:
    if scorer.available():
        try:
            return float(scorer.similarity(text_a, text_b))
        except Exception:
            pass
    return SequenceMatcher(None, text_a.lower(), text_b.lower()).ratio()


def _time_score(a: Optional[datetime], b: Optional[datetime], time_window_days: int) -> float:
    if not a or not b:
        return 0.5
    delta = abs((a - b).total_seconds())
    days = delta / 86400.0
    if days > time_window_days:
        return 0.0
    return max(0.1, 1.0 - (days / max(time_window_days, 1)))


def _decision(
    event_score: float,
    text_score: float,
    title_score: float,
    time_score: float,
    rejection_reason: Optional[str],
    rejection_type: Optional[str],
) -> dict:
    return {
        "event_score": event_score,
        "text_score": text_score,
        "title_score": title_score,
        "time_score": time_score,
        "rejection_reason": rejection_reason,
        "rejection_type": rejection_type,
    }


def _track_reject(counts: dict, reason: Optional[str]) -> None:
    if not reason:
        return
    counts[reason] = counts.get(reason, 0) + 1


def _update_top_pairs(
    top_pairs: List[dict],
    score: float,
    text_score: float,
    kalshi: EventGroup,
    poly: EventGroup,
    limit: int,
) -> List[dict]:
    top_pairs.append(
        {
            "kalshi_event_id": kalshi.event_id,
            "polymarket_event_id": poly.event_id,
            "score": score,
            "text_score": text_score,
            "kalshi_time": kalshi.close_time.isoformat() if kalshi.close_time else None,
            "poly_time": poly.close_time.isoformat() if poly.close_time else None,
        }
    )
    top_pairs.sort(key=lambda item: item["score"], reverse=True)
    return top_pairs[:limit]


def _cosine_similarity(vec_a, vec_b) -> float:
    dot = float(sum(a * b for a, b in zip(vec_a, vec_b)))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _infer_event_type(text: str) -> Optional[str]:
    lowered = text.lower()
    if "temperature" in lowered or "degrees" in lowered:
        return "weather"
    if "election" in lowered or "votes" in lowered or "president" in lowered:
        return "election"
    if "price" in lowered or "settle" in lowered or "closing" in lowered:
        return "price"
    if "rate" in lowered or "inflation" in lowered or "cpi" in lowered:
        return "macro"
    return None


def _infer_predicate(text: str) -> Optional[str]:
    lowered = text.lower()
    if "at least" in lowered or "greater than" in lowered or "above" in lowered:
        return "above"
    if "at most" in lowered or "less than" in lowered or "below" in lowered:
        return "below"
    if "exactly" in lowered or "equal to" in lowered:
        return "equal"
    return None


def _infer_units(text: str) -> Optional[str]:
    lowered = text.lower()
    for unit in ["percent", "%", "bps", "basis points", "usd", "dollars", "points"]:
        if unit in lowered:
            return unit
    return None


def _entities_disjoint(text_a: str, text_b: str) -> bool:
    entities_a = _simple_entities(text_a)
    entities_b = _simple_entities(text_b)
    if not entities_a or not entities_b:
        return False
    return len(entities_a & entities_b) == 0


def _simple_entities(text: str) -> set:
    tokens = []
    current = []
    for char in text:
        if char.isalnum():
            current.append(char)
        else:
            if current:
                token = "".join(current)
                if token.istitle() and len(token) > 2:
                    tokens.append(token.lower())
                current = []
    if current:
        token = "".join(current)
        if token.istitle() and len(token) > 2:
            tokens.append(token.lower())
    return set(tokens)


def extract_event_features(text: str) -> dict:
    return {
        "event_type": _infer_event_type(text),
        "predicate": _infer_predicate(text),
        "units": _infer_units(text),
        "entities": sorted(_simple_entities(text)),
    }
