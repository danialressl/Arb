from __future__ import annotations

import math
from datetime import datetime
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

from arbscan.models import MatchCandidate, ResolutionSpec


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

    def _load(self):
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer("all-MiniLM-L6-v2")

    def similarity(self, text_a: str, text_b: str) -> float:
        self._load()
        embeddings = self._model.encode([text_a, text_b])
        return _cosine_similarity(embeddings[0], embeddings[1])


def match_markets(
    kalshi_specs: List[ResolutionSpec],
    poly_specs: List[ResolutionSpec],
    top_k: int = 5,
    reject_stats: Optional[dict] = None,
    candidate_limit: int = 200,
    time_window_days: int = 7,
    min_review_score: float = 0.5,
    debug_top_n: int = 10,
) -> List[MatchCandidate]:
    scorer = EmbeddingScorer()
    candidates: List[MatchCandidate] = []
    poly_tokens = [_tokenize(spec) for spec in poly_specs]

    total_comparisons = 0
    text_scores: List[float] = []
    top_pairs: List[dict] = []

    for idx, kalshi in enumerate(kalshi_specs, start=1):
        scored: List[Tuple[ResolutionSpec, float, List[str], List[str]]] = []
        poly_subset = _candidate_subset(kalshi, poly_specs, poly_tokens, candidate_limit)
        for poly in poly_subset:
            total_comparisons += 1
            decision = evaluate_pair(
                kalshi,
                poly,
                scorer,
                time_window_days=time_window_days,
                min_review_score=min_review_score,
            )
            text_scores.append(decision["text_score"])
            top_pairs = _update_top_pairs(
                top_pairs,
                decision["equivalence_score"],
                decision["text_score"],
                kalshi,
                poly,
                debug_top_n,
            )
            if decision["rejection_reason"]:
                _track_rejects(reject_stats, [decision["rejection_reason"]])
            hard_reasons = []
            soft_reasons = []
            if decision["rejection_reason"]:
                if decision["rejection_type"] == "HARD":
                    hard_reasons = [decision["rejection_reason"]]
                else:
                    soft_reasons = [decision["rejection_reason"]]
            scored.append((poly, decision["equivalence_score"], hard_reasons, soft_reasons))

        scored.sort(key=lambda item: item[1], reverse=True)
        for poly, score, hard, soft in scored[:top_k]:
            candidates.append(
                MatchCandidate(
                    kalshi_ticker=kalshi.market_id,
                    polymarket_id=poly.market_id,
                    score=round(score, 4),
                    hard_fail_reasons=hard,
                    soft_reasons=soft,
                )
            )

    stats = {
        "total_comparisons": total_comparisons,
        "text_scores": text_scores,
        "top_pairs": top_pairs,
    }
    return candidates, stats


def determine_status(
    score: float, hard_fail: List[str], match_threshold: float, min_review_score: float
) -> str:
    if hard_fail:
        return "NO"
    if score >= match_threshold:
        return "MATCH"
    if score >= min_review_score:
        return "REVIEW"
    return "NO"


def _text_similarity(text_a: str, text_b: str, scorer: EmbeddingScorer) -> float:
    if scorer.available():
        try:
            return float(scorer.similarity(text_a, text_b))
        except Exception:
            pass
    return SequenceMatcher(None, text_a.lower(), text_b.lower()).ratio()


def _time_score(a: Optional[datetime], b: Optional[datetime]) -> float:
    if not a or not b:
        return 0.5
    delta = abs((a - b).total_seconds())
    days = delta / 86400.0
    return max(0.0, 1.0 - min(days / 30.0, 1.0))


def _entity_overlap(entities_a: List[str], entities_b: List[str]) -> float:
    if not entities_a or not entities_b:
        return 0.0
    set_a = set([e.lower() for e in entities_a])
    set_b = set([e.lower() for e in entities_b])
    return len(set_a & set_b) / max(len(set_a | set_b), 1)


def _predicate_score(kalshi: ResolutionSpec, poly: ResolutionSpec) -> float:
    if not kalshi.predicate or not poly.predicate:
        return 0.5
    return 1.0 if kalshi.predicate == poly.predicate else 0.0


def _compatibility_reasons(kalshi: ResolutionSpec, poly: ResolutionSpec) -> Tuple[List[str], List[str]]:
    hard: List[str] = []
    soft: List[str] = []

    if kalshi.event_type and poly.event_type and kalshi.event_type != poly.event_type:
        soft.append("event_type mismatch")

    if kalshi.predicate and poly.predicate and kalshi.predicate != poly.predicate:
        soft.append("predicate mismatch")

    if kalshi.threshold_units and poly.threshold_units and kalshi.threshold_units != poly.threshold_units:
        soft.append("threshold units mismatch")

    if kalshi.resolution_source and poly.resolution_source:
        if kalshi.resolution_source.lower() != poly.resolution_source.lower():
            soft.append("resolution source differs")

    if kalshi.measurement_time and poly.measurement_time:
        if kalshi.measurement_time != poly.measurement_time:
            soft.append("measurement time differs")

    return hard, soft


def _track_rejects(reject_stats: Optional[dict], reasons: List[str]) -> None:
    if reject_stats is None:
        return
    for reason in reasons:
        reject_stats[reason] = reject_stats.get(reason, 0) + 1


def evaluate_pair(
    kalshi: ResolutionSpec,
    poly: ResolutionSpec,
    scorer: EmbeddingScorer,
    time_window_days: int,
    min_review_score: float,
) -> dict:
    analysis = analyze_pair(kalshi, poly, scorer, time_window_days)
    rejection_reason = None
    rejection_type = None
    if analysis["hard_reason"]:
        rejection_reason = analysis["hard_reason"]
        rejection_type = "HARD"
    elif analysis["equivalence_score"] < min_review_score:
        rejection_reason = analysis["soft_reason"]
        rejection_type = "SOFT"

    analysis["rejection_reason"] = rejection_reason
    analysis["rejection_type"] = rejection_type
    return analysis


def analyze_pair(
    kalshi: ResolutionSpec,
    poly: ResolutionSpec,
    scorer: EmbeddingScorer,
    time_window_days: int,
) -> dict:
    hard_reason = _hard_rejection_reason(kalshi, poly, time_window_days)
    text_score = _text_similarity(kalshi.rules_text, poly.rules_text, scorer)
    time_score = _time_score(kalshi.close_time or kalshi.end_time, poly.end_time or poly.close_time)
    entity_score = _entity_overlap(kalshi.entities, poly.entities)
    predicate_score = _predicate_score(kalshi, poly)
    equivalence_score = 0.6 * text_score + 0.2 * time_score + 0.1 * entity_score + 0.1 * predicate_score
    soft_reason = _soft_rejection_reason(text_score, entity_score, time_score)

    return {
        "equivalence_score": equivalence_score,
        "text_score": text_score,
        "time_score": time_score,
        "entity_score": entity_score,
        "predicate_score": predicate_score,
        "hard_reason": hard_reason,
        "soft_reason": soft_reason,
    }


def _hard_rejection_reason(kalshi: ResolutionSpec, poly: ResolutionSpec, time_window_days: int) -> Optional[str]:
    if kalshi.is_binary is False or poly.is_binary is False:
        if kalshi.is_binary != poly.is_binary:
            return "binary_mismatch"
    if kalshi.event_type and poly.event_type and kalshi.event_type != poly.event_type:
        return "event_type_mismatch"
    if kalshi.predicate and poly.predicate and kalshi.predicate != poly.predicate:
        return "predicate_conflict"
    if kalshi.threshold_units and poly.threshold_units and kalshi.threshold_units != poly.threshold_units:
        return "unit_conflict"
    if _time_non_overlapping(kalshi, poly, time_window_days):
        return "non_overlapping_time_window"
    if not kalshi.rules_text or not poly.rules_text:
        return "bug_or_missing_field"
    return None


def _soft_rejection_reason(text_score: float, entity_score: float, time_score: float) -> str:
    if text_score < 0.2:
        return "text_similarity_too_low"
    if entity_score == 0.0:
        return "entity_disjoint"
    if time_score < 0.2:
        return "non_overlapping_time_window"
    return "text_similarity_too_low"


def _time_non_overlapping(kalshi: ResolutionSpec, poly: ResolutionSpec, time_window_days: int) -> bool:
    a = kalshi.close_time or kalshi.end_time
    b = poly.close_time or poly.end_time
    if not a or not b:
        return False
    delta_days = abs((a - b).total_seconds()) / 86400.0
    return delta_days > time_window_days


def _update_top_pairs(
    top_pairs: List[dict],
    score: float,
    text_score: float,
    kalshi: ResolutionSpec,
    poly: ResolutionSpec,
    limit: int,
) -> List[dict]:
    top_pairs.append(
        {
            "kalshi_ticker": kalshi.market_id,
            "polymarket_id": poly.market_id,
            "score": score,
            "text_score": text_score,
            "kalshi_entities": kalshi.entities,
            "poly_entities": poly.entities,
            "kalshi_time": (kalshi.close_time or kalshi.end_time).isoformat()
            if (kalshi.close_time or kalshi.end_time)
            else None,
            "poly_time": (poly.close_time or poly.end_time).isoformat()
            if (poly.close_time or poly.end_time)
            else None,
        }
    )
    top_pairs.sort(key=lambda item: item["score"], reverse=True)
    return top_pairs[:limit]


def _candidate_subset(
    kalshi: ResolutionSpec,
    poly_specs: List[ResolutionSpec],
    poly_tokens: List[set],
    limit: int,
) -> List[ResolutionSpec]:
    if limit <= 0 or limit >= len(poly_specs):
        return poly_specs
    kalshi_tokens = _tokenize(kalshi)
    scores: List[Tuple[int, int]] = []
    for idx, token_set in enumerate(poly_tokens):
        overlap = len(kalshi_tokens & token_set)
        scores.append((overlap, idx))
    scores.sort(key=lambda item: item[0], reverse=True)
    top_indices = [idx for _, idx in scores[:limit]]
    return [poly_specs[idx] for idx in top_indices]


def _tokenize(spec: ResolutionSpec) -> set:
    text = f"{spec.title} {spec.rules_text}".lower()
    tokens = []
    current = []
    for char in text:
        if char.isalnum():
            current.append(char)
        else:
            if current:
                token = "".join(current)
                if len(token) > 2:
                    tokens.append(token)
                current = []
    if current:
        token = "".join(current)
        if len(token) > 2:
            tokens.append(token)
    return set(tokens)


def _cosine_similarity(vec_a, vec_b) -> float:
    dot = float(sum(a * b for a, b in zip(vec_a, vec_b)))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def spec_from_dict(data: dict) -> ResolutionSpec:
    def _parse_time(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    return ResolutionSpec(
        venue=data.get("venue", ""),
        market_id=str(data.get("market_id", "")),
        title=data.get("title", ""),
        close_time=_parse_time(data.get("close_time")),
        end_time=_parse_time(data.get("end_time")),
        resolution_source=data.get("resolution_source"),
        rules_text=data.get("rules_text", ""),
        is_binary=data.get("is_binary"),
        event_type=data.get("event_type"),
        entities=data.get("entities") or [],
        predicate=data.get("predicate"),
        threshold=data.get("threshold"),
        threshold_units=data.get("threshold_units"),
        measurement_time=data.get("measurement_time"),
        timezone=data.get("timezone"),
        void_conditions=data.get("void_conditions") or [],
    )
