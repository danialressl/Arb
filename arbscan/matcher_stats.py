from __future__ import annotations

import logging

from arbscan.config import AppConfig
from arbscan.storage import list_event_matches

logger = logging.getLogger(__name__)


def matcher_stats(config: AppConfig) -> None:
    matches = list_event_matches(config.db_path)
    if not matches:
        logger.warning("No matches found in DB.")
        return

    scores = [m["score"] for m in matches]
    counts = {"MATCH": 0, "REVIEW": 0, "NO": 0}
    for match in matches:
        counts[match["status"]] = counts.get(match["status"], 0) + 1

    thresholds = [0.5, 0.6, 0.7, 0.8, 0.9]
    threshold_counts = {}
    for threshold in thresholds:
        threshold_counts[str(threshold)] = sum(1 for score in scores if score >= threshold)

    scores_sorted = sorted(scores)
    min_score = scores_sorted[0]
    max_score = scores_sorted[-1]
    median = _median(scores_sorted)

    print("Matcher stats:")
    print(f"  total comparisons: {len(scores)}")
    print(f"  status counts: {counts}")
    print(f"  score min/median/max: {min_score:.3f} / {median:.3f} / {max_score:.3f}")
    print(f"  threshold counts: {threshold_counts}")


def _median(values: list[float]) -> float:
    mid = len(values) // 2
    if len(values) % 2 == 0:
        return (values[mid - 1] + values[mid]) / 2.0
    return values[mid]
