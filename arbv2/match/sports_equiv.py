import re
import unicodedata
from typing import Set, Tuple

from arbv2.models import MatchResult, SportsPredicate


def normalize_tokens(text: str) -> Set[str]:
    if not text:
        return set()
    value = unicodedata.normalize("NFKD", text)
    value = value.lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return set()
    return set(value.split(" "))


def containment(a: Set[str], b: Set[str]) -> bool:
    if not a or not b:
        return False
    return a.issubset(b) or b.issubset(a)


def predicates_equivalent(p1: SportsPredicate, p2: SportsPredicate, max_hours: int) -> MatchResult:
    t1a = normalize_tokens(p1.team_a)
    t1b = normalize_tokens(p1.team_b)
    t2a = normalize_tokens(p2.team_a)
    t2b = normalize_tokens(p2.team_b)

    if not containment(t1a, t2a):
        return MatchResult(p1.market_id, p2.market_id, False, "team_a_mismatch")
    if not containment(t1b, t2b):
        return MatchResult(p1.market_id, p2.market_id, False, "team_b_mismatch")

    if _role_separation_failed(t1a, t1b) or _role_separation_failed(t2a, t2b):
        return MatchResult(p1.market_id, p2.market_id, False, "role_separation_failed")

    if not _winner_consistent(p1, t1a, t1b, p2, t2a, t2b):
        return MatchResult(p1.market_id, p2.market_id, False, "winner_mismatch")

    return MatchResult(p1.market_id, p2.market_id, True, "ok")


def _winner_consistent(
    p1: SportsPredicate,
    t1a: Set[str],
    t1b: Set[str],
    p2: SportsPredicate,
    t2a: Set[str],
    t2b: Set[str],
) -> bool:
    w1 = normalize_tokens(p1.winner)
    w2 = normalize_tokens(p2.winner)
    w1_is_a = containment(w1, t1a)
    w1_is_b = containment(w1, t1b)
    w2_is_a = containment(w2, t2a)
    w2_is_b = containment(w2, t2b)
    if w1_is_a and w1_is_b:
        return False
    if w2_is_a and w2_is_b:
        return False
    return (w1_is_a and w2_is_a) or (w1_is_b and w2_is_b)


def _role_separation_failed(a: Set[str], b: Set[str]) -> bool:
    if not a or not b:
        return True
    intersection = a.intersection(b)
    ratio = len(intersection) / max(1, min(len(a), len(b)))
    return ratio >= 0.4
