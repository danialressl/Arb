# Semantic core: domain detection + predicate parsing. DO NOT RUN IN LIVE LOOP.
import re
from dataclasses import asdict
from typing import Optional

from arbscan.models import EventDomain, Predicate


def detect_event_domain(title: str, rules_text: str) -> str:
    # Market-level domain detection is intentionally aggressive for rate-delta markets.
    text = f"{title} {rules_text}".lower()
    keywords = [
        "bps",
        "basis point",
        "rate",
        "interest rate",
        "hike",
        "cut",
        "increase",
        "decrease",
        "maintain",
        "unchanged",
    ]
    magnitude_signals = ["+", ">", "<", "or more", "at least"]
    has_keyword = any(word in text for word in keywords)
    has_magnitude = any(token in text for token in magnitude_signals) or any(char.isdigit() for char in text)
    if has_keyword and has_magnitude:
        return EventDomain.RATE_DELTA
    return EventDomain.GENERIC_BINARY


def parse_rate_delta_outcome(label_text: str, rules_text: str) -> Optional[Predicate]:
    text = f"{label_text} {rules_text}".lower()
    text = re.sub(r"[^\w\s\+\-<>]", " ", text)

    if _is_no_change(text):
        return Predicate(
            variable="delta_bps",
            operator="==",
            value=0,
            unit="bps",
            reference="pre_meeting_rate",
        )

    direction = _direction(text)
    if direction is None:
        return None

    magnitude = _extract_magnitude(text)
    if magnitude is None:
        return None
    if magnitude == 0:
        return Predicate(
            variable="delta_bps",
            operator="==",
            value=0,
            unit="bps",
            reference="pre_meeting_rate",
        )

    plus_semantics = _has_plus_semantics(text)
    if direction < 0:
        operator = "<=" if plus_semantics else "=="
        value = -magnitude
    else:
        operator = ">=" if plus_semantics else "=="
        value = magnitude

    return Predicate(
        variable="delta_bps",
        operator=operator,
        value=value,
        unit="bps",
        reference="pre_meeting_rate",
    )


def predicate_key(predicate: Predicate) -> str:
    return f"{predicate.variable}|{predicate.operator}|{predicate.value}|{predicate.unit}"


def predicate_to_dict(predicate: Predicate) -> dict:
    return asdict(predicate)


def predicate_from_dict(data: Optional[dict]) -> Optional[Predicate]:
    if not data:
        return None
    return Predicate(
        variable=data.get("variable") or "",
        operator=data.get("operator") or "",
        value=int(data.get("value") or 0),
        unit=data.get("unit") or "",
        reference=data.get("reference") or "",
    )


def _is_no_change(text: str) -> bool:
    phrases = [
        "no change",
        "unchanged",
        "maintain",
        "maintains",
        "hike 0",
        "increase 0",
        "cut 0",
        "decrease 0",
        "0 bps",
        "0 bp",
        "0 basis point",
    ]
    return any(phrase in text for phrase in phrases)


def _direction(text: str) -> Optional[int]:
    if any(word in text for word in ["cut", "decrease", "down", "lower"]):
        return -1
    if any(word in text for word in ["hike", "increase", "up", "raise"]):
        return 1
    return None


def _extract_magnitude(text: str) -> Optional[int]:
    match = re.search(r"(\d+)\s*(?:bps|bp|basis points?)", text)
    if not match:
        match = re.search(r"(\d+)", text)
    if not match:
        return None
    return int(match.group(1))


def _has_plus_semantics(text: str) -> bool:
    return any(token in text for token in ["+", "or more", "at least", ">=", ">"])
