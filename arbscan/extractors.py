from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import List, Optional

import requests

from arbscan.models import ResolutionSpec


def polymarket_rules_text(question: str, description: str, resolution_source: str) -> str:
    parts = [question or "", description or "", resolution_source or ""]
    return "\n".join(part for part in parts if part).strip()


def kalshi_rules_text(title: str, subtitle: str, rules_primary: str, rules_secondary: str) -> str:
    parts = [title or "", subtitle or "", rules_primary or "", rules_secondary or ""]
    return "\n".join(part for part in parts if part).strip()


def extract_resolution_spec(
    venue: str,
    market_id: str,
    title: str,
    close_time: Optional[datetime],
    end_time: Optional[datetime],
    resolution_source: Optional[str],
    rules_text: str,
    is_binary: Optional[bool],
) -> ResolutionSpec:
    text = rules_text or ""
    llm_override = _llm_extract_spec(title, text)
    event_type = llm_override.get("event_type") if llm_override else _infer_event_type(text)
    entities = llm_override.get("entities") if llm_override else _extract_entities(title, text)
    if not isinstance(entities, list):
        entities = _extract_entities(title, text)
    predicate = llm_override.get("predicate") if llm_override else _extract_predicate(text)
    threshold = llm_override.get("threshold") if llm_override else None
    units = llm_override.get("threshold_units") if llm_override else None
    if threshold is not None:
        try:
            threshold = float(threshold)
        except (TypeError, ValueError):
            threshold = None
    if threshold is None and units is None:
        threshold, units = _extract_threshold(text)
    measurement_time = llm_override.get("measurement_time") if llm_override else None
    timezone = llm_override.get("timezone") if llm_override else None
    if measurement_time is None and timezone is None:
        measurement_time, timezone = _extract_measurement_time(text)
    void_conditions = (
        llm_override.get("void_conditions") if llm_override else _extract_void_conditions(text)
    )

    return ResolutionSpec(
        venue=venue,
        market_id=market_id,
        title=title,
        close_time=close_time,
        end_time=end_time,
        resolution_source=resolution_source,
        rules_text=text,
        is_binary=is_binary,
        event_type=event_type,
        entities=entities,
        predicate=predicate,
        threshold=threshold,
        threshold_units=units,
        measurement_time=measurement_time,
        timezone=timezone,
        void_conditions=void_conditions,
    )


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


def _extract_entities(title: str, text: str) -> List[str]:
    entities = set()
    source = f"{title} {text}"
    for match in re.findall(r"\b[A-Z][a-zA-Z]{2,}(?:\s+[A-Z][a-zA-Z]{2,})*", source):
        entities.add(match.strip())
    return sorted(entities)[:10]


def _extract_predicate(text: str) -> Optional[str]:
    lowered = text.lower()
    if "at least" in lowered or "greater than" in lowered or "above" in lowered:
        return "above"
    if "at most" in lowered or "less than" in lowered or "below" in lowered:
        return "below"
    if "exactly" in lowered or "equal to" in lowered:
        return "equal"
    return None


def _extract_threshold(text: str) -> tuple[Optional[float], Optional[str]]:
    pattern = re.compile(
        r"(\d+(?:\.\d+)?)\s*(percent|%|bps|basis points|points|degrees|deg|degf|degc|f|c|usd|dollars|\$)",
        re.IGNORECASE,
    )
    match = pattern.search(text)
    if not match:
        return None, None
    value = float(match.group(1))
    unit = match.group(2).lower()
    return value, unit


def _extract_measurement_time(text: str) -> tuple[Optional[str], Optional[str]]:
    match = re.search(r"(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?)", text)
    if not match:
        return None, None
    return match.group(1), _extract_timezone(text)


def _extract_timezone(text: str) -> Optional[str]:
    tz_match = re.search(r"\b(UTC|ET|EST|EDT|CST|CDT|PST|PDT)\b", text)
    if tz_match:
        return tz_match.group(1)
    return None


def _extract_void_conditions(text: str) -> List[str]:
    voids = []
    lowered = text.lower()
    if "void" in lowered:
        voids.append("void mentioned")
    if "insufficient" in lowered and "data" in lowered:
        voids.append("insufficient data")
    return voids


def _llm_extract_spec(title: str, rules_text: str) -> Optional[dict]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    prompt = {
        "title": title,
        "rules_text": rules_text,
        "instructions": (
            "Extract a JSON object with fields: event_type, entities, predicate, "
            "threshold, threshold_units, measurement_time, timezone, void_conditions. "
            "Use null for unknown."
        ),
    }
    try:
        resp = requests.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "input": prompt,
                "temperature": 0,
                "response_format": {"type": "json_object"},
            },
            timeout=20,
        )
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
        content = data.get("output")[0].get("content")[0].get("text")
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except (ValueError, KeyError, IndexError, TypeError):
        return None
    return None
