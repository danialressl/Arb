from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import yaml

from arbscan.models import MappedEvent, VenueMarketRef


@dataclass(frozen=True)
class MappingFile:
    events: list[MappedEvent]


def _parse_event(raw: dict) -> MappedEvent:
    refs = []
    venues = raw.get("venues", {})
    for venue, details in venues.items():
        if not isinstance(details, dict):
            raise ValueError(f"Invalid mapping for venue {venue}")
        refs.append(_parse_venue(venue, details))
    return MappedEvent(
        canonical_event_id=str(raw.get("canonical_event_id", "")),
        refs=refs,
        notes=_optional_str(raw.get("notes")),
    )


def _optional_str(value):
    if value is None:
        return None
    return str(value)


def _parse_venue(venue: str, details: dict) -> VenueMarketRef:
    if venue == "kalshi":
        ticker = details.get("ticker") or details.get("market_id")
        return VenueMarketRef(
            venue=venue,
            market_id=str(ticker or ""),
        )
    if venue == "polymarket":
        market_id = details.get("condition_id") or details.get("market_id")
        yes_id = details.get("yes_token_id") or details.get("yes_id")
        no_id = details.get("no_token_id") or details.get("no_id")
        return VenueMarketRef(
            venue=venue,
            market_id=str(market_id or ""),
            yes_id=_optional_str(yes_id),
            no_id=_optional_str(no_id),
        )
    return VenueMarketRef(
        venue=venue,
        market_id=str(details.get("market_id", "")),
        yes_id=_optional_str(details.get("yes_id")),
        no_id=_optional_str(details.get("no_id")),
    )


def load_mappings(path: str) -> MappingFile:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    if not isinstance(raw, dict) or "events" not in raw:
        raise ValueError("mappings.yml must have top-level 'events'")

    events = [_parse_event(item) for item in raw.get("events", [])]
    _validate_events(events)
    return MappingFile(events=events)


def _validate_events(events: Iterable[MappedEvent]) -> None:
    for event in events:
        if not event.canonical_event_id:
            raise ValueError("canonical_event_id is required")
        if not event.refs:
            raise ValueError(f"No venue refs for {event.canonical_event_id}")
        for ref in event.refs:
            if not ref.venue or not ref.market_id:
                raise ValueError(f"Invalid venue ref for {event.canonical_event_id}")
            if ref.venue == "polymarket" and (not ref.yes_id or not ref.no_id):
                raise ValueError(
                    f"Polymarket mapping requires yes_token_id and no_token_id for {event.canonical_event_id}"
                )
