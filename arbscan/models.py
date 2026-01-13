from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class EventDomain:
    RATE_DELTA = "RATE_DELTA"
    PRICE_THRESHOLD = "PRICE_THRESHOLD"
    ELECTION_WINNER = "ELECTION_WINNER"
    GENERIC_BINARY = "GENERIC_BINARY"


@dataclass(frozen=True)
class Predicate:
    variable: str
    operator: str
    value: int
    unit: str
    reference: str


@dataclass(frozen=True)
class MarketQuote:
    best_bid: float
    best_ask: float
    bid_size: float
    ask_size: float
    timestamp: datetime


@dataclass(frozen=True)
class VenueMarketRef:
    venue: str
    market_id: str
    yes_id: Optional[str] = None
    no_id: Optional[str] = None


@dataclass(frozen=True)
class MappedEvent:
    canonical_event_id: str
    refs: list[VenueMarketRef]
    notes: Optional[str] = None


@dataclass(frozen=True)
class ArbSignal:
    canonical_event_id: str
    yes_ref: VenueMarketRef
    no_ref: VenueMarketRef
    yes_price: float
    no_price: float
    yes_size: float
    no_size: float
    executable_size: float
    margin_abs: float
    margin_pct: float
    total_cost: float
    fees: dict[str, float]
    slippage: dict[str, float]
    timestamp: datetime
    alert_id: str
    outcome_mapping: dict[str, str]


@dataclass(frozen=True)
class ResolutionSpec:
    venue: str
    market_id: str
    title: str
    close_time: Optional[datetime]
    end_time: Optional[datetime]
    resolution_source: Optional[str]
    rules_text: str
    is_binary: Optional[bool]
    event_type: Optional[str]
    entities: list[str]
    predicate: Optional[str]
    threshold: Optional[float]
    threshold_units: Optional[str]
    measurement_time: Optional[str]
    timezone: Optional[str]
    void_conditions: list[str]


@dataclass(frozen=True)
class MatchCandidate:
    kalshi_ticker: str
    polymarket_id: str
    score: float
    hard_fail_reasons: list[str]
    soft_reasons: list[str]


@dataclass(frozen=True)
class OutcomeMarket:
    outcome_key: str
    display_label: str
    market_id: str
    rules_text: str
    event_domain: str
    canonical_predicate: Optional[Predicate]
    yes_token_id: Optional[str] = None
    no_token_id: Optional[str] = None


@dataclass(frozen=True)
class EventGroup:
    venue: str
    event_id: str
    title: str
    close_time: Optional[datetime]
    rules_text: str
    event_domain: str
    outcomes: list[OutcomeMarket]


@dataclass(frozen=True)
class OutcomePair:
    kalshi_market_ticker: str
    polymarket_market_id: str
    outcome_key: str
    mapping_confidence: float
    mapping_reason: str
