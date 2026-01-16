from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Market:
    venue: str
    market_id: str
    event_id: str
    title: str
    event_title: str
    raw_json: Dict[str, Any]
    series_ticker: Optional[str] = None
    market_type: Optional[str] = None
    status: Optional[str] = None
    event_date: Optional[str] = None
    outcome_label: Optional[str] = None


@dataclass
class SportsPredicate:
    venue: str
    market_id: str
    event_id: str
    team_a: str
    team_b: str
    winner: str


@dataclass
class MatchResult:
    kalshi_market_id: str
    polymarket_market_id: str
    equivalent: bool
    reason: str
    kalshi_title: Optional[str] = None
    polymarket_title: Optional[str] = None


@dataclass
class PriceSnapshot:
    venue: str
    market_id: str
    outcome_label: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    last_trade: Optional[float]
    ts_utc: str
    raw_json: Optional[Dict[str, Any]] = None
