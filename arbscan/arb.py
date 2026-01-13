from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import product
from typing import Dict, Iterable, Optional, Tuple
from uuid import uuid4

from arbscan.models import ArbSignal, MarketQuote, MappedEvent, VenueMarketRef


@dataclass(frozen=True)
class ArbCosts:
    fee_yes: float
    fee_no: float
    slippage_yes: float
    slippage_no: float


def compute_arb_signals(
    event: MappedEvent,
    quotes: Dict[Tuple[str, str, str], MarketQuote],
    fees: Dict[str, float],
    slippages: Dict[str, float],
    threshold: float,
    now: Optional[datetime] = None,
) -> list[ArbSignal]:
    now = now or datetime.utcnow()
    refs = event.refs
    signals: list[ArbSignal] = []

    for yes_ref, no_ref in product(refs, refs):
        if yes_ref.venue == no_ref.venue and yes_ref.market_id == no_ref.market_id:
            continue
        yes_quote = _get_quote(quotes, yes_ref, "YES")
        no_quote = _get_quote(quotes, no_ref, "NO")
        if not yes_quote or not no_quote:
            continue
        costs = ArbCosts(
            fee_yes=fees.get(yes_ref.venue, 0.0),
            fee_no=fees.get(no_ref.venue, 0.0),
            slippage_yes=slippages.get(yes_ref.venue, 0.0),
            slippage_no=slippages.get(no_ref.venue, 0.0),
        )
        signal = _compute_signal(event, yes_ref, no_ref, yes_quote, no_quote, costs, now)
        if signal and signal.margin_abs >= threshold:
            signals.append(signal)

    return signals


def _get_quote(
    quotes: Dict[Tuple[str, str, str], MarketQuote],
    ref: VenueMarketRef,
    outcome: str,
) -> Optional[MarketQuote]:
    key = (ref.venue, ref.market_id, outcome)
    if key in quotes:
        return quotes[key]
    if outcome == "YES" and ref.yes_id:
        return quotes.get((ref.venue, ref.yes_id, outcome))
    if outcome == "NO" and ref.no_id:
        return quotes.get((ref.venue, ref.no_id, outcome))
    return None


def _compute_signal(
    event: MappedEvent,
    yes_ref: VenueMarketRef,
    no_ref: VenueMarketRef,
    yes_quote: MarketQuote,
    no_quote: MarketQuote,
    costs: ArbCosts,
    now: datetime,
) -> Optional[ArbSignal]:
    yes_price = yes_quote.best_ask
    no_price = no_quote.best_ask

    total_cost = yes_price + no_price + costs.fee_yes + costs.fee_no + costs.slippage_yes + costs.slippage_no

    margin_abs = 1.0 - total_cost
    if total_cost <= 0:
        return None
    margin_pct = margin_abs / total_cost
    executable_size = min(yes_quote.ask_size, no_quote.ask_size)

    return ArbSignal(
        canonical_event_id=event.canonical_event_id,
        yes_ref=yes_ref,
        no_ref=no_ref,
        yes_price=yes_price,
        no_price=no_price,
        yes_size=yes_quote.ask_size,
        no_size=no_quote.ask_size,
        executable_size=executable_size,
        margin_abs=margin_abs,
        margin_pct=margin_pct,
        total_cost=total_cost,
        fees={yes_ref.venue: costs.fee_yes, no_ref.venue: costs.fee_no},
        slippage={yes_ref.venue: costs.slippage_yes, no_ref.venue: costs.slippage_no},
        timestamp=now,
        alert_id=str(uuid4()),
        outcome_mapping={
            yes_ref.venue: "YES",
            no_ref.venue: "NO",
        },
    )
