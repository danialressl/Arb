from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from arbscan.arb import compute_arb_signals
from arbscan.config import AppConfig
from arbscan.connectors import KalshiConnector, PolymarketConnector
from arbscan.connectors.base import QuoteUpdate
from arbscan.dedupe import DedupeCache
from arbscan.models import MappedEvent, VenueMarketRef
from arbscan.notify import notify
from arbscan.storage import (
    list_active_market_matches,
    record_signal,
    upsert_market_state,
    upsert_quote,
)
from arbscan.validation import QuoteValidator

logger = logging.getLogger(__name__)


def build_events_from_matches(config: AppConfig, domain: Optional[str]) -> List[MappedEvent]:
    matches = list_active_market_matches(config.db_path, domain=domain)
    events: List[MappedEvent] = []
    for match in matches:
        kalshi_market = match["kalshi_market_id"]
        poly_market = match["polymarket_market_id"]
        yes_token = match.get("yes_token_id") or ""
        no_token = match.get("no_token_id") or ""
        if not yes_token or not no_token:
            logger.warning("Missing Polymarket token IDs for %s", poly_market)
            continue
        refs = [
            VenueMarketRef(venue="kalshi", market_id=kalshi_market),
            VenueMarketRef(venue="polymarket", market_id=str(poly_market), yes_id=yes_token, no_id=no_token),
        ]
        events.append(
            MappedEvent(
                canonical_event_id=match["match_id"],
                refs=refs,
                notes=None,
            )
        )
    return events


async def run_live(config: AppConfig, sports_only: bool = False) -> None:
    if any(
        name in sys.modules
        for name in ("arbscan.event_matching", "arbscan.catalog", "arbscan.canonical")
    ):
        logger.error("Semantic code loaded in live mode. Exiting.")
        return
    domain = "SPORTS_WINNER" if sports_only else None
    events = build_events_from_matches(config, domain)
    if not events:
        logger.warning("No matched events to monitor. Run scan-catalog first.")
        return

    queue: asyncio.Queue[QuoteUpdate] = asyncio.Queue()
    quotes: Dict[Tuple[str, str, str], any] = {}
    quote_state: Dict[Tuple[str, str], dict] = {}
    dedupe = DedupeCache(config.dedupe_seconds, config.margin_step_resend, config.size_step_resend)
    validator = QuoteValidator()

    async def on_update(update: QuoteUpdate) -> None:
        if validator.validate(update):
            await queue.put(update)

    poly_refs = [ref for event in events for ref in event.refs if ref.venue == "polymarket"]
    kalshi_refs = [ref for event in events for ref in event.refs if ref.venue == "kalshi"]

    connectors = [PolymarketConnector(config, on_update, poly_refs)]
    if config.kalshi_enabled:
        connectors.append(KalshiConnector(config, on_update, kalshi_refs))

    async def connector_runner(connector) -> None:
        backoff = 1
        while True:
            try:
                await connector.run()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Connector %s failed: %s", connector.venue, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def process_updates() -> None:
        while True:
            update = await queue.get()
            key = (update.venue, update.market_id, update.outcome)
            quotes[key] = update.quote
            state_key = (update.venue, update.market_id)
            state = quote_state.get(state_key, {})
            if update.outcome == "YES":
                state.update(
                    {
                        "yes_bid": update.quote.best_bid,
                        "yes_ask": update.quote.best_ask,
                        "yes_bid_size": update.quote.bid_size,
                        "yes_ask_size": update.quote.ask_size,
                    }
                )
            elif update.outcome == "NO":
                state.update(
                    {
                        "no_bid": update.quote.best_bid,
                        "no_ask": update.quote.best_ask,
                        "no_bid_size": update.quote.bid_size,
                        "no_ask_size": update.quote.ask_size,
                    }
                )
            state["ts"] = update.quote.timestamp.isoformat()
            quote_state[state_key] = state
            upsert_quote(config.db_path, update.venue, update.market_id, state)
            upsert_market_state(
                config.db_path,
                update.venue,
                update.market_id,
                state.get("yes_bid"),
                state.get("yes_ask"),
                state.get("no_bid"),
                state.get("no_ask"),
                None,
                state.get("ts"),
            )

            for event in events:
                signals = compute_arb_signals(
                    event=event,
                    quotes=quotes,
                    fees={
                        "polymarket": config.fee_polymarket,
                        "kalshi": config.fee_kalshi,
                    },
                    slippages={
                        "polymarket": config.slippage_polymarket,
                        "kalshi": config.slippage_kalshi,
                    },
                    threshold=config.arb_threshold,
                    now=datetime.utcnow(),
                )
                for signal in signals:
                    if signal.executable_size < config.min_size:
                        continue
                    if dedupe.should_send(signal, signal.timestamp):
                        notify(signal, config.slack_webhook_url)
                        record_signal(
                            config.db_path,
                            event.canonical_event_id,
                            f"{signal.yes_ref.venue}->{signal.no_ref.venue}",
                            f"{signal.yes_ref.venue}:{signal.yes_ref.market_id}",
                            f"{signal.no_ref.venue}:{signal.no_ref.market_id}",
                            signal.margin_abs,
                            signal.executable_size,
                        )

    tasks = [asyncio.create_task(connector_runner(connector)) for connector in connectors]
    tasks.append(asyncio.create_task(process_updates()))

    await asyncio.gather(*tasks)
