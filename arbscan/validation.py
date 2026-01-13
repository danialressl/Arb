from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Tuple

from arbscan.connectors.base import QuoteUpdate

logger = logging.getLogger(__name__)


@dataclass
class ValidationState:
    last_timestamp: datetime


class QuoteValidator:
    def __init__(self) -> None:
        self._state: Dict[Tuple[str, str, str], ValidationState] = {}

    def validate(self, update: QuoteUpdate) -> bool:
        quote = update.quote
        if not (0 <= quote.best_bid <= quote.best_ask <= 1.0):
            logger.warning(
                "Rejecting quote: invalid bid/ask range venue=%s market=%s outcome=%s bid=%s ask=%s",
                update.venue,
                update.market_id,
                update.outcome,
                quote.best_bid,
                quote.best_ask,
            )
            return False

        if quote.bid_size < 0 or quote.ask_size < 0:
            logger.warning(
                "Rejecting quote: invalid size venue=%s market=%s outcome=%s bid_size=%s ask_size=%s",
                update.venue,
                update.market_id,
                update.outcome,
                quote.bid_size,
                quote.ask_size,
            )
            return False

        key = (update.venue, update.market_id, update.outcome)
        state = self._state.get(key)
        if state and quote.timestamp < state.last_timestamp:
            logger.warning(
                "Rejecting quote: non-monotonic timestamp venue=%s market=%s outcome=%s ts=%s last=%s",
                update.venue,
                update.market_id,
                update.outcome,
                quote.timestamp.isoformat(),
                state.last_timestamp.isoformat(),
            )
            return False

        self._state[key] = ValidationState(last_timestamp=quote.timestamp)
        return True
