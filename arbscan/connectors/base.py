from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable, Optional

from arbscan.models import MarketQuote


@dataclass(frozen=True)
class QuoteUpdate:
    venue: str
    market_id: str
    outcome: str
    quote: MarketQuote


QuoteCallback = Callable[[QuoteUpdate], Awaitable[None]]


class BaseConnector:
    def __init__(self, venue: str, on_update: QuoteCallback) -> None:
        self.venue = venue
        self.on_update = on_update
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def run(self) -> None:
        raise NotImplementedError

    async def emit(
        self,
        market_id: str,
        outcome: str,
        best_bid: float,
        best_ask: float,
        bid_size: float,
        ask_size: float,
        timestamp: Optional[datetime] = None,
    ) -> None:
        quote = MarketQuote(
            best_bid=best_bid,
            best_ask=best_ask,
            bid_size=bid_size,
            ask_size=ask_size,
            timestamp=timestamp or datetime.utcnow(),
        )
        await self.on_update(
            QuoteUpdate(
                venue=self.venue,
                market_id=market_id,
                outcome=outcome,
                quote=quote,
            )
        )
