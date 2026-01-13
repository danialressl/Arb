from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional

import requests

from arbscan.models import VenueMarketRef

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LimitlessQuote:
    market_id: str
    outcome: str
    best_bid: float
    best_ask: float
    bid_size: float
    ask_size: float


class LimitlessAdapter:
    async def fetch_quotes(self, refs: Iterable[VenueMarketRef]) -> List[LimitlessQuote]:
        raise NotImplementedError


class HttpLimitlessAdapter(LimitlessAdapter):
    def __init__(self, base_url: Optional[str], path_template: str, timeout_seconds: float) -> None:
        self.base_url = base_url
        self.path_template = path_template
        self.timeout_seconds = timeout_seconds

    async def fetch_quotes(self, refs: Iterable[VenueMarketRef]) -> List[LimitlessQuote]:
        if not self.base_url:
            raise RuntimeError(
                "Limitless HTTP adapter requires LIMITLESS_HTTP_URL. "
                "Provide the official endpoint if available."
            )

        def _fetch() -> List[LimitlessQuote]:
            quotes: List[LimitlessQuote] = []
            for ref in refs:
                url = self.base_url.rstrip("/") + self.path_template.format(market_id=ref.market_id)
                try:
                    resp = requests.get(url, timeout=self.timeout_seconds)
                except requests.RequestException as exc:
                    logger.warning("Limitless HTTP fetch error for %s: %s", ref.market_id, exc)
                    continue
                if resp.status_code != 200:
                    logger.warning("Limitless HTTP fetch failed %s %s", url, resp.status_code)
                    continue
                data = resp.json()
                for item in data.get("quotes", []):
                    quotes.append(
                        LimitlessQuote(
                            market_id=str(item["market_id"]),
                            outcome=str(item["outcome"]).upper(),
                            best_bid=float(item["best_bid"]),
                            best_ask=float(item["best_ask"]),
                            bid_size=float(item["bid_size"]),
                            ask_size=float(item["ask_size"]),
                        )
                    )
            return quotes

        import asyncio

        return await asyncio.to_thread(_fetch)


class OnChainLimitlessAdapter(LimitlessAdapter):
    async def fetch_quotes(self, refs: Iterable[VenueMarketRef]) -> List[LimitlessQuote]:
        raise RuntimeError(
            "Limitless on-chain adapter needs contract addresses and/or an indexer. "
            "Provide contract metadata and a log/indexing strategy before enabling."
        )
