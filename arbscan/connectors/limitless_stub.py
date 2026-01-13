import asyncio
import json
import logging
from pathlib import Path
from typing import Iterable

from arbscan.config import AppConfig
from arbscan.connectors.base import BaseConnector
from arbscan.connectors.limitless_adapter import (
    HttpLimitlessAdapter,
    LimitlessAdapter,
    OnChainLimitlessAdapter,
)
from arbscan.models import VenueMarketRef

logger = logging.getLogger(__name__)


class LimitlessStubConnector(BaseConnector):
    """
    TODO:
    - Identify on-chain contracts or official API for Limitless markets
    - Implement indexing of order books or quote sources
    - Add authentication and rate limits once official endpoints exist
    """

    def __init__(self, config: AppConfig, on_update, refs: Iterable[VenueMarketRef]) -> None:
        super().__init__("limitless", on_update)
        self.config = config
        self.refs = list(refs)
        self._mock_path = Path(__file__).resolve().parent.parent / "data" / "mock_limitless.json"
        self._adapter = self._build_adapter()

    async def run(self) -> None:
        if self.config.mock_mode:
            await self._run_mock()
            return

        if not self.refs:
            logger.warning("No Limitless refs provided; skipping")
            return

        if not self._adapter:
            logger.info(
                "Limitless adapter is disabled; set LIMITLESS_ADAPTER to 'http' or 'onchain' to enable."
            )
            return

        await self._run_live()

    async def _run_mock(self) -> None:
        logger.info("Limitless mock mode: loading %s", self._mock_path)
        data = json.loads(self._mock_path.read_text(encoding="utf-8"))
        for item in data.get("quotes", []):
            await self.emit(
                market_id=str(item["market_id"]),
                outcome=str(item["outcome"]),
                best_bid=float(item["best_bid"]),
                best_ask=float(item["best_ask"]),
                bid_size=float(item["bid_size"]),
                ask_size=float(item["ask_size"]),
            )

    def _build_adapter(self):
        adapter = self.config.limitless_adapter.lower()
        if adapter == "http":
            return HttpLimitlessAdapter(
                base_url=self.config.limitless_http_url,
                path_template=self.config.limitless_http_path_template,
                timeout_seconds=self.config.limitless_timeout_seconds,
            )
        if adapter == "onchain":
            return OnChainLimitlessAdapter()
        if adapter == "stub":
            return None
        raise ValueError(f"Unknown LIMITLESS_ADAPTER: {self.config.limitless_adapter}")

    async def _run_live(self) -> None:
        if not self._adapter:
            raise RuntimeError(
                "Limitless adapter is disabled. Set LIMITLESS_ADAPTER to 'http' or 'onchain'."
            )
        while True:
            quotes = await self._adapter.fetch_quotes(self.refs)
            for quote in quotes:
                await self.emit(
                    market_id=quote.market_id,
                    outcome=quote.outcome,
                    best_bid=quote.best_bid,
                    best_ask=quote.best_ask,
                    bid_size=quote.bid_size,
                    ask_size=quote.ask_size,
                )
            await asyncio.sleep(self.config.scan_interval_seconds)
