import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Iterable, Optional

import requests
import websockets

from arbscan.config import AppConfig
from arbscan.connectors.base import BaseConnector
from arbscan.models import VenueMarketRef

logger = logging.getLogger(__name__)


class PolymarketConnector(BaseConnector):
    def __init__(self, config: AppConfig, on_update, refs: Iterable[VenueMarketRef]) -> None:
        super().__init__("polymarket", on_update)
        self.config = config
        self.refs = list(refs)
        self._mock_path = Path(__file__).resolve().parent.parent / "data" / "mock_polymarket.json"
        self._token_outcome: Dict[str, str] = {}
        self._last_sequence: Dict[str, int] = {}

    async def run(self) -> None:
        if self.config.mock_mode:
            await self._run_mock()
            return

        self._prepare_token_map()
        await self._load_metadata()
        await self._run_live()

    async def _run_mock(self) -> None:
        logger.info("Polymarket mock mode: loading %s", self._mock_path)
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

    def _prepare_token_map(self) -> None:
        for ref in self.refs:
            if ref.yes_id:
                self._token_outcome[ref.yes_id] = "YES"
            if ref.no_id:
                self._token_outcome[ref.no_id] = "NO"

    async def _load_metadata(self) -> None:
        if not self.refs:
            logger.warning("No Polymarket refs provided; skipping metadata fetch")
            return

        unique_markets = {ref.market_id for ref in self.refs if ref.market_id}
        if not unique_markets:
            return

        def _fetch() -> None:
            for market_id in unique_markets:
                url = f"{self.config.polymarket_rest_url}/markets/{market_id}"
                try:
                    resp = requests.get(url, timeout=10)
                    if resp.status_code != 200:
                        logger.warning(
                            "Polymarket metadata fetch failed %s %s", url, resp.status_code
                        )
                        continue
                    logger.info("Polymarket metadata loaded for %s", market_id)
                except requests.RequestException as exc:
                    logger.warning("Polymarket metadata fetch error for %s: %s", market_id, exc)

        await asyncio.to_thread(_fetch)

    async def _run_live(self) -> None:
        markets = self._subscription_markets()
        if not markets:
            logger.warning("No Polymarket markets to subscribe to")
            return

        backoff = 1
        while True:
            try:
                ws_url = await self._connect_ws()
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    await ws.send(
                        json.dumps(
                            {
                                "type": "subscribe",
                                "channel": "market",
                                "markets": markets,
                            }
                        )
                    )
                    logger.info("Polymarket WS subscribed to %d markets", len(markets))
                    backoff = 1
                    async for message in ws:
                        await self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Polymarket WS error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _connect_ws(self) -> str:
        candidates = self._candidate_ws_urls()
        last_error: Optional[Exception] = None
        for url in candidates:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20):
                    logger.info("Polymarket WS reachable at %s", url)
                return url
            except Exception as exc:
                last_error = exc
                logger.warning("Polymarket WS connect failed for %s: %s", url, exc)
                continue
        if last_error:
            raise last_error
        return self.config.polymarket_ws_url

    def _candidate_ws_urls(self) -> list[str]:
        base = self.config.polymarket_ws_url.rstrip("/")
        candidates = [base]
        if not base.endswith("/ws"):
            candidates.append(f"{base}/ws")
        if not base.endswith("/market"):
            candidates.append(f"{base}/market")
        return list(dict.fromkeys(candidates))

    def _subscription_markets(self) -> list[str]:
        markets: list[str] = []
        for ref in self.refs:
            if ref.yes_id:
                markets.append(ref.yes_id)
            if ref.no_id:
                markets.append(ref.no_id)
            if not ref.yes_id and not ref.no_id and ref.market_id:
                markets.append(ref.market_id)
        return list(dict.fromkeys(markets))

    async def _handle_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logger.debug("Polymarket WS non-JSON message: %s", message)
            return

        data = payload.get("data", payload)
        market_id = self._extract_market_id(data)
        if not market_id:
            return

        if not self._accept_sequence(data, market_id):
            return

        outcome = self._extract_outcome(data, market_id)
        if not outcome:
            logger.debug("Polymarket WS message missing outcome for %s", market_id)
            return

        book = self._extract_book(data)
        if not book:
            return

        best_bid, bid_size = book["bid"]
        best_ask, ask_size = book["ask"]
        await self.emit(
            market_id=market_id,
            outcome=outcome,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_size=bid_size,
            ask_size=ask_size,
        )

    def _extract_market_id(self, data: dict) -> Optional[str]:
        for key in ("market", "market_id", "token_id", "id"):
            value = data.get(key)
            if value:
                return str(value)
        return None

    def _extract_outcome(self, data: dict, market_id: str) -> Optional[str]:
        outcome = data.get("outcome")
        if outcome:
            return str(outcome).upper()
        return self._token_outcome.get(market_id)

    def _accept_sequence(self, data: dict, market_id: str) -> bool:
        sequence = data.get("sequence") or data.get("seq")
        if sequence is None:
            return True
        try:
            sequence_int = int(sequence)
        except (TypeError, ValueError):
            return True
        last = self._last_sequence.get(market_id)
        if last is not None and sequence_int <= last:
            return False
        if last is not None and sequence_int != last + 1:
            logger.warning(
                "Polymarket sequence gap for %s: %s -> %s", market_id, last, sequence_int
            )
        self._last_sequence[market_id] = sequence_int
        return True

    def _extract_book(self, data: dict) -> Optional[dict]:
        bids = data.get("bids") or data.get("bid")
        asks = data.get("asks") or data.get("ask")

        best_bid = self._best_from_entries(bids, side="bid")
        best_ask = self._best_from_entries(asks, side="ask")
        if best_bid is None or best_ask is None:
            best_bid_val = data.get("best_bid") or data.get("bestBid")
            best_ask_val = data.get("best_ask") or data.get("bestAsk")
            bid_size = data.get("best_bid_size") or data.get("bestBidSize")
            ask_size = data.get("best_ask_size") or data.get("bestAskSize")
            if best_bid_val is None or best_ask_val is None:
                return None
            return {
                "bid": (float(best_bid_val), float(bid_size or 0)),
                "ask": (float(best_ask_val), float(ask_size or 0)),
            }
        return {"bid": best_bid, "ask": best_ask}

    def _best_from_entries(self, entries, side: str) -> Optional[tuple[float, float]]:
        if not entries:
            return None
        if isinstance(entries, list) and entries:
            first = entries[0]
            if isinstance(first, dict):
                price = first.get("price")
                size = first.get("size") or first.get("amount") or 0
                if price is None:
                    return None
                return float(price), float(size)
            if isinstance(first, (list, tuple)) and len(first) >= 2:
                return float(first[0]), float(first[1])
        if isinstance(entries, dict):
            price = entries.get("price")
            size = entries.get("size") or entries.get("amount") or 0
            if price is None:
                return None
            return float(price), float(size)
        return None
