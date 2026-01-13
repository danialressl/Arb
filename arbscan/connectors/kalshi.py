import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Iterable, Optional

import base64
import requests

from arbscan.config import AppConfig
from arbscan.connectors.base import BaseConnector
from arbscan.models import VenueMarketRef

logger = logging.getLogger(__name__)


class KalshiConnector(BaseConnector):
    def __init__(self, config: AppConfig, on_update, refs: Iterable[VenueMarketRef]) -> None:
        super().__init__("kalshi", on_update)
        self.config = config
        self.refs = list(refs)
        self._mock_path = Path(__file__).resolve().parent.parent / "data" / "mock_kalshi.json"
        self._backoff: dict[str, float] = {}
        self._next_allowed: dict[str, float] = {}

    async def run(self) -> None:
        if self.config.mock_mode:
            await self._run_mock()
            return

        if not self.refs:
            logger.warning("No Kalshi refs provided; skipping polling")
            return

        await self._run_live()

    async def _run_mock(self) -> None:
        logger.info("Kalshi mock mode: loading %s", self._mock_path)
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

    async def _run_live(self) -> None:
        rate_interval = 1.0 / max(self.config.kalshi_rate_limit_per_second, 0.1)
        while True:
            start = time.monotonic()
            for ref in self.refs:
                now = time.monotonic()
                if now < self._next_allowed.get(ref.market_id, 0):
                    continue
                await self._poll_market(ref)
                await asyncio.sleep(rate_interval)

            elapsed = time.monotonic() - start
            await asyncio.sleep(max(self.config.scan_interval_seconds - elapsed, 0))

    async def _poll_market(self, ref: VenueMarketRef) -> None:
        data, status = await self.get_orderbook(ref.market_id)
        if not data:
            self._register_failure(ref.market_id, status)
            return
        self._register_success(ref.market_id)

        quotes = self.compute_normalized_quotes(data)
        if quotes.get("yes_ask") is not None:
            await self.emit(
                market_id=ref.market_id,
                outcome="YES",
                best_bid=quotes.get("yes_bid", 0.0),
                best_ask=quotes["yes_ask"],
                bid_size=quotes.get("yes_bid_size", 0.0),
                ask_size=quotes.get("yes_ask_size", 0.0),
            )
        if quotes.get("no_ask") is not None:
            await self.emit(
                market_id=ref.market_id,
                outcome="NO",
                best_bid=quotes.get("no_bid", 0.0),
                best_ask=quotes["no_ask"],
                bid_size=quotes.get("no_bid_size", 0.0),
                ask_size=quotes.get("no_ask_size", 0.0),
            )

    async def list_markets(self, params: dict) -> tuple[Optional[dict], Optional[int]]:
        url = f"{self.config.kalshi_base_url}/markets"
        return await self._get(url, "/markets", params=params)

    async def get_market(self, ticker: str) -> tuple[Optional[dict], Optional[int]]:
        url = f"{self.config.kalshi_base_url}/markets/{ticker}"
        return await self._get(url, f"/markets/{ticker}")

    async def get_orderbook(self, ticker: str) -> tuple[Optional[dict], Optional[int]]:
        url = f"{self.config.kalshi_base_url}/markets/{ticker}/orderbook"
        return await self._get(url, f"/markets/{ticker}/orderbook")

    async def _get(
        self, url: str, path: str, params: Optional[dict] = None
    ) -> tuple[Optional[dict], Optional[int]]:
        def _fetch():
            headers = self._build_headers("GET", path)
            try:
                resp = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.config.kalshi_timeout_seconds,
                )
                if resp.status_code != 200:
                    if resp.status_code == 401 and self.config.kalshi_signing_mode.lower() == "none":
                        logger.warning(
                            "Kalshi endpoint requires auth. Set KALSHI_SIGNING_MODE=ed25519, "
                            "KALSHI_API_KEY, and KALSHI_PRIVATE_KEY_PATH."
                        )
                    logger.warning("Kalshi fetch failed %s %s", url, resp.status_code)
                    return None, resp.status_code
                return resp.json(), 200
            except requests.RequestException as exc:
                logger.warning("Kalshi fetch error for %s: %s", url, exc)
                return None, None

        result = await asyncio.to_thread(_fetch)
        return result if isinstance(result, tuple) else (result, None)

    def compute_normalized_quotes(self, data: dict) -> dict:
        book = data.get("orderbook") or data
        yes_bids = self._extract_bids(book, "yes")
        no_bids = self._extract_bids(book, "no")
        yes_asks = self._extract_asks(book, "yes")
        no_asks = self._extract_asks(book, "no")

        yes_bid = self._best_price(yes_bids)
        no_bid = self._best_price(no_bids)

        yes_ask = self._best_ask_price(yes_asks)
        no_ask = self._best_ask_price(no_asks)

        # Kalshi orderbooks are bid-centric; derive implied asks from opposite bids when needed.
        if yes_ask is None:
            yes_ask = self._implied_ask(no_bid)
        if no_ask is None:
            no_ask = self._implied_ask(yes_bid)

        return {
            "yes_bid": yes_bid,
            "yes_bid_size": self._best_size(yes_bids),
            "yes_ask": yes_ask,
            "yes_ask_size": self._best_ask_size(yes_asks)
            if yes_asks
            else (self._best_size(no_bids) if yes_ask is not None else None),
            "no_bid": no_bid,
            "no_bid_size": self._best_size(no_bids),
            "no_ask": no_ask,
            "no_ask_size": self._best_ask_size(no_asks)
            if no_asks
            else (self._best_size(yes_bids) if no_ask is not None else None),
        }

    def _extract_bids(self, book: dict, side: str) -> list[tuple[float, float]]:
        side_book = book.get(side) or {}
        bids = side_book.get("bids") or side_book.get("bid") or side_book.get("buy") or []
        return self._normalize_levels(bids)

    def _extract_asks(self, book: dict, side: str) -> list[tuple[float, float]]:
        side_book = book.get(side) or {}
        asks = side_book.get("asks") or side_book.get("ask") or side_book.get("sell") or []
        return self._normalize_levels(asks)

    def _normalize_levels(self, levels) -> list[tuple[float, float]]:
        normalized = []
        if isinstance(levels, list):
            for entry in levels:
                if isinstance(entry, dict):
                    price = entry.get("price")
                    size = entry.get("size") or entry.get("quantity") or entry.get("amount")
                    if price is None:
                        continue
                    normalized.append((self._normalize_price(price), float(size or 0)))
                elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                    normalized.append((self._normalize_price(entry[0]), float(entry[1])))
        return normalized

    def _best_price(self, levels: list[tuple[float, float]]) -> Optional[float]:
        if not levels:
            return None
        return max(level[0] for level in levels)

    def _best_ask_price(self, levels: list[tuple[float, float]]) -> Optional[float]:
        if not levels:
            return None
        return min(level[0] for level in levels)

    def _best_size(self, levels: list[tuple[float, float]]) -> float:
        if not levels:
            return 0.0
        best_price = self._best_price(levels)
        for price, size in levels:
            if price == best_price:
                return size
        return 0.0

    def _best_ask_size(self, levels: list[tuple[float, float]]) -> float:
        if not levels:
            return 0.0
        best_price = self._best_ask_price(levels)
        for price, size in levels:
            if price == best_price:
                return size
        return 0.0

    def _implied_ask(self, opposite_bid: Optional[float]) -> Optional[float]:
        if opposite_bid is None:
            return None
        return max(0.0, min(1.0, 1.0 - opposite_bid))

    def _normalize_price(self, value) -> float:
        price = float(value)
        if price > 1.0:
            price = price / 100.0
        return price

    def _register_failure(self, market_id: str, status: int = None) -> None:
        if status and status < 500:
            delay = 5.0
        else:
            delay = min(self._backoff.get(market_id, 1.0) * 2, 60.0)
        self._backoff[market_id] = delay
        self._next_allowed[market_id] = time.monotonic() + delay

    def _register_success(self, market_id: str) -> None:
        self._backoff[market_id] = 1.0
        self._next_allowed[market_id] = time.monotonic()

    def _build_headers(self, method: str, path: str) -> dict:
        headers = {}
        if self.config.kalshi_api_key:
            headers["KALSHI-ACCESS-KEY"] = self.config.kalshi_api_key

        if self.config.kalshi_signing_mode.lower() != "none":
            if not self.config.kalshi_private_key_path:
                raise RuntimeError("KALSHI_PRIVATE_KEY_PATH is required for signed requests.")
            signature, timestamp = self._sign_request(method, path, body="")
            headers["KALSHI-ACCESS-SIGNATURE"] = signature
            headers["KALSHI-ACCESS-TIMESTAMP"] = timestamp

        return headers

    def _sign_request(self, method: str, path: str, body: str) -> tuple[str, str]:
        if self.config.kalshi_signing_mode.lower() != "ed25519":
            raise RuntimeError(
                "Unsupported KALSHI_SIGNING_MODE. Use 'none' or 'ed25519'."
            )
        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
            from cryptography.hazmat.primitives import serialization
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "cryptography is required for ed25519 signing. "
                "Install with: pip install cryptography"
            ) from exc

        with open(self.config.kalshi_private_key_path, "rb") as handle:
            key_data = handle.read()

        private_key = serialization.load_pem_private_key(key_data, password=None)
        if not isinstance(private_key, Ed25519PrivateKey):
            raise RuntimeError("Kalshi private key is not an Ed25519 key.")

        timestamp = str(int(time.time()))
        message = f"{timestamp}{method.upper()}{path}{body}".encode("utf-8")
        signature = private_key.sign(message)
        return base64.b64encode(signature).decode("ascii"), timestamp
