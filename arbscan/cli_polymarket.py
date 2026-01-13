from __future__ import annotations

import json
import logging
from typing import Optional

from arbscan.config import AppConfig
from arbscan.http_client import get_json

logger = logging.getLogger(__name__)


def polymarket_search(config: AppConfig, query: str, limit: int, out_path: Optional[str]) -> None:
    url = f"{config.polymarket_gamma_url}/markets"
    params = {"search": query, "limit": limit}
    payload, status = get_json(url, params=params, timeout=10)
    if not payload or status is None:
        raise SystemExit("Polymarket request failed: no response")
    if status != 200:
        raise SystemExit(f"Polymarket request failed: {status}")
    markets = payload.get("markets") if isinstance(payload, dict) else payload
    if not isinstance(markets, list):
        markets = []
    _print_markets(markets)

    if out_path:
        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(markets, handle, indent=2)
        logger.info("Wrote %d markets to %s", len(markets), out_path)


def _print_markets(markets) -> None:
    header = f"{'market_id':18} {'end_time':25} {'yes_token':18} {'no_token':18} question"
    print(header)
    print("-" * len(header))
    for item in markets:
        market_id = str(item.get("id") or item.get("market_id") or item.get("condition_id") or "")
        question = item.get("question") or item.get("title") or ""
        end_time = item.get("end_date") or item.get("end_time") or item.get("close_time") or ""
        yes_token, no_token = _extract_tokens(item)
        print(f"{market_id:18} {end_time:25} {yes_token:18} {no_token:18} {question}")


def _extract_tokens(item) -> tuple[str, str]:
    yes_token = ""
    no_token = ""
    clob_tokens = item.get("clobTokenIds")
    if isinstance(clob_tokens, dict):
        yes_token = str(clob_tokens.get("YES") or clob_tokens.get("yes") or "")
        no_token = str(clob_tokens.get("NO") or clob_tokens.get("no") or "")
        if yes_token or no_token:
            return yes_token, no_token
    if isinstance(clob_tokens, list) and len(clob_tokens) >= 2:
        yes_token = str(clob_tokens[0])
        no_token = str(clob_tokens[1])
        return yes_token, no_token
    tokens = item.get("tokens") or []
    for token in tokens:
        outcome = str(token.get("outcome") or token.get("name") or "").lower()
        token_id = str(token.get("token_id") or token.get("id") or "")
        if outcome == "yes":
            yes_token = token_id
        elif outcome == "no":
            no_token = token_id
    return yes_token, no_token
