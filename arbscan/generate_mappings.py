from __future__ import annotations

import json
from typing import List, Optional

import yaml


def generate_mappings(kalshi_path: Optional[str], polymarket_path: Optional[str], out_path: str) -> None:
    events: List[dict] = []

    if kalshi_path:
        with open(kalshi_path, "r", encoding="utf-8") as handle:
            kalshi_markets = json.load(handle)
        for item in kalshi_markets:
            ticker = item.get("ticker") or item.get("market_ticker") or item.get("id") or ""
            title = item.get("title") or item.get("question") or ""
            if not ticker:
                continue
            events.append(
                {
                    "canonical_event_id": f"kalshi-{ticker}",
                    "notes": f"Add Polymarket mapping for: {title}",
                    "venues": {"kalshi": {"ticker": ticker}},
                }
            )

    if polymarket_path:
        with open(polymarket_path, "r", encoding="utf-8") as handle:
            poly_markets = json.load(handle)
        for item in poly_markets:
            market_id = str(item.get("id") or item.get("market_id") or item.get("condition_id") or "")
            question = item.get("question") or item.get("title") or ""
            yes_token, no_token = _extract_tokens(item)
            if not market_id:
                continue
            events.append(
                {
                    "canonical_event_id": f"poly-{market_id}",
                    "notes": f"Add Kalshi mapping for: {question}",
                    "venues": {
                        "polymarket": {
                            "market_id": market_id,
                            "yes_token_id": yes_token,
                            "no_token_id": no_token,
                        }
                    },
                }
            )

    with open(out_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump({"events": events}, handle, sort_keys=False)


def _extract_tokens(item) -> tuple[str, str]:
    yes_token = ""
    no_token = ""
    tokens = item.get("tokens") or []
    for token in tokens:
        outcome = str(token.get("outcome") or token.get("name") or "").lower()
        token_id = str(token.get("token_id") or token.get("id") or "")
        if outcome == "yes":
            yes_token = token_id
        elif outcome == "no":
            no_token = token_id
    return yes_token, no_token
