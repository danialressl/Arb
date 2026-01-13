from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from arbscan.config import AppConfig
from arbscan.mapping import load_mappings
from arbscan.http_client import get_json

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationError:
    message: str


def validate_mappings(config: AppConfig, mappings_path: str) -> List[ValidationError]:
    mapping_file = load_mappings(mappings_path)
    errors: List[ValidationError] = []

    for event in mapping_file.events:
        for ref in event.refs:
            if ref.venue == "kalshi":
                errors.extend(_validate_kalshi_market(config, ref.market_id))
            elif ref.venue == "polymarket":
                errors.extend(_validate_polymarket_market(config, ref.market_id, ref.yes_id, ref.no_id))

    return errors


def _validate_kalshi_market(config: AppConfig, ticker: str) -> List[ValidationError]:
    url = f"{config.kalshi_base_url}/markets/{ticker}"
    data, status = get_json(url, timeout=config.kalshi_timeout_seconds)
    if status == 404:
        return [ValidationError(message=f"Kalshi ticker not found: {ticker}")]
    if status != 200:
        return [ValidationError(message=f"Kalshi request failed {ticker}: {status}")]

    return []


def _validate_polymarket_market(
    config: AppConfig, market_id: str, yes_token_id: Optional[str], no_token_id: Optional[str]
) -> List[ValidationError]:
    url = f"{config.polymarket_rest_url}/markets/{market_id}"
    data, status = get_json(url, timeout=10)
    if status == 404:
        return [ValidationError(message=f"Polymarket market not found: {market_id}")]
    if status != 200:
        return [ValidationError(message=f"Polymarket request failed {market_id}: {status}")]

    tokens = data.get("tokens") or data.get("outcomes") or data.get("marketTokens")
    if not tokens:
        clob_tokens = data.get("clobTokenIds")
        if not clob_tokens:
            return [
                ValidationError(
                    message=(
                        f"Polymarket market {market_id} missing tokens info; cannot validate yes/no token IDs"
                    )
                )
            ]
        tokens = _normalize_clob_tokens(clob_tokens)

    found_yes = _token_in_list(tokens, yes_token_id, "yes")
    found_no = _token_in_list(tokens, no_token_id, "no")

    errors: List[ValidationError] = []
    if yes_token_id and not found_yes:
        errors.append(
            ValidationError(
                message=f"Polymarket yes_token_id not found for {market_id}: {yes_token_id}"
            )
        )
    if no_token_id and not found_no:
        errors.append(
            ValidationError(
                message=f"Polymarket no_token_id not found for {market_id}: {no_token_id}"
            )
        )
    if not yes_token_id or not no_token_id:
        errors.append(
            ValidationError(
                message=f"Polymarket mapping missing yes_token_id or no_token_id for {market_id}"
            )
        )

    return errors


def _token_in_list(tokens, token_id: Optional[str], outcome_label: str) -> bool:
    if not token_id:
        return False
    for item in tokens:
        if isinstance(item, dict):
            token_value = item.get("token_id") or item.get("tokenId") or item.get("id")
            outcome = item.get("outcome") or item.get("label") or item.get("name")
            if token_value and str(token_value) == str(token_id):
                if outcome is None:
                    return True
                return str(outcome).lower() == outcome_label
    return False


def _normalize_clob_tokens(clob_tokens) -> list[dict]:
    if isinstance(clob_tokens, dict):
        return [
            {"token_id": clob_tokens.get("YES"), "outcome": "YES"},
            {"token_id": clob_tokens.get("NO"), "outcome": "NO"},
        ]
    if isinstance(clob_tokens, list) and len(clob_tokens) >= 2:
        return [
            {"token_id": clob_tokens[0], "outcome": "YES"},
            {"token_id": clob_tokens[1], "outcome": "NO"},
        ]
    return []
