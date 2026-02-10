import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw


def _env_first(names: list[str], default: str = "") -> str:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip():
            return raw
    return default


@dataclass
class Config:
    # Core
    db_path: str = _env_str("ARBV2_DB_PATH", "arbv2.db")
    http_timeout_seconds: float = _env_float("ARBV2_HTTP_TIMEOUT", 20.0)

    # Ingest / matching
    ingest_limit: int = _env_int("ARBV2_INGEST_LIMIT", 0)
    max_event_time_delta_hours: int = _env_int("ARBV2_EVENT_DELTA_HOURS", 24)
    subset_matching_enabled: bool = _env_bool("ARBV2_SUBSET_MATCHING", True)

    # Polymarket ingest
    polymarket_base_url: str = _env_str("POLYMARKET_BASE_URL", "https://gamma-api.polymarket.com")
    polymarket_games_tag_id: str = _env_str("ARBV2_POLY_GAMES_TAG_ID", "100639")
    polymarket_series_ids: str = _env_str("ARBV2_POLY_SERIES_IDS", "")
    polymarket_series_id_overrides: str = _env_str("ARBV2_POLY_SERIES_ID_OVERRIDES", "10500=38")

    # Kalshi
    kalshi_base_url: str = _env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
    kalshi_ws_url: str = _env_str("KALSHI_WS_URL", "wss://api.elections.kalshi.com/")
    kalshi_api_key: str = _env_first(["KALSHI_API_KEY", "KALSHI_ACCESS_KEY"], "")
    kalshi_private_key_path: str = _env_str("KALSHI_PRIVATE_KEY_PATH", "")

    # Polymarket CLOB
    polymarket_clob_rest_url: str = _env_str("POLY_CLOB_REST_URL", "https://clob.polymarket.com")
    polymarket_clob_ws_url: str = _env_str("POLY_CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com")
    polymarket_clob_api_key: str = _env_str("POLY_CLOB_API_KEY", "")
    polymarket_clob_api_secret: str = _env_str("POLY_CLOB_API_SECRET", "")
    polymarket_clob_api_passphrase: str = _env_str("POLY_CLOB_API_PASSPHRASE", "")


def load_config() -> Config:
    return Config()
