import os
from dataclasses import dataclass
def _env_truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Config:
    kalshi_base_url: str = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
    polymarket_base_url: str = os.getenv("POLYMARKET_BASE_URL", "https://gamma-api.polymarket.com")
    db_path: str = os.getenv("ARBV2_DB_PATH", "arbv2.db")
    http_timeout_seconds: float = float(os.getenv("ARBV2_HTTP_TIMEOUT", "20"))
    ingest_limit: int = int(os.getenv("ARBV2_INGEST_LIMIT", "0"))
    sports_tag: str = os.getenv("ARBV2_SPORTS_TAG", "sports")
    games_tag: str = os.getenv("ARBV2_GAMES_TAG", "games")
    max_event_time_delta_hours: int = int(os.getenv("ARBV2_EVENT_DELTA_HOURS", "24"))
    polymarket_games_tag_id: str = os.getenv("ARBV2_POLY_GAMES_TAG_ID", "100639")
    polymarket_series_ids: str = os.getenv("ARBV2_POLY_SERIES_IDS", "")
    subset_matching_enabled: bool = _env_truthy(os.getenv("ARBV2_SUBSET_MATCHING", "true"))
    polymarket_clob_rest_url: str = os.getenv("POLY_CLOB_REST_URL", "https://clob.polymarket.com")
    polymarket_clob_ws_url: str = os.getenv("POLY_CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com")
    polymarket_clob_api_key: str = os.getenv("POLY_CLOB_API_KEY", "")
    polymarket_clob_api_secret: str = os.getenv("POLY_CLOB_API_SECRET", "")
    polymarket_clob_api_passphrase: str = os.getenv("POLY_CLOB_API_PASSPHRASE", "")


def load_config() -> Config:
    return Config()
