import os
from dataclasses import dataclass


@dataclass
class Config:
    kalshi_base_url: str = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
    polymarket_base_url: str = os.getenv("POLYMARKET_BASE_URL", "https://gamma-api.polymarket.com")
    db_path: str = os.getenv("ARBV2_DB_PATH", "arbv2.db")
    http_timeout_seconds: float = float(os.getenv("ARBV2_HTTP_TIMEOUT", "20"))
    ingest_limit: int = int(os.getenv("ARBV2_INGEST_LIMIT", "1000"))
    sports_tag: str = os.getenv("ARBV2_SPORTS_TAG", "sports")
    games_tag: str = os.getenv("ARBV2_GAMES_TAG", "games")
    max_event_time_delta_hours: int = int(os.getenv("ARBV2_EVENT_DELTA_HOURS", "24"))
    polymarket_games_tag_id: str = os.getenv("ARBV2_POLY_GAMES_TAG_ID", "100639")
    polymarket_series_ids: str = os.getenv("ARBV2_POLY_SERIES_IDS", "")


def load_config() -> Config:
    return Config()
