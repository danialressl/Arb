import os
from dataclasses import dataclass
from typing import Optional

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional dependency fallback
    def load_dotenv() -> None:
        return None


@dataclass(frozen=True)
class AppConfig:
    scan_interval_seconds: float
    arb_threshold: float
    dedupe_seconds: int
    margin_step_resend: float
    size_step_resend: float
    min_size: float
    match_threshold: float
    include_review: bool
    polymarket_scan_limit: int
    polymarket_scan_page_size: int
    kalshi_scan_limit: int
    match_candidate_limit: int
    time_window_days: int
    min_review_score: float

    polymarket_l1_private_key: Optional[str]
    polymarket_l2_api_key: Optional[str]
    polymarket_l2_api_secret: Optional[str]
    polymarket_l2_api_passphrase: Optional[str]

    kalshi_api_key: Optional[str]
    kalshi_private_key_path: Optional[str]
    kalshi_enabled: bool

    slack_webhook_url: Optional[str]

    fee_polymarket: float
    fee_kalshi: float
    fee_limitless: float

    slippage_polymarket: float
    slippage_kalshi: float
    slippage_limitless: float

    polymarket_rest_url: str
    polymarket_ws_url: str
    polymarket_gamma_url: str
    kalshi_base_url: str
    kalshi_timeout_seconds: float
    kalshi_rate_limit_per_second: float
    kalshi_sports_min_volume_24h: float
    sports_window_days: int
    sports_window_past_days: int
    sports_league_filter_kalshi: Optional[str]
    sports_league_filter_poly: Optional[str]
    kalshi_signing_mode: str
    limitless_adapter: str
    limitless_http_url: Optional[str]
    limitless_http_path_template: str
    limitless_timeout_seconds: float

    mock_mode: bool
    db_path: str


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}: {raw}") from exc


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid int for {name}: {raw}") from exc


def load_config(mock_mode: bool = False) -> AppConfig:
    load_dotenv()

    return AppConfig(
        scan_interval_seconds=_get_float("SCAN_INTERVAL_SECONDS", 5.0),
        arb_threshold=_get_float("ARB_THRESHOLD", 0.01),
        dedupe_seconds=_get_int("DEDUPE_SECONDS", 60),
        margin_step_resend=_get_float("MARGIN_STEP_RESEND", 0.005),
        size_step_resend=_get_float("SIZE_STEP_RESEND", 10.0),
        min_size=_get_float("MIN_SIZE", 10.0),
        match_threshold=_get_float("MATCH_THRESHOLD", 0.85),
        include_review=os.getenv("INCLUDE_REVIEW", "false").lower() == "true",
        polymarket_scan_limit=_get_int("POLYMARKET_SCAN_LIMIT", 1000),
        polymarket_scan_page_size=_get_int("POLYMARKET_SCAN_PAGE_SIZE", 200),
        kalshi_scan_limit=_get_int("KALSHI_SCAN_LIMIT", 1000),
        match_candidate_limit=_get_int("MATCH_CANDIDATE_LIMIT", 200),
        time_window_days=_get_int("TIME_WINDOW_DAYS", 7),
        min_review_score=_get_float("MIN_REVIEW_SCORE", 0.5),
        polymarket_l1_private_key=os.getenv("POLYMARKET_L1_PRIVATE_KEY"),
        polymarket_l2_api_key=os.getenv("POLYMARKET_L2_API_KEY"),
        polymarket_l2_api_secret=os.getenv("POLYMARKET_L2_API_SECRET"),
        polymarket_l2_api_passphrase=os.getenv("POLYMARKET_L2_API_PASSPHRASE"),
        kalshi_api_key=os.getenv("KALSHI_API_KEY"),
        kalshi_private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH"),
        kalshi_enabled=os.getenv("KALSHI_ENABLED", "true").lower() == "true",
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
        fee_polymarket=_get_float("FEE_POLYMARKET", 0.0),
        fee_kalshi=_get_float("FEE_KALSHI", 0.0),
        fee_limitless=_get_float("FEE_LIMITLESS", 0.0),
        slippage_polymarket=_get_float("SLIPPAGE_POLYMARKET", 0.005),
        slippage_kalshi=_get_float("SLIPPAGE_KALSHI", 0.005),
        slippage_limitless=_get_float("SLIPPAGE_LIMITLESS", 0.005),
        polymarket_rest_url=os.getenv("POLYMARKET_REST_URL", "https://clob.polymarket.com"),
        polymarket_ws_url=os.getenv(
            "POLYMARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com"
        ),
        polymarket_gamma_url=os.getenv(
            "POLYMARKET_GAMMA_URL", "https://gamma-api.polymarket.com"
        ),
        kalshi_base_url=os.getenv(
            "KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"
        ),
        kalshi_timeout_seconds=_get_float("KALSHI_TIMEOUT_SECONDS", 10.0),
        kalshi_rate_limit_per_second=_get_float("KALSHI_RATE_LIMIT_PER_SECOND", 5.0),
        kalshi_sports_min_volume_24h=_get_float("KALSHI_SPORTS_MIN_VOLUME_24H", 0.0),
        sports_window_days=_get_int("SPORTS_WINDOW_DAYS", 7),
        sports_window_past_days=_get_int("SPORTS_WINDOW_PAST_DAYS", 1),
        sports_league_filter_kalshi=os.getenv("SPORTS_LEAGUE_FILTER_KALSHI"),
        sports_league_filter_poly=os.getenv("SPORTS_LEAGUE_FILTER_POLY"),
        kalshi_signing_mode=os.getenv("KALSHI_SIGNING_MODE", "none"),
        limitless_adapter=os.getenv("LIMITLESS_ADAPTER", "stub"),
        limitless_http_url=os.getenv("LIMITLESS_HTTP_URL"),
        limitless_http_path_template=os.getenv(
            "LIMITLESS_HTTP_PATH_TEMPLATE", "/markets/{market_id}/book"
        ),
        limitless_timeout_seconds=_get_float("LIMITLESS_TIMEOUT_SECONDS", 10.0),
        mock_mode=mock_mode,
        db_path=os.getenv("ARBSCAN_DB_PATH", "arbscan.db"),
    )
