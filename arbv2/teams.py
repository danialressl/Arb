import json
import re
from urllib.parse import urlparse
from pathlib import Path
from typing import Dict, Optional


_DATA_DIR = Path(__file__).resolve().parent / "data"
_LEAGUE_FILES = {
    "nba": _DATA_DIR / "teams_nba.json",
    "nhl": _DATA_DIR / "teams_nhl.json",
}

_ALIASES_BY_LEAGUE: Dict[str, Dict[str, str]] = {}
for league, path in _LEAGUE_FILES.items():
    if not path.exists():
        raise FileNotFoundError(f"Missing team alias table: {path}")
    _ALIASES_BY_LEAGUE[league] = json.loads(path.read_text(encoding="utf-8"))

_SERIES_TO_LEAGUE = {
    "KXNBAGAME": "nba",
    "KXNHLGAME": "nhl",
}
_LEAGUE_DOMAINS = {
    "nba.com": "nba",
    "nhl.com": "nhl",
}


def canonicalize_team(name: Optional[str], league_hint: Optional[str] = None) -> Optional[str]:
    if not name:
        return None
    normalized = re.sub(r"[^\w\s]", " ", str(name).upper())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return None
    if league_hint:
        aliases = _ALIASES_BY_LEAGUE.get(league_hint.lower())
        if aliases:
            return aliases.get(normalized, normalized)
    return normalized


def league_hint_from_series_ticker(series_ticker: Optional[str]) -> Optional[str]:
    if not series_ticker:
        return None
    return _SERIES_TO_LEAGUE.get(str(series_ticker))


def league_hint_from_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    if re.search(r"\bNBA\b", text, re.IGNORECASE):
        return "nba"
    if re.search(r"\bNHL\b", text, re.IGNORECASE):
        return "nhl"
    return None


def league_hint_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        host = urlparse(str(url)).netloc.lower()
    except ValueError:
        return None
    host = host.split(":")[0]
    for domain, league in _LEAGUE_DOMAINS.items():
        if host == domain or host.endswith(f".{domain}"):
            return league
    return None
