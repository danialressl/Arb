import re
from typing import Optional, Tuple

from arbv2.models import Market, SportsPredicate
from arbv2.teams import canonicalize_team


_TEAM_SEP_PATTERNS = [
    re.compile(r"\s+vs\.?\s+", re.IGNORECASE),
    re.compile(r"\s+@\s+", re.IGNORECASE),
    re.compile(r"\s+at\s+", re.IGNORECASE),
    re.compile(r"\s+beat\s+", re.IGNORECASE),
    re.compile(r"\s+defeat\s+", re.IGNORECASE),
    re.compile(r"\s+defeats\s+", re.IGNORECASE),
]


def parse_sports_predicate(market: Market) -> Optional[SportsPredicate]:
    event_title = market.event_title or ""
    title = market.title or ""
    teams = None
    if market.venue == "polymarket" and market.raw_json:
        description = market.raw_json.get("description")
        teams = _extract_teams_from_description(description)
    if not teams:
        teams = _extract_teams(event_title) or _extract_teams(title)
    if not teams:
        return None
    team_a, team_b = teams
    team_a = canonicalize_team(team_a)
    team_b = canonicalize_team(team_b)
    if not team_a or not team_b:
        return None
    winner = _extract_winner(title, market)
    if not winner:
        return None
    return SportsPredicate(
        venue=market.venue,
        market_id=market.market_id,
        event_id=market.event_id,
        team_a=team_a,
        team_b=team_b,
        winner=winner,
    )


def _extract_teams(text: str) -> Optional[Tuple[str, str]]:
    if not text:
        return None
    base = text.split(":")[0].strip()
    for pattern in _TEAM_SEP_PATTERNS:
        if pattern.search(base):
            parts = pattern.split(base, maxsplit=1)
            if len(parts) == 2:
                left = _clean_team_name(parts[0])
                right = _clean_team_name(parts[1])
                if left and right:
                    return left, right
    match = re.search(r"(.+?)\s+or\s+(.+?)\s+win", base, re.IGNORECASE)
    if match:
        left = _clean_team_name(match.group(1))
        right = _clean_team_name(match.group(2))
        if left and right:
            return left, right
    return None


def _extract_teams_from_description(description: Optional[str]) -> Optional[Tuple[str, str]]:
    if not description:
        return None
    matches = re.findall(r'"([^"]+)"', description)
    if len(matches) < 2:
        return None
    left = _clean_team_name(matches[0])
    right = _clean_team_name(matches[1])
    if left and left.strip().upper() in {"YES", "NO", "DRAW", "TIE"}:
        return None
    if right and right.strip().upper() in {"YES", "NO", "DRAW", "TIE"}:
        return None
    if left and right:
        return left, right
    return None


def _extract_winner(title: str, market: Market) -> Optional[str]:
    if market.outcome_label:
        return str(market.outcome_label).strip()
    if market.raw_json:
        yes_sub_title = market.raw_json.get("yes_sub_title") or market.raw_json.get("yes_subtitle")
        if yes_sub_title:
            return str(yes_sub_title).strip()
    if not title:
        return None
    match = re.search(r"will\s+(.+?)\s+win", title, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"(.+?)\s+to\s+win", title, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"will\s+(.+?)\s+beat", title, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def _clean_team_name(value: str) -> Optional[str]:
    if not value:
        return None
    text = value.strip()
    text = re.sub(r"^will\s+.+?\s+win\s+(the\s+)?", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"^(the|a|an)\s+", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\b(winner|win)\b\??$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\b(match|game)\b\??$", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text or None
