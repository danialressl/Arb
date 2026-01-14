import json
import re
from pathlib import Path
from typing import Dict, Optional


_DATA_DIR = Path(__file__).resolve().parent / "data"
_ALIAS_FILES = [
    _DATA_DIR / "teams_nba.json",
    _DATA_DIR / "teams_nhl.json",
]

_ALIASES: Dict[str, str] = {}
for path in _ALIAS_FILES:
    if not path.exists():
        raise FileNotFoundError(f"Missing team alias table: {path}")
    _ALIASES.update(json.loads(path.read_text(encoding="utf-8")))


def canonicalize_team(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    normalized = re.sub(r"[^\w\s]", " ", str(name).upper())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return None
    return _ALIASES.get(normalized, normalized)
