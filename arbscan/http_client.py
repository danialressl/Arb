from __future__ import annotations

import time
from typing import Optional

import requests


def get_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 10.0,
    retries: int = 3,
    backoff: float = 0.5,
) -> tuple[Optional[dict], Optional[int]]:
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code >= 500 and attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            try:
                return resp.json(), resp.status_code
            except ValueError:
                return None, resp.status_code
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            return None, None
    if last_exc:
        return None, None
    return None, None
