import json
from dataclasses import asdict
from typing import Optional

import requests

from arbscan.models import ArbSignal


def format_signal(signal: ArbSignal) -> str:
    payload = {
        "alert_id": signal.alert_id,
        "timestamp": signal.timestamp.isoformat(),
        "canonical_event_id": signal.canonical_event_id,
        "yes_leg": {
            "venue": signal.yes_ref.venue,
            "market_id": signal.yes_ref.market_id,
            "price": signal.yes_price,
            "size": signal.yes_size,
        },
        "no_leg": {
            "venue": signal.no_ref.venue,
            "market_id": signal.no_ref.market_id,
            "price": signal.no_price,
            "size": signal.no_size,
        },
        "executable_size": signal.executable_size,
        "fees": signal.fees,
        "slippage": signal.slippage,
        "total_cost": signal.total_cost,
        "margin_abs": signal.margin_abs,
        "margin_pct": signal.margin_pct,
        "outcome_mapping": signal.outcome_mapping,
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def notify(signal: ArbSignal, slack_webhook_url: Optional[str]) -> None:
    message = format_signal(signal)
    print(message)

    if not slack_webhook_url:
        return

    try:
        requests.post(slack_webhook_url, json={"text": message}, timeout=10)
    except requests.RequestException:
        return
