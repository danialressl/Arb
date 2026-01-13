from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Tuple

from arbscan.models import ArbSignal


@dataclass
class DedupeState:
    last_sent: datetime
    last_margin: float
    last_size: float


class DedupeCache:
    def __init__(
        self, dedupe_seconds: int, margin_step_resend: float, size_step_resend: float
    ) -> None:
        self.dedupe_seconds = dedupe_seconds
        self.margin_step_resend = margin_step_resend
        self.size_step_resend = size_step_resend
        self._state: Dict[str, DedupeState] = {}

    def should_send(self, signal: ArbSignal, now: datetime) -> bool:
        key = self._key(signal)
        state = self._state.get(key)
        if state is None:
            self._state[key] = DedupeState(
                last_sent=now,
                last_margin=signal.margin_abs,
                last_size=signal.executable_size,
            )
            return True

        age = now - state.last_sent
        if age >= timedelta(seconds=self.dedupe_seconds):
            state.last_sent = now
            state.last_margin = signal.margin_abs
            state.last_size = signal.executable_size
            return True

        if signal.margin_abs - state.last_margin >= self.margin_step_resend:
            state.last_sent = now
            state.last_margin = signal.margin_abs
            state.last_size = signal.executable_size
            return True

        if signal.executable_size - state.last_size >= self.size_step_resend:
            state.last_sent = now
            state.last_margin = signal.margin_abs
            state.last_size = signal.executable_size
            return True

        return False

    @staticmethod
    def _key(signal: ArbSignal) -> str:
        return (
            f"{signal.canonical_event_id}|"
            f"YES:{signal.yes_ref.venue}:{signal.yes_ref.market_id}|"
            f"NO:{signal.no_ref.venue}:{signal.no_ref.market_id}"
        )
