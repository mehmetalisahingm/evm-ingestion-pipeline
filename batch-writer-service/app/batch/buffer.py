from dataclasses import dataclass
from time import monotonic
from typing import Any

from app.batch.models import CanonicalEvent


@dataclass
class BufferedRecord:
    kafka_message: Any
    event: CanonicalEvent


class BatchBuffer:
    def __init__(
        self,
        *,
        max_size: int,
        flush_interval_seconds: float,
    ) -> None:
        self._max_size = max_size
        self._flush_interval_seconds = flush_interval_seconds

        self._records: list[BufferedRecord] = []
        self._last_flush_time = monotonic()

    def add(
        self,
        kafka_message: Any,
        event: CanonicalEvent,
    ) -> None:
        self._records.append(
            BufferedRecord(
                kafka_message=kafka_message,
                event=event,
            )
        )

    def size(self) -> int:
        return len(self._records)

    def is_empty(self) -> bool:
        return not self._records

    def should_flush_by_size(self) -> bool:
        return self.size() >= self._max_size

    def should_flush_by_time(self) -> bool:
        if self.is_empty():
            return False

        return self.seconds_until_flush() <= 0

    def seconds_until_flush(self) -> float:
        elapsed = monotonic() - self._last_flush_time

        remaining = (
            self._flush_interval_seconds - elapsed
        )

        return max(remaining, 0.0)

    def take_all(self) -> list[BufferedRecord]:
        records = self._records
        self._records = []

        return records

    def restore(
        self,
        records: list[BufferedRecord],
    ) -> None:
        self._records = records + self._records

    def mark_flushed(self) -> None:
        self._last_flush_time = monotonic()