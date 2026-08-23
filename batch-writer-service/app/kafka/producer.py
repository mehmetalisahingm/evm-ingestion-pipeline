import json
from typing import Any

from aiokafka import AIOKafkaProducer


class BatchWriterKafkaProducer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        retry_topic: str,
        dlq_topic: str,
    ) -> None:
        self._retry_topic = retry_topic
        self._dlq_topic = dlq_topic

        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send_retry_batch(
        self,
        *,
        records: list[dict[str, Any]],
        error_type: str,
        error_reason: str,
        retry_count: int,
    ) -> None:
        message = {
            "source_service": "batch-writer-service",
            "error_type": error_type,
            "error_reason": error_reason,
            "retry_count": retry_count,
            "records": records,
        }

        value = json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        await self._producer.send_and_wait(
            self._retry_topic,
            value=value,
        )

    async def send_dlq(
        self,
        *,
        original_value: bytes,
        error_type: str,
        error_reason: str,
    ) -> None:
        message = {
            "source_service": "batch-writer-service",
            "source_topic": "canonical-events",
            "error_type": error_type,
            "error_reason": error_reason,
            "original_message": original_value.decode(
                "utf-8",
                errors="replace",
            ),
        }

        value = json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        await self._producer.send_and_wait(
            self._dlq_topic,
            value=value,
        )