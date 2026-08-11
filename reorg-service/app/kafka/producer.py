import json
from typing import Any

from aiokafka import AIOKafkaProducer

from app.settings import settings


def _serialize(message: dict[str, Any]) -> bytes:
    return json.dumps(
        message,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


class ReorgKafkaProducer:
    def __init__(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send_canonical(
        self,
        event: dict[str, Any],
    ) -> None:
        event_id = event.get("event_id")

        if not isinstance(event_id, str):
            raise ValueError(
                "Canonical event içerisinde event_id bulunamadı"
            )

        await self._producer.send_and_wait(
            topic=settings.canonical_topic,
            key=event_id.encode("utf-8"),
            value=_serialize(event),
        )

    async def send_dlq(
        self,
        message: dict[str, Any],
    ) -> None:
        event_id = message.get("event_id")

        key = (
            event_id.encode("utf-8")
            if isinstance(event_id, str)
            else b"reorg-error"
        )

        await self._producer.send_and_wait(
            topic=settings.dlq_topic,
            key=key,
            value=_serialize(message),
        )
