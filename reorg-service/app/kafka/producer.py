import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer

from app.settings import settings


def _serialize(
    message: dict[str, Any],
) -> bytes:
    return json.dumps(
        message,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


class ReorgKafkaProducer:
    def __init__(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=(
                settings.kafka_bootstrap_servers
            ),
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            max_batch_size=131072,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def queue_canonical(
        self,
        event: dict[str, Any],
    ) -> Any:
        event_id = event.get("event_id")

        if not isinstance(event_id, str):
            raise ValueError(
                "Canonical event içerisinde "
                "event_id bulunamadı."
            )

        delivery_future = (
            await self._producer.send(
                topic=settings.canonical_topic,
                key=event_id.encode("utf-8"),
                value=_serialize(event),
            )
        )

        return delivery_future

    async def send_canonical(
        self,
        event: dict[str, Any],
    ) -> None:
        """
        Eski davranışı korur.

        Kafka ACK gelene kadar bekler.
        """

        delivery_future = (
            await self.queue_canonical(
                event
            )
        )

        await delivery_future

    async def queue_dlq(
        self,
        message: dict[str, Any],
    ) -> Any:
        event_id = message.get("event_id")

        key = (
            event_id.encode("utf-8")
            if isinstance(event_id, str)
            else b"reorg-error"
        )

        delivery_future = (
            await self._producer.send(
                topic=settings.dlq_topic,
                key=key,
                value=_serialize(message),
            )
        )

        return delivery_future

    async def send_dlq(
        self,
        message: dict[str, Any],
    ) -> None:
        """
        Eski davranışı korur.
        """

        delivery_future = (
            await self.queue_dlq(
                message
            )
        )

        await delivery_future

    async def wait_for_deliveries(
        self,
        deliveries: list[Any],
    ) -> None:
        if not deliveries:
            return

        await asyncio.gather(
            *deliveries
        )

    async def flush(self) -> None:
        await self._producer.flush()