import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer

from app.settings import settings


def _serialize_message(
    message: dict[str, Any],
) -> bytes:
    return json.dumps(
        message,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


class NormalizerKafkaProducer:
    def __init__(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=(
                settings.kafka_bootstrap_servers
            ),
            acks="all",
            enable_idempotence=True,

            # Kafka'nın kısa süre içinde gelen mesajları
            # aynı batch içinde toplamasına fırsat verir.
            linger_ms=5,

            # Producer batch boyutu.
            max_batch_size=131072,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def queue_normalized(
        self,
        event: dict[str, Any],
    ) -> Any:
        chain_id = event.get("chain_id")

        if not isinstance(chain_id, int):
            raise ValueError(
                "Normalized event chain_id "
                "integer olmalıdır."
            )

        delivery_future = (
            await self._producer.send(
                topic=settings.normalized_topic,
                key=str(chain_id).encode(
                    "utf-8"
                ),
                value=_serialize_message(
                    event
                ),
            )
        )

        return delivery_future

    async def send_normalized(
        self,
        event: dict[str, Any],
    ) -> None:
        """
        Eski davranışı korur.

        Mesaj Kafka tarafından onaylanana
        kadar bekler.
        """
        delivery_future = (
            await self.queue_normalized(
                event
            )
        )

        await delivery_future

    async def queue_dlq(
        self,
        message: dict[str, Any],
    ) -> Any:
        original_message = (
            message.get(
                "original_message"
            )
        )

        chain_id: Any = None

        if isinstance(
            original_message,
            dict,
        ):
            chain_id = (
                original_message.get(
                    "chain_id"
                )
            )

        if isinstance(chain_id, int):
            key = str(chain_id).encode(
                "utf-8"
            )
        else:
            key = b"unknown"

        delivery_future = (
            await self._producer.send(
                topic=settings.dlq_topic,
                key=key,
                value=_serialize_message(
                    message
                ),
            )
        )

        return delivery_future

    async def send_dlq(
        self,
        message: dict[str, Any],
    ) -> None:
        """
        Eski davranışı korur.

        DLQ mesajının Kafka tarafından
        onaylanmasını bekler.
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