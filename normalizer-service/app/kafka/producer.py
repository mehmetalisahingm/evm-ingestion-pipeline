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
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send_normalized(
        self,
        event: dict[str, Any],
    ) -> None:

        chain_id = event.get("chain_id")

        if not isinstance(chain_id, int):
            raise ValueError(
            )

        await self._producer.send_and_wait(
            topic=settings.normalized_topic,
            key=str(chain_id).encode("utf-8"),
            value=_serialize_message(event),
        )

    async def send_dlq(
        self,
        message: dict[str, Any],
    ) -> None:
        original_message = message.get("original_message")
        chain_id: Any = None

        if isinstance(original_message, dict):
            chain_id = original_message.get("chain_id")

        if isinstance(chain_id, int):
            key = str(chain_id).encode("utf-8")
        else:
            key = b"unknown"

        await self._producer.send_and_wait(
            topic=settings.dlq_topic,
            key=key,
            value=_serialize_message(message),
        )
