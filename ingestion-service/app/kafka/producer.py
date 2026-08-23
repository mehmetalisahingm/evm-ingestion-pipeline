import asyncio
import json

from aiokafka import AIOKafkaProducer

from app.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


class KafkaProducerService:
    def __init__(self) -> None:
        self._producer: (
            AIOKafkaProducer | None
        ) = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=(
                KAFKA_BOOTSTRAP_SERVERS
            ),
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            max_batch_size=131072,
        )

        await self._producer.start()

    def _serialize_event(
        self,
        event: dict,
    ) -> tuple[bytes, bytes]:
        chain_id = event["chain_id"]

        key = str(
            chain_id
        ).encode(
            "utf-8"
        )

        value = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode(
            "utf-8"
        )

        return key, value

    async def send_event(
        self,
        event: dict,
    ) -> None:
        if self._producer is None:
            raise RuntimeError(
                "Kafka producer başlatılmadı"
            )

        key, value = self._serialize_event(
            event
        )

        await self._producer.send_and_wait(
            topic=KAFKA_TOPIC,
            key=key,
            value=value,
        )

    async def send_events(
        self,
        events: list[dict],
    ) -> None:
        if self._producer is None:
            raise RuntimeError(
                "Kafka producer başlatılmadı"
            )

        if not events:
            return

        delivery_futures = []

        for event in events:
            key, value = (
                self._serialize_event(
                    event
                )
            )

            future = await self._producer.send(
                topic=KAFKA_TOPIC,
                key=key,
                value=value,
            )

            delivery_futures.append(
                future
            )

        await asyncio.gather(
            *delivery_futures
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

            self._producer = None