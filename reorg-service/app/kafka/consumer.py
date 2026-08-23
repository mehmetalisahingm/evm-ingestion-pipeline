from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from app.settings import settings


class ReorgKafkaConsumer:
    def __init__(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.normalized_topic,
            bootstrap_servers=(
                settings.kafka_bootstrap_servers
            ),
            group_id=settings.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def get_message(
        self,
    ) -> Any:
        """
        Eski tek mesaj kullanımını korur.
        """
        return await self._consumer.getone()

    async def get_batch(
        self,
        *,
        timeout_ms: int = 100,
        max_records: int = 500,
    ) -> list[Any]:
        """
        Kafka'dan tek seferde en fazla
        max_records kadar mesaj alır.

        Mesajlar partition ve offset
        sırasına göre sıralanır.
        """

        records = await self._consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=max_records,
        )

        messages: list[Any] = []

        for partition_messages in (
            records.values()
        ):
            messages.extend(
                partition_messages
            )

        messages.sort(
            key=lambda message: (
                message.partition,
                message.offset,
            )
        )

        return messages

    async def commit_message(
        self,
        message: Any,
    ) -> None:
        """
        Eski tek mesaj commit davranışı.
        """

        topic_partition = TopicPartition(
            message.topic,
            message.partition,
        )

        await self._consumer.commit(
            {
                topic_partition:
                message.offset + 1
            }
        )

    async def commit_batch(
        self,
        messages: list[Any],
    ) -> None:
        """
        Batch içindeki her partition için
        yalnızca en yüksek offset commit edilir.

        Örnek:

        500
        501
        502
        ...
        999

        Kafka commit:
        1000
        """

        if not messages:
            return

        highest_offsets: dict[
            TopicPartition,
            int,
        ] = {}

        for message in messages:
            topic_partition = TopicPartition(
                message.topic,
                message.partition,
            )

            next_offset = (
                message.offset + 1
            )

            current_offset = (
                highest_offsets.get(
                    topic_partition
                )
            )

            if (
                current_offset is None
                or next_offset
                > current_offset
            ):
                highest_offsets[
                    topic_partition
                ] = next_offset

        await self._consumer.commit(
            highest_offsets
        )