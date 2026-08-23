from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from app.settings import settings


class NormalizerKafkaConsumer:
    def __init__(self) -> None:
        self._consumer = AIOKafkaConsumer(
            settings.raw_topic,
            bootstrap_servers=(
                settings.kafka_bootstrap_servers
            ),
            group_id=(
                settings.consumer_group
            ),
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
        Kafka'dan tek seferde birden fazla
        mesaj çeker.

        Şu an evm.raw 1 partition olsa da
        kod birden fazla partition için
        çalışabilecek şekilde hazırlanır.
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
        Tek mesaj için eski commit
        davranışı.
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
        yalnızca en yüksek offset'i commit
        eder.

        Örnek:

        offset:
        100
        101
        102
        ...
        599

        Kafka'ya tek commit:
        600
        """

        if not messages:
            return

        highest_offsets: dict[
            TopicPartition,
            int,
        ] = {}

        for message in messages:
            topic_partition = (
                TopicPartition(
                    message.topic,
                    message.partition,
                )
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