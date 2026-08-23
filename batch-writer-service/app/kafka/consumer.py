from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import OffsetAndMetadata, TopicPartition


class BatchWriterKafkaConsumer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
    ) -> None:
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def getone(self) -> Any:
        return await self._consumer.getone()

    async def commit_message(
        self,
        message: Any,
    ) -> None:
        topic_partition = TopicPartition(
            message.topic,
            message.partition,
        )

        await self._consumer.commit(
            {
                topic_partition: OffsetAndMetadata(
                    message.offset + 1,
                    "",
                )
            }
        )

    async def commit_batch(
        self,
        messages: list[Any],
    ) -> None:
        if not messages:
            return

        highest_offsets: dict[TopicPartition, int] = {}

        for message in messages:
            topic_partition = TopicPartition(
                message.topic,
                message.partition,
            )

            current_offset = highest_offsets.get(
                topic_partition
            )

            if (
                current_offset is None
                or message.offset > current_offset
            ):
                highest_offsets[
                    topic_partition
                ] = message.offset

        offsets_to_commit = {
            topic_partition: OffsetAndMetadata(
                offset + 1,
                "",
            )
            for topic_partition, offset
            in highest_offsets.items()
        }

        await self._consumer.commit(
            offsets_to_commit
        )