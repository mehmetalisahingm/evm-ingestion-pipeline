# from typing import Any
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from app.settings import settings

class NormalizerKafkaConsumer:
    def __init__(self):
        self._consumer=AIOKafkaConsumer(
            settings.raw_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    async def start(self)-> None:
        await self._consumer.start()

    async def stop(self)->None:
        await self._consumer.stop()

    async def get_message(self):
        return await self._consumer.getone()

    async def commit_message(self,message)-> None:
        topic_partition= TopicPartition(
            message.topic,
            message.partition,
        )
        await self._consumer.commit(
            {
        topic_partition: message.offset + 1,
            }
)
