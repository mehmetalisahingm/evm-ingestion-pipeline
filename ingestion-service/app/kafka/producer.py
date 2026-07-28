import json
from aiokafka import AIOKafkaProducer
from app.settings import KAFKA_BOOTSTRAP_SERVERS,KAFKA_TOPIC

class KafkaProducerService:
    def __init__(self)->None:
        self._producer:AIOKafkaProducer | None=None

    async def start(self)-> None:
        self._producer=AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks="all",
            enable_idempotence=True,
            
        )


        await self._producer.start()

    async def send_event(self,event:dict)->None:
        if self._producer is None:
            raise RuntimeError("kafka producer başlatamadı")

        chain_id=event["chain_id"]

        await self._producer.send_and_wait(
            topic=KAFKA_TOPIC,
            key=str(chain_id).encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer =None