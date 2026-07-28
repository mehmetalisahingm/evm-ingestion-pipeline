import asyncio
import json
from uuid import uuid4

from aiokafka import AIOKafkaConsumer

from app.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


async def main() -> None:
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"payload-test-{uuid4()}",
    )

    await consumer.start()

    try:
        print("Kafka mesajları kontrol ediliyor...")

        async for message in consumer:
            if not message.value:
                print("Boş mesaj atlandı.")
                continue

            try:
                message_text = message.value.decode("utf-8")
                event = json.loads(message_text)

            except UnicodeDecodeError:
                print("UTF-8 olmayan eski mesaj atlandı.")
                continue

            except json.JSONDecodeError:
                print("JSON olmayan eski mesaj atlandı.")
                continue

            if not isinstance(event, dict):
                print("JSON nesnesi olmayan mesaj atlandı.")
                continue

            payload = event.get("payload")

            if not isinstance(payload, dict):
                continue

            transactions = payload.get("transactions")

            if not isinstance(transactions, list):
                continue

            print("Blok numarası:", event.get("block_number"))
            print("Transaction sayısı:", len(transactions))
            print("Kafka partition:", message.partition)
            print("Kafka offset:", message.offset)

            if transactions:
                print(
                    "İlk transaction veri tipi:",
                    type(transactions[0]).__name__,
                )
            else:
                print("Bu blokta transaction bulunmuyor.")

            break

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())