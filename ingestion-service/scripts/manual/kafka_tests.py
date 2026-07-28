# import asyncio
# import json

# from aiokafka import AIOKafkaProducer


# KAFKA_BOOSTRAP_SERVİCES = "localhost:9092"
# KAFKA_TOPIC = "evm.raw"

# async def main():
#     producer = AIOKafkaProducer(
#         bootstrap_servers=KAFKA_BOOSTRAP_SERVİCES,
#     )
#     await producer.start()

#     try:
#         message ={
#             "chain_id": 56,
#             "data_type":"test",
#             "message":"python kafka mesaj",
#         }

#         await producer.send_and_wait(
#             topic=KAFKA_TOPIC,
#             key=str(message["chain_id"]).encode("utf-8"),
#             value=json.dumps(message).encode("utf-8"),
#         )

#         print("mesaj gönderildi")


#     finally:
#         await producer.stop()


# if __name__=="__main__":
#     asyncio.run(main())




import asyncio

from app.kafka.producer import KafkaProducerService


async def main():
    producer = KafkaProducerService()

    await producer.start()

    try:
        event = {
            "schema_version": 1,
            "chain_id": 56,
            "data_type": "test",
            "message": "Gerçek Kafka producer modülü çalışıyor",
        }

        await producer.send_event(event)

        print("Mesaj gönderildi.")

    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())