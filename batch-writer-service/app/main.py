import asyncio

from app.batch.buffer import BatchBuffer
from app.batch.processor import BatchProcessor
from app.batch.service import BatchWriterService
from app.clickhouse.client import ClickHouseClient
from app.kafka.consumer import BatchWriterKafkaConsumer
from app.kafka.producer import BatchWriterKafkaProducer
from app.monitoring.server import MonitoringServer
from app.settings import settings


async def main() -> None:
    consumer = BatchWriterKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.canonical_topic,
        group_id=settings.consumer_group,
    )

    producer = BatchWriterKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        retry_topic=settings.retry_writer_topic,
        dlq_topic=settings.dlq_topic,
    )

    clickhouse_client = ClickHouseClient(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )

    monitoring_server = MonitoringServer(
        port=settings.monitoring_port,
    )

    buffer = BatchBuffer(
        max_size=settings.batch_size,
        flush_interval_seconds=(
            settings.flush_interval_seconds
        ),
    )

    service = BatchWriterService(
        clickhouse_client=clickhouse_client,
        consumer=consumer,
        producer=producer,
    )

    processor = BatchProcessor(
        consumer=consumer,
        producer=producer,
        service=service,
        buffer=buffer,
        flush_interval_seconds=(
            settings.flush_interval_seconds
        ),
    )

    try:
        await monitoring_server.start()

        await clickhouse_client.start()
        await producer.start()
        await consumer.start()

        monitoring_server.set_ready(True)

        print("Batch Writer Service başladı.")

        await processor.run()

    finally:
        monitoring_server.set_ready(False)

        await consumer.stop()
        await producer.stop()
        await clickhouse_client.stop()
        await monitoring_server.stop()


if __name__ == "__main__":
    asyncio.run(main())