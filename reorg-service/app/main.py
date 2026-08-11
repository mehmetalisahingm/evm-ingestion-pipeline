import asyncio

from app.kafka.consumer import ReorgKafkaConsumer
from app.kafka.producer import ReorgKafkaProducer
from app.monitoring.metrics import (
    CANONICAL_EVENTS_TOTAL,
    DLQ_MESSAGES_TOTAL,
    DUPLICATE_EVENTS_TOTAL,
    NORMALIZED_MESSAGES_TOTAL,
    OFFSET_COMMITS_TOTAL,
    PROCESSING_ERRORS_TOTAL,
    REORG_DETECTED_TOTAL,
    SERVICE_READY,
)
from app.monitoring.server import start_monitoring_server
from app.reorg.processor import process_message
from app.reorg.service import ReorgService
from app.settings import settings
from app.state.redis_store import RedisStateStore


async def run() -> None:
    consumer = ReorgKafkaConsumer()
    producer = ReorgKafkaProducer()

    state_store = RedisStateStore(
        redis_url=settings.redis_url,
        window_size=settings.redis_window_size,
        pending_ttl_seconds=settings.pending_event_ttl_seconds,
    )

    reorg_service = ReorgService(
        state_store=state_store
    )

    monitoring_runner = None
    consumer_started = False
    producer_started = False
    redis_started = False

    SERVICE_READY.set(0)

    try:
        monitoring_runner = await start_monitoring_server(
            settings.monitoring_port
        )

        await state_store.start()
        redis_started = True
        print("Redis bağlantısı hazır")

        await producer.start()
        producer_started = True
        print("Kafka producer hazır")

        await consumer.start()
        consumer_started = True
        print("Kafka consumer hazır")

        SERVICE_READY.set(1)

        print("Re-org Service çalışıyor...")

        while True:
            message = await consumer.get_message()

            NORMALIZED_MESSAGES_TOTAL.inc()

            try:
                result = await process_message(
                    message,
                    reorg_service=reorg_service,
                    producer=producer,
                    state_store=state_store,
                )

                CANONICAL_EVENTS_TOTAL.inc(
                    result.canonical_count
                )

                DUPLICATE_EVENTS_TOTAL.inc(
                    result.duplicate_count
                )

                if result.reorg_detected:
                    REORG_DETECTED_TOTAL.inc()

                if result.sent_to_dlq:
                    DLQ_MESSAGES_TOTAL.inc()

                await consumer.commit_message(message)
                OFFSET_COMMITS_TOTAL.inc()

            except Exception:
                PROCESSING_ERRORS_TOTAL.inc()
                raise

    finally:
        SERVICE_READY.set(0)

        if consumer_started:
            await consumer.stop()

        if producer_started:
            await producer.stop()

        if redis_started:
            await state_store.stop()

        if monitoring_runner is not None:
            await monitoring_runner.cleanup()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nRe-org Service durduruldu")


if __name__ == "__main__":
    main()
