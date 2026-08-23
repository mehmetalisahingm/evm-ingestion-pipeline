import asyncio
from collections import defaultdict
from typing import Any

from app.kafka.consumer import NormalizerKafkaConsumer
from app.kafka.producer import NormalizerKafkaProducer
from app.monitoring.metrics import (
    DLQ_MESSAGES_TOTAL,
    NORMALIZED_EVENTS_TOTAL,
    OFFSET_COMMITS_TOTAL,
    PROCESSING_DURATION_SECONDS,
    PROCESSING_ERRORS_TOTAL,
    RAW_MESSAGES_TOTAL,
)
from app.monitoring.server import (
    monitoring_state,
    start_monitoring_server,
)
from app.normalizer.processor import (
    prepare_kafka_message,
    queue_prepared_message,
)
from app.settings import settings


BATCH_SIZE = 500
BATCH_TIMEOUT_MS = 100


async def run_normalizer() -> None:
    consumer = NormalizerKafkaConsumer()
    producer = NormalizerKafkaProducer()

    consumer_started = False
    producer_started = False

    monitoring_runner = (
        await start_monitoring_server(
            settings.monitoring_port
        )
    )

    try:
        await producer.start()
        producer_started = True

        await consumer.start()
        consumer_started = True

        monitoring_state.set_ready(True)

        print(
            "Normalizer Service çalışıyor | "
            f"Kaynak: {settings.raw_topic} | "
            f"Çıktı: {settings.normalized_topic} | "
            f"Batch size: {BATCH_SIZE}",
            flush=True,
        )

        while True:
            messages = await consumer.get_batch(
                timeout_ms=BATCH_TIMEOUT_MS,
                max_records=BATCH_SIZE,
            )

            if not messages:
                continue

            deliveries: list[Any] = []

            normalized_count = 0
            dlq_count = 0

            event_counts: dict[str, int] = (
                defaultdict(int)
            )

            try:
                for message in messages:
                    RAW_MESSAGES_TOTAL.inc()

                    with (
                        PROCESSING_DURATION_SECONDS.time()
                    ):
                        prepared = (
                            prepare_kafka_message(
                                message
                            )
                        )

                        message_deliveries = (
                            await queue_prepared_message(
                                prepared=prepared,
                                producer=producer,
                            )
                        )

                    deliveries.extend(
                        message_deliveries
                    )

                    if prepared.sent_to_dlq:
                        error_type = (
                            prepared.error_type
                            or
                            "UnknownNormalizationError"
                        )

                        DLQ_MESSAGES_TOTAL.labels(
                            error_type=error_type
                        ).inc()

                        dlq_count += 1

                    else:
                        normalized_count += (
                            prepared.normalized_count
                        )

                        for event_type in (
                            prepared.event_types
                        ):
                            event_counts[
                                event_type
                            ] += 1

                # Bütün eventler producer bufferına
                # konduktan sonra ACK'leri birlikte
                # beklenir.
                await producer.wait_for_deliveries(
                    deliveries
                )

                # Tüm Kafka çıktıları başarılıysa
                # batch için tek offset commit yapılır.
                await consumer.commit_batch(
                    messages
                )

                OFFSET_COMMITS_TOTAL.inc()

                for (
                    event_type,
                    count,
                ) in event_counts.items():
                    NORMALIZED_EVENTS_TOTAL.labels(
                        event_type=event_type
                    ).inc(count)

                first_offset = min(
                    message.offset
                    for message in messages
                )

                last_offset = max(
                    message.offset
                    for message in messages
                )

                print(
                    "Normalizer batch tamamlandı | "
                    f"Raw: {len(messages)} | "
                    f"Normalized: "
                    f"{normalized_count} | "
                    f"DLQ: {dlq_count} | "
                    f"Offset: "
                    f"{first_offset}-"
                    f"{last_offset}",
                    flush=True,
                )

            except Exception as error:
                error_type = (
                    type(error).__name__
                )

                PROCESSING_ERRORS_TOTAL.labels(
                    error_type=error_type
                ).inc()

                print(
                    "Normalizer batch hatası | "
                    f"Mesaj sayısı: "
                    f"{len(messages)} | "
                    f"Hata: "
                    f"{error_type}: {error}",
                    flush=True,
                )

                # Commit yapılmaz.
                # Restart sonrası batch tekrar
                # Kafka'dan okunur.
                raise

    except asyncio.CancelledError:
        print(
            "Normalizer Service "
            "durduruluyor...",
            flush=True,
        )
        raise

    finally:
        monitoring_state.set_ready(
            False
        )

        if consumer_started:
            await consumer.stop()

        if producer_started:
            await producer.stop()

        await monitoring_runner.cleanup()

        print(
            "Normalizer Service "
            "bağlantıları kapatıldı.",
            flush=True,
        )


def main() -> None:
    asyncio.run(
        run_normalizer()
    )


if __name__ == "__main__":
    main()