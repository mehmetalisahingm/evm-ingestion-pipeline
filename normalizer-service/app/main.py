import asyncio

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
from app.normalizer.processor import process_kafka_message
from app.settings import settings


async def run_normalizer() -> None:
    """
    Normalizer Service çalışma döngüsü.

    evm.raw mesajlarını tüketir, normalize eder ve sonucu
    evm.normalized veya evm.dlq topic'ine gönderir.
    """

    consumer = NormalizerKafkaConsumer()
    producer = NormalizerKafkaProducer()

    consumer_started = False
    producer_started = False

    monitoring_runner = await start_monitoring_server(
        settings.monitoring_port
    )

    try:
        # Önce producer başlatılır. Böylece mesaj tüketmeye
        # başlamadan önce hedef topic'lere yazmaya hazır oluruz.
        await producer.start()
        producer_started = True

        await consumer.start()
        consumer_started = True

        monitoring_state.set_ready(True)

        print(
            "Normalizer Service çalışıyor | "
            f"Kaynak topic: {settings.raw_topic} | "
            f"Çıktı topic: {settings.normalized_topic} | "
            f"DLQ topic: {settings.dlq_topic}"
        )

        while True:
            message = await consumer.get_message()
            RAW_MESSAGES_TOTAL.inc()

            try:
                with PROCESSING_DURATION_SECONDS.time():
                    result = await process_kafka_message(
                        message=message,
                        producer=producer,
                    )

                if result.sent_to_dlq:
                    error_type = (
                        result.error_type
                        or "UnknownNormalizationError"
                    )

                    DLQ_MESSAGES_TOTAL.labels(
                        error_type=error_type
                    ).inc()

                    print(
                        "Mesaj DLQ'ya gönderildi | "
                        f"Topic: {message.topic} | "
                        f"Partition: {message.partition} | "
                        f"Offset: {message.offset} | "
                        f"Hata: {error_type}"
                    )

                else:
                    for event_type in result.event_types:
                        NORMALIZED_EVENTS_TOTAL.labels(
                            event_type=event_type
                        ).inc()

                    print(
                        "Mesaj normalize edildi | "
                        f"Offset: {message.offset} | "
                        f"Üretilen event: "
                        f"{result.normalized_count}"
                    )

                # Yalnızca evm.normalized veya evm.dlq gönderimi
                # başarılı olduktan sonra offset commit edilir.
                await consumer.commit_message(message)
                OFFSET_COMMITS_TOTAL.inc()

            except Exception as error:
                error_type = type(error).__name__

                PROCESSING_ERRORS_TOTAL.labels(
                    error_type=error_type
                ).inc()

                print(
                    "Normalizer mesaj işleme hatası | "
                    f"Topic: {message.topic} | "
                    f"Partition: {message.partition} | "
                    f"Offset: {message.offset} | "
                    f"Hata: {error_type}: {error}"
                )

                # Offset commit edilmeden servis durdurulur.
                # Container yeniden başladığında mesaj tekrar işlenir.
                raise

    except asyncio.CancelledError:
        print("Normalizer Service durduruluyor...")
        raise

    finally:
        monitoring_state.set_ready(False)

        if consumer_started:
            await consumer.stop()

        if producer_started:
            await producer.stop()

        await monitoring_runner.cleanup()

        print("Normalizer Service bağlantıları kapatıldı.")


def main() -> None:
    asyncio.run(run_normalizer())


if __name__ == "__main__":
    main()
