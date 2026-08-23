import asyncio
import json
from collections import defaultdict
from typing import Any

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
from app.reorg.processor import (
    ProcessingResult,
    process_message,
)
from app.reorg.service import ReorgService
from app.settings import settings
from app.state.redis_store import RedisStateStore


BATCH_SIZE = 500
BATCH_TIMEOUT_MS = 100

CHILD_CONCURRENCY = 100


def get_event_metadata(
    message: Any,
) -> tuple[
    str | None,
    str,
    dict[str, Any] | None,
]:
    """
    Kafka mesajını bir kere JSON olarak
    parse eder.

    event_type, event_id ve parse edilmiş
    raw event birlikte döndürülür.

    Böylece processor.py içinde aynı JSON
    tekrar parse edilmez.
    """

    fallback_id = (
        f"{message.topic}:"
        f"{message.partition}:"
        f"{message.offset}"
    )

    try:
        raw_event = json.loads(
            message.value.decode(
                "utf-8"
            )
        )

        if not isinstance(
            raw_event,
            dict,
        ):
            return (
                None,
                fallback_id,
                None,
            )

        event_type = raw_event.get(
            "event_type"
        )

        event_id = raw_event.get(
            "event_id"
        )

        if not isinstance(
            event_type,
            str,
        ):
            event_type = None

        if not isinstance(
            event_id,
            str,
        ):
            event_id = fallback_id

        return (
            event_type,
            event_id,
            raw_event,
        )

    except Exception:
        return (
            None,
            fallback_id,
            None,
        )


async def run() -> None:
    consumer = ReorgKafkaConsumer()
    producer = ReorgKafkaProducer()

    state_store = RedisStateStore(
        redis_url=settings.redis_url,
        window_size=(
            settings.redis_window_size
        ),
        pending_ttl_seconds=(
            settings.pending_event_ttl_seconds
        ),
    )

    reorg_service = ReorgService(
        state_store=state_store
    )

    monitoring_runner = None

    consumer_started = False
    producer_started = False
    redis_started = False

    SERVICE_READY.set(0)

    semaphore = asyncio.Semaphore(
        CHILD_CONCURRENCY
    )

    # Aynı event_id'nin iki kopyası
    # aynı anda işlenmesin.
    event_locks: dict[
        str,
        asyncio.Lock,
    ] = defaultdict(
        asyncio.Lock
    )

    try:
        monitoring_runner = (
            await start_monitoring_server(
                settings.monitoring_port
            )
        )

        await state_store.start()
        redis_started = True

        print(
            "Redis bağlantısı hazır",
            flush=True,
        )

        await producer.start()
        producer_started = True

        print(
            "Kafka producer hazır",
            flush=True,
        )

        await consumer.start()
        consumer_started = True

        print(
            "Kafka consumer hazır",
            flush=True,
        )

        SERVICE_READY.set(1)

        print(
            "Re-org Service çalışıyor | "
            f"Batch size: {BATCH_SIZE} | "
            f"Child concurrency: "
            f"{CHILD_CONCURRENCY}",
            flush=True,
        )

        async def process_child(
            message: Any,
            event_id: str,
            raw_event: dict[
                str,
                Any,
            ] | None,
        ) -> ProcessingResult:
            """
            Transaction ve log eventleri
            paralel çalışabilir.

            Aynı event_id'nin tekrar gelen
            kopyaları lock sayesinde aynı anda
            state değiştiremez.
            """

            async with semaphore:
                async with (
                    event_locks[event_id]
                ):
                    return await process_message(
                        message,
                        reorg_service=(
                            reorg_service
                        ),
                        producer=producer,
                        state_store=(
                            state_store
                        ),
                        raw_event=raw_event,
                    )

        while True:
            messages = (
                await consumer.get_batch(
                    timeout_ms=(
                        BATCH_TIMEOUT_MS
                    ),
                    max_records=BATCH_SIZE,
                )
            )

            if not messages:
                continue

            canonical_count = 0
            duplicate_count = 0
            dlq_count = 0
            reorg_count = 0

            child_messages: list[
                tuple[
                    Any,
                    str,
                    dict[str, Any] | None,
                ]
            ] = []

            async def flush_children() -> None:
                nonlocal canonical_count
                nonlocal duplicate_count
                nonlocal dlq_count
                nonlocal reorg_count

                if not child_messages:
                    return

                tasks = [
                    process_child(
                        message,
                        event_id,
                        raw_event,
                    )
                    for (
                        message,
                        event_id,
                        raw_event,
                    ) in child_messages
                ]

                results = (
                    await asyncio.gather(
                        *tasks,
                        return_exceptions=True,
                    )
                )

                child_messages.clear()

                first_error: (
                    BaseException | None
                ) = None

                for result in results:
                    if isinstance(
                        result,
                        BaseException,
                    ):
                        if first_error is None:
                            first_error = result

                        continue

                    canonical_count += (
                        result.canonical_count
                    )

                    duplicate_count += (
                        result.duplicate_count
                    )

                    if result.reorg_detected:
                        reorg_count += 1

                    if result.sent_to_dlq:
                        dlq_count += 1

                if first_error is not None:
                    raise first_error

            try:
                for message in messages:
                    NORMALIZED_MESSAGES_TOTAL.inc()

                    (
                        event_type,
                        event_id,
                        raw_event,
                    ) = get_event_metadata(
                        message
                    )

                    if event_type == "block":
                        # Önce önceki bloğa ait
                        # transaction ve log işleri
                        # tamamen bitsin.
                        await flush_children()

                        # Block eventleri mutlaka
                        # sırayla işlenir.
                        result = (
                            await process_message(
                                message,
                                reorg_service=(
                                    reorg_service
                                ),
                                producer=producer,
                                state_store=(
                                    state_store
                                ),
                                raw_event=(
                                    raw_event
                                ),
                            )
                        )

                        canonical_count += (
                            result.canonical_count
                        )

                        duplicate_count += (
                            result.duplicate_count
                        )

                        if result.reorg_detected:
                            reorg_count += 1

                        if result.sent_to_dlq:
                            dlq_count += 1

                    else:
                        child_messages.append(
                            (
                                message,
                                event_id,
                                raw_event,
                            )
                        )

                # Batch sonunda kalan transaction
                # ve log eventlerini tamamla.
                await flush_children()

                # Bütün batch başarıyla işlendi.
                # Partition başına tek offset
                # commit yapılır.
                await consumer.commit_batch(
                    messages
                )

                OFFSET_COMMITS_TOTAL.inc()

                CANONICAL_EVENTS_TOTAL.inc(
                    canonical_count
                )

                DUPLICATE_EVENTS_TOTAL.inc(
                    duplicate_count
                )

                if reorg_count:
                    REORG_DETECTED_TOTAL.inc(
                        reorg_count
                    )

                if dlq_count:
                    DLQ_MESSAGES_TOTAL.inc(
                        dlq_count
                    )

                first_offset = min(
                    message.offset
                    for message in messages
                )

                last_offset = max(
                    message.offset
                    for message in messages
                )

                print(
                    "Re-org batch tamamlandı | "
                    f"Normalized: "
                    f"{len(messages)} | "
                    f"Canonical: "
                    f"{canonical_count} | "
                    f"Duplicate: "
                    f"{duplicate_count} | "
                    f"Reorg: "
                    f"{reorg_count} | "
                    f"DLQ: "
                    f"{dlq_count} | "
                    f"Offset: "
                    f"{first_offset}-"
                    f"{last_offset}",
                    flush=True,
                )

            except Exception as error:
                PROCESSING_ERRORS_TOTAL.inc()

                print(
                    "Re-org batch hatası | "
                    f"Mesaj sayısı: "
                    f"{len(messages)} | "
                    f"Hata: "
                    f"{type(error).__name__}: "
                    f"{error}",
                    flush=True,
                )

                # Offset commit edilmez.
                #
                # Daha önce başarılı olmuş
                # eventler replay edilirse
                # Redis idempotency tarafından
                # duplicate olarak yakalanır.
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
        asyncio.run(
            run()
        )

    except KeyboardInterrupt:
        print(
            "\nRe-org Service durduruldu"
        )


if __name__ == "__main__":
    main()