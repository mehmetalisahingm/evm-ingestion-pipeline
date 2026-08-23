import asyncio
import json
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
    PreparedProcessing,
    commit_prepared_batch,
    prepare_message,
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
    fallback_id = (
        f"{message.topic}:"
        f"{message.partition}:"
        f"{message.offset}"
    )

    try:
        raw_event = json.loads(
            message.value.decode("utf-8")
        )

        if not isinstance(raw_event, dict):
            return None, fallback_id, None

        event_type = raw_event.get("event_type")
        event_id = raw_event.get("event_id")

        if not isinstance(event_type, str):
            event_type = None

        if not isinstance(event_id, str):
            event_id = fallback_id

        return event_type, event_id, raw_event

    except Exception:
        return None, fallback_id, None


async def run() -> None:
    consumer = ReorgKafkaConsumer()
    producer = ReorgKafkaProducer()

    state_store = RedisStateStore(
        redis_url=settings.redis_url,
        window_size=settings.redis_window_size,
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

        async def prepare_child(
            message: Any,
            raw_event: dict[str, Any] | None,
        ) -> PreparedProcessing:
            async with semaphore:
                return await prepare_message(
                    message,
                    reorg_service=reorg_service,
                    producer=producer,
                    raw_event=raw_event,
                )

        while True:
            messages = await consumer.get_batch(
                timeout_ms=BATCH_TIMEOUT_MS,
                max_records=BATCH_SIZE,
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

            parsed_messages: list[
                tuple[
                    Any,
                    str | None,
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

                pending = list(child_messages)
                child_messages.clear()

                while pending:
                    seen_event_ids: set[str] = set()

                    wave: list[
                        tuple[
                            Any,
                            str,
                            dict[str, Any] | None,
                        ]
                    ] = []

                    deferred: list[
                        tuple[
                            Any,
                            str,
                            dict[str, Any] | None,
                        ]
                    ] = []

                    for (
                        message,
                        event_id,
                        raw_event,
                    ) in pending:
                        if event_id in seen_event_ids:
                            deferred.append(
                                (
                                    message,
                                    event_id,
                                    raw_event,
                                )
                            )
                            continue

                        seen_event_ids.add(event_id)
                        wave.append(
                            (
                                message,
                                event_id,
                                raw_event,
                            )
                        )

                    tasks = [
                        prepare_child(
                            message,
                            raw_event,
                        )
                        for (
                            message,
                            _event_id,
                            raw_event,
                        ) in wave
                    ]

                    results = await asyncio.gather(
                        *tasks,
                        return_exceptions=True,
                    )

                    prepared_wave: list[
                        PreparedProcessing
                    ] = []

                    first_error: BaseException | None = None

                    for result in results:
                        if isinstance(
                            result,
                            BaseException,
                        ):
                            if first_error is None:
                                first_error = result
                            continue

                        prepared_wave.append(result)

                    if first_error is not None:
                        raise first_error

                    await commit_prepared_batch(
                        prepared_wave,
                        producer=producer,
                        state_store=state_store,
                    )

                    for prepared in prepared_wave:
                        result = prepared.result

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

                    pending = deferred

            try:
                event_ids_to_prefetch: list[str] = []

                for message in messages:
                    (
                        event_type,
                        event_id,
                        raw_event,
                    ) = get_event_metadata(message)

                    parsed_messages.append(
                        (
                            message,
                            event_type,
                            event_id,
                            raw_event,
                        )
                    )

                    if (
                        raw_event is not None
                        and isinstance(
                            raw_event.get("event_id"),
                            str,
                        )
                    ):
                        event_ids_to_prefetch.append(
                            event_id
                        )

                await state_store.prefetch_event_states(
                    event_ids_to_prefetch
                )

                for (
                    message,
                    event_type,
                    event_id,
                    raw_event,
                ) in parsed_messages:
                    NORMALIZED_MESSAGES_TOTAL.inc()

                    if event_type == "block":
                        await flush_children()

                        result = await process_message(
                            message,
                            reorg_service=reorg_service,
                            producer=producer,
                            state_store=state_store,
                            raw_event=raw_event,
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

                await flush_children()

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
                    f"Normalized: {len(messages)} | "
                    f"Canonical: {canonical_count} | "
                    f"Duplicate: {duplicate_count} | "
                    f"Reorg: {reorg_count} | "
                    f"DLQ: {dlq_count} | "
                    f"Offset: {first_offset}-"
                    f"{last_offset}",
                    flush=True,
                )

            except Exception as error:
                PROCESSING_ERRORS_TOTAL.inc()

                print(
                    "Re-org batch hatası | "
                    f"Mesaj sayısı: {len(messages)} | "
                    f"Hata: "
                    f"{type(error).__name__}: "
                    f"{error}",
                    flush=True,
                )

                raise

            finally:
                state_store.clear_prefetched_event_states()

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
        print(
            "\nRe-org Service durduruldu"
        )


if __name__ == "__main__":
    main()
