import asyncio
from datetime import datetime, timezone
from typing import Any

import aiohttp
from redis.asyncio import Redis

from app.kafka.producer import KafkaProducerService
from app.monitoring.metrics import (
    BACKFILL_BLOCKS_TOTAL,
    BLOCK_QUEUE_SIZE,
    BLOCKS_PUBLISHED_TOTAL,
    CHECKPOINT_BLOCK,
    LOGS_PUBLISHED_TOTAL,
)
from app.monitoring.server import (
    monitoring_state,
    start_monitoring_server,
)
from app.rpc.http_client import (
    get_block_by_number,
    get_block_receipts,
    get_latest_block_number,
)
from app.rpc.websocket_client import stream_new_blocks
from app.settings import (
    BACKFILL_CONCURRENCY,
    BLOCK_QUEUE_MAX_SIZE,
    CHAIN_ID,
    INGESTION_TARGET_EVENTS_PER_SECOND,
    MONITORING_PORT,
)
from app.storage.checkpoint import (
    get_checkpoint,
    save_checkpoint,
)
from app.storage.redis_client import (
    create_redis_client,
)


HTTP_POLL_INTERVAL_SECONDS = 2
HTTP_FALLBACK_DURATION_SECONDS = 30


def create_raw_block_event(
    block: dict,
) -> dict:
    return {
        "schema_version": 1,
        "chain_id": CHAIN_ID,
        "data_type": "block",
        "block_number": int(
            block["number"],
            16,
        ),
        "block_hash": block["hash"],
        "parent_hash": block["parentHash"],
        "received_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "payload": block,
    }


def create_raw_log_event(
    log: dict,
) -> dict:
    return {
        "schema_version": 1,
        "chain_id": CHAIN_ID,
        "data_type": "log",
        "block_number": int(
            log["blockNumber"],
            16,
        ),
        "block_hash": log["blockHash"],
        "transaction_hash": log[
            "transactionHash"
        ],
        "log_index": int(
            log["logIndex"],
            16,
        ),
        "removed": log.get(
            "removed",
            False,
        ),
        "received_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "payload": log,
    }


def extract_logs_from_receipts(
    receipts: list[dict[str, Any]],
) -> list[dict]:
    logs: list[dict] = []

    for receipt in receipts:
        receipt_logs = receipt.get(
            "logs",
            [],
        )

        if not isinstance(
            receipt_logs,
            list,
        ):
            continue

        logs.extend(receipt_logs)

    return logs


def estimate_normalized_event_count(
    bundle: tuple[dict, list[dict]],
) -> int:
    """
    Bir raw block bundle'ının Normalizer sonrasında
    yaklaşık kaç event üreteceğini hesaplar.

    1 block event + transaction eventleri + log eventleri.
    """

    block_event, log_events = bundle

    transactions = (
        block_event.get("payload", {})
        .get("transactions", [])
    )

    transaction_count = (
        len(transactions)
        if isinstance(transactions, list)
        else 0
    )

    return (
        1
        + transaction_count
        + len(log_events)
    )


async def create_block_bundle(
    *,
    session: aiohttp.ClientSession,
    block_number: int,
) -> tuple[dict, list[dict]]:
    block, receipts = await asyncio.gather(
        get_block_by_number(
            session=session,
            block_number=block_number,
        ),
        get_block_receipts(
            session=session,
            block_number=block_number,
        ),
    )

    raw_logs = extract_logs_from_receipts(
        receipts
    )

    block_event = create_raw_block_event(
        block
    )

    log_events = [
        create_raw_log_event(log)
        for log in raw_logs
    ]

    return (
        block_event,
        log_events,
    )


async def backfill_blocks(
    block_queue: asyncio.Queue[
        tuple[dict, list[dict]]
    ],
    session: aiohttp.ClientSession,
    start_block: int,
    end_block: int,
) -> None:
    if start_block > end_block:
        return

    print(
        "Eksik bloklar düzenleniyor "
        f"{start_block} ---> {end_block} | "
        f"Concurrency: {BACKFILL_CONCURRENCY}",
        flush=True,
    )

    total = (
        end_block
        - start_block
        + 1
    )

    completed = 0

    for batch_start in range(
        start_block,
        end_block + 1,
        BACKFILL_CONCURRENCY,
    ):
        batch_end = min(
            batch_start
            + BACKFILL_CONCURRENCY,
            end_block + 1,
        )

        block_numbers = list(
            range(
                batch_start,
                batch_end,
            )
        )

        bundles = await asyncio.gather(
            *[
                create_block_bundle(
                    session=session,
                    block_number=block_number,
                )
                for block_number
                in block_numbers
            ]
        )

        for bundle in bundles:
            await block_queue.put(
                bundle
            )

            BACKFILL_BLOCKS_TOTAL.inc()

            BLOCK_QUEUE_SIZE.set(
                block_queue.qsize()
            )

            completed += 1

        if (
            completed % 100
            < BACKFILL_CONCURRENCY
            or completed == total
        ):
            print(
                "Backfill ilerlemesi "
                f"{completed}/{total} "
                "blok kuyruğa eklendi.",
                flush=True,
            )


async def http_poll_fallback(
    block_queue: asyncio.Queue[
        tuple[dict, list[dict]]
    ],
    session: aiohttp.ClientSession,
    next_expected_block: int,
) -> int:
    print(
        "WebSocket kullanılamıyor. "
        "HTTP polling fallback aktif.",
        flush=True,
    )

    loop = asyncio.get_running_loop()

    fallback_end_time = (
        loop.time()
        + HTTP_FALLBACK_DURATION_SECONDS
    )

    while loop.time() < fallback_end_time:
        latest_block = (
            await get_latest_block_number(
                session
            )
        )

        if latest_block >= next_expected_block:
            await backfill_blocks(
                block_queue=block_queue,
                session=session,
                start_block=next_expected_block,
                end_block=latest_block,
            )

            next_expected_block = (
                latest_block + 1
            )

        await asyncio.sleep(
            HTTP_POLL_INTERVAL_SECONDS
        )

    print(
        "HTTP polling turu tamamlandı. "
        "WebSocket tekrar denenecek. "
        "Beklenen blok: "
        f"{next_expected_block}",
        flush=True,
    )

    return next_expected_block


async def ingest_blocks(
    block_queue: asyncio.Queue[
        tuple[dict, list[dict]]
    ],
    session: aiohttp.ClientSession,
    next_expected_block: int,
) -> None:
    while True:
        try:
            print(
                "WebSocket newHeads "
                "bağlantısı deneniyor...",
                flush=True,
            )

            async for block_header in (
                stream_new_blocks()
            ):
                block_number = int(
                    block_header["number"],
                    16,
                )

                if block_number < next_expected_block:
                    print(
                        "Eski blok atlandı "
                        f"{block_number}",
                        flush=True,
                    )
                    continue

                if block_number > next_expected_block:
                    await backfill_blocks(
                        block_queue=block_queue,
                        session=session,
                        start_block=next_expected_block,
                        end_block=(
                            block_number - 1
                        ),
                    )

                bundle = (
                    await create_block_bundle(
                        session=session,
                        block_number=block_number,
                    )
                )

                await block_queue.put(
                    bundle
                )

                BLOCK_QUEUE_SIZE.set(
                    block_queue.qsize()
                )

                next_expected_block = (
                    block_number + 1
                )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            print(
                "WebSocket canlı akış "
                "kullanılamıyor: "
                f"{error}",
                flush=True,
            )

            try:
                next_expected_block = (
                    await http_poll_fallback(
                        block_queue=block_queue,
                        session=session,
                        next_expected_block=(
                            next_expected_block
                        ),
                    )
                )

            except asyncio.CancelledError:
                raise

            except Exception as fallback_error:
                print(
                    "HTTP polling fallback "
                    "sırasında hata oluştu: "
                    f"{fallback_error}. "
                    "5 saniye sonra tekrar "
                    "denenecek.",
                    flush=True,
                )

                await asyncio.sleep(5)


async def publish_block_bundles(
    block_queue: asyncio.Queue[
        tuple[dict, list[dict]]
    ],
    producer: KafkaProducerService,
    redis_client: Redis,
) -> None:
    last_checkpoint = (
        await get_checkpoint(
            redis_client
        )
    )

    loop = asyncio.get_running_loop()
    next_publish_at = loop.time()

    print(
        "Ingestion publish hedefi: "
        f"{INGESTION_TARGET_EVENTS_PER_SECOND:.0f} "
        "normalized event/s",
        flush=True,
    )

    while True:
        bundle = await block_queue.get()
        block_event, log_events = bundle

        BLOCK_QUEUE_SIZE.set(
            block_queue.qsize()
        )

        try:
            block_number = (
                block_event[
                    "block_number"
                ]
            )

            if last_checkpoint is not None:
                expected_block = (
                    last_checkpoint + 1
                )

                if block_number != expected_block:
                    raise RuntimeError(
                        "Blok sırası bozuldu. "
                        f"Beklenen: "
                        f"{expected_block}, "
                        f"Gelen: "
                        f"{block_number}"
                    )

            estimated_events = (
                estimate_normalized_event_count(
                    bundle
                )
            )

            now = loop.time()

            if next_publish_at > now:
                await asyncio.sleep(
                    next_publish_at - now
                )
                now = loop.time()

            events = [
                block_event,
                *log_events,
            ]

            await producer.send_events(
                events
            )

            pacing_seconds = (
                estimated_events
                / INGESTION_TARGET_EVENTS_PER_SECOND
            )

            next_publish_at = (
                max(
                    next_publish_at,
                    now,
                )
                + pacing_seconds
            )

            if log_events:
                LOGS_PUBLISHED_TOTAL.inc(
                    len(log_events)
                )

            await save_checkpoint(
                redis_client=redis_client,
                block_number=block_number,
            )

            last_checkpoint = block_number

            transaction_count = len(
                block_event[
                    "payload"
                ]["transactions"]
            )

            print(
                "Tam blok Kafka'ya "
                "gönderildi: "
                f"{block_number} | "
                "Transaction: "
                f"{transaction_count} | "
                "Log: "
                f"{len(log_events)} | "
                "Tahmini normalized: "
                f"{estimated_events} | "
                "Checkpoint kaydedildi | "
                "Blok kuyruğu: "
                f"{block_queue.qsize()}/"
                f"{block_queue.maxsize}",
                flush=True,
            )

            BLOCKS_PUBLISHED_TOTAL.inc()

            CHECKPOINT_BLOCK.set(
                block_number
            )

        finally:
            block_queue.task_done()


async def run_ingestion() -> None:
    producer = KafkaProducerService()

    redis_client = create_redis_client()

    monitoring_runner = (
        await start_monitoring_server(
            MONITORING_PORT
        )
    )

    block_queue: asyncio.Queue[
        tuple[dict, list[dict]]
    ] = asyncio.Queue(
        maxsize=BLOCK_QUEUE_MAX_SIZE
    )

    try:
        await producer.start()

        await redis_client.ping()

        monitoring_state.ready = True

        checkpoint = (
            await get_checkpoint(
                redis_client
            )
        )

        print(
            "Başlangıç checkpoint:",
            checkpoint,
            flush=True,
        )

        if checkpoint is not None:
            CHECKPOINT_BLOCK.set(
                checkpoint
            )

        async with (
            aiohttp.ClientSession()
            as session
        ):
            latest_block = (
                await get_latest_block_number(
                    session
                )
            )

            if checkpoint is None:
                next_expected_block = (
                    latest_block + 1
                )
            else:
                next_expected_block = (
                    checkpoint + 1
                )

            async with (
                asyncio.TaskGroup()
                as tasks
            ):
                tasks.create_task(
                    publish_block_bundles(
                        block_queue,
                        producer,
                        redis_client,
                    )
                )

                if next_expected_block <= latest_block:
                    await backfill_blocks(
                        block_queue=block_queue,
                        session=session,
                        start_block=next_expected_block,
                        end_block=latest_block,
                    )

                    await block_queue.join()

                    next_expected_block = (
                        latest_block + 1
                    )

                    print(
                        "Başlangıç backfill "
                        "tamamlandı. "
                        "Canlı moda "
                        "geçiliyor: "
                        f"{next_expected_block}",
                        flush=True,
                    )

                print(
                    "Canlı akışta "
                    "beklenen sıradaki "
                    "blok:",
                    next_expected_block,
                    flush=True,
                )

                tasks.create_task(
                    ingest_blocks(
                        block_queue,
                        session,
                        next_expected_block,
                    )
                )

    finally:
        monitoring_state.ready = False

        await producer.stop()

        await redis_client.aclose()

        await monitoring_runner.cleanup()
