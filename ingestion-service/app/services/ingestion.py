import asyncio
from datetime import datetime, timezone
from redis.asyncio import Redis
import aiohttp

from app.kafka.producer import KafkaProducerService
from app.rpc.http_client import get_block_by_number,get_latest_block_number
from app.rpc.websocket_client import stream_logs, stream_new_blocks
from app.settings import CHAIN_ID, BLOCK_QUEUE_MAX_SIZE,LOG_QUEUE_MAX_SIZE,MONITORING_PORT
from app.storage.checkpoint import get_checkpoint, save_checkpoint
from app.storage.redis_client import create_redis_client
from app.monitoring.server import (
    monitoring_state,
    start_monitoring_server,
)
from app.monitoring.metrics import (
    BACKFILL_BLOCKS_TOTAL,
    BLOCK_QUEUE_SIZE,
    BLOCKS_PUBLISHED_TOTAL,
    CHECKPOINT_BLOCK,
    LOG_QUEUE_SIZE,
    LOGS_PUBLISHED_TOTAL,
)

#1
def create_raw_block_event(block: dict) -> dict:
    return {
        "schema_version": 1,
        "chain_id": CHAIN_ID,
        "data_type": "block",
        "block_number": int(block["number"], 16),
        "block_hash": block["hash"],
        "parent_hash": block["parentHash"],
        "received_at": datetime.now(timezone.utc).isoformat(),
        "payload": block,
    }


#2
async def backfill_blocks(
    block_queue: asyncio.Queue[dict],
    session: aiohttp.ClientSession,
    start_block: int,
    end_block: int,
) -> None:
    if start_block > end_block:
        return

    print(
        f"eksik bloklar düzenleniyor  "
        f"{start_block} ---> {end_block}"
    )

    total = end_block - start_block + 1

    for block_number in range(start_block, end_block + 1):
        full_block = await get_block_by_number(
            session=session,
            block_number=block_number,
        )

        event = create_raw_block_event(full_block)
        await block_queue.put(event)
        BACKFILL_BLOCKS_TOTAL.inc()
        BLOCK_QUEUE_SIZE.set(block_queue.qsize())

        completed = block_number - start_block + 1

        if completed % 100 == 0 or block_number == end_block:
            print(
                f"Backfill ilerlemesi şu kadar  "
                f"{completed}/{total} blok kuyruğa eklendi."
            )


#3
def create_raw_log_event(log: dict) -> dict:
    return {
        "schema_version": 1,
        "chain_id": CHAIN_ID,
        "data_type": "log",
        "block_number": int(log["blockNumber"], 16),
        "block_hash": log["blockHash"],
        "transaction_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "removed": log.get("removed", False),
        "received_at": datetime.now(timezone.utc).isoformat(),
        "payload": log,
    }


#4
async def ingest_blocks(
    block_queue: asyncio.Queue[dict],
    session: aiohttp.ClientSession,
    next_expected_block: int,
) -> None:
    async for block_header in stream_new_blocks():
        block_number = int(block_header["number"], 16)

        if block_number < next_expected_block:
            print(f"eski blok atlandı {block_number}")
            continue

        if block_number > next_expected_block:
            await backfill_blocks(
                block_queue=block_queue,
                session=session,
                start_block=next_expected_block,
                end_block=block_number - 1,
            )

        full_block = await get_block_by_number(
            session=session,
            block_number=block_number,
        )

        event = create_raw_block_event(full_block)
        await block_queue.put(event)
        BLOCK_QUEUE_SIZE.set(block_queue.qsize())

        next_expected_block = block_number + 1



#5    
async def ingest_logs(
    log_queue: asyncio.Queue[dict],
) -> None:
    async for log in stream_logs():
        event = create_raw_log_event(log)
        await log_queue.put(event)
        LOG_QUEUE_SIZE.set(log_queue.qsize())




#6        
async def publish_blocks(
    block_queue: asyncio.Queue[dict],
    producer: KafkaProducerService,
    redis_client: Redis,
) -> None:
    last_checkpoint = await get_checkpoint(redis_client)

    while True:
        event = await block_queue.get()
        BLOCK_QUEUE_SIZE.set(block_queue.qsize())

        try:
            block_number = event["block_number"]

            if last_checkpoint is not None:
                expected_block = last_checkpoint + 1

                if block_number != expected_block:
                    raise RuntimeError(
                        f"Blok sırası bozuldu. "
                        f"Beklenen: {expected_block}, "
                        f"Gelen: {block_number}"
                    )

            await producer.send_event(event)

            await save_checkpoint(
                redis_client=redis_client,
                block_number=block_number,
            )

            last_checkpoint = block_number

            transaction_count = len(
                event["payload"]["transactions"]
            )

            print(
                f"Tam blok Kafka'ya gönderildi: "
                f"{block_number} | "
                f"Transaction: {transaction_count} | "
                f"Checkpoint kaydedildi | "
                f"Blok kuyruğu: "
                f"{block_queue.qsize()}/{block_queue.maxsize}"
            )
            BLOCKS_PUBLISHED_TOTAL.inc()
            CHECKPOINT_BLOCK.set(block_number)
        finally:
            block_queue.task_done()




#7
async def publish_logs(
    log_queue: asyncio.Queue[dict],
    producer: KafkaProducerService,
) -> None:
    log_count = 0

    while True:
        event = await log_queue.get()
        LOG_QUEUE_SIZE.set(log_queue.qsize())

        try:
            await producer.send_event(event)
            log_count += 1

            if log_count % 500 == 0:
                print(
                    f"{log_count} log Kafka'ya gönderildi. "
                    f"Log kuyruğu: "
                    f"{log_queue.qsize()}/{log_queue.maxsize}"
                )
            LOGS_PUBLISHED_TOTAL.inc()

        finally:
            log_queue.task_done()




#8
async def run_ingestion() -> None:
    producer = KafkaProducerService()
    redis_client = create_redis_client()

    monitoring_runner = await start_monitoring_server(
        MONITORING_PORT
    )

    block_queue: asyncio.Queue[dict] = asyncio.Queue(
        maxsize=BLOCK_QUEUE_MAX_SIZE
    )

    log_queue: asyncio.Queue[dict] = asyncio.Queue(
        maxsize=LOG_QUEUE_MAX_SIZE
    )

    try:
        await producer.start()
        await redis_client.ping()

        monitoring_state.ready = True

        checkpoint = await get_checkpoint(redis_client)
        
        print("Başlangıç checkpoint:", checkpoint)
        if checkpoint is not None:
            CHECKPOINT_BLOCK.set(checkpoint)

        async with aiohttp.ClientSession() as session:
            latest_block = await get_latest_block_number(session)

            if checkpoint is None:
                next_expected_block = latest_block + 1
            else:
                next_expected_block = checkpoint + 1

            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(
                    publish_blocks(
                        block_queue,
                        producer,
                        redis_client,
                    )
                )

                tasks.create_task(
                    publish_logs(
                        log_queue,
                        producer,
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

                    next_expected_block = latest_block + 1

                    print(
                        "Başlangıç backfill tamamlandı. "
                        f"Canlı moda geçiliyor: "
                        f"{next_expected_block}"
                    )

                print(
                    "Canlı akışta beklenen sıradaki blok:",
                    next_expected_block,
                )

                tasks.create_task(
                    ingest_blocks(
                        block_queue,
                        session,
                        next_expected_block,
                    )
                )

                tasks.create_task(
                    ingest_logs(log_queue)
                )

    finally:
        monitoring_state.ready = False

        await producer.stop()
        await redis_client.aclose()
        await monitoring_runner.cleanup()