import asyncio
import hashlib
import json
import time
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition


KAFKA = "localhost:9092"
INPUT_TOPIC = "evm.normalized"
OUTPUT_TOPIC = "canonical-events"


def sha256(value: str) -> str:
    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def block_hash(value: str) -> str:
    return "0x" + sha256(value)


def now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def make_block(
    *,
    chain_id: int,
    number: int,
    hash_value: str,
    parent_hash: str,
) -> dict:
    event_id = sha256(
        f"block|{chain_id}|{hash_value}"
    )

    return {
        "schema_version": 1,
        "event_id": event_id,
        "event_type": "block",
        "chain_id": chain_id,
        "block_number": number,
        "block_hash": hash_value,
        "normalized_at": now(),
        "payload": {
            "parent_hash": parent_hash,
            "transactions_count": 1,
            "timestamp": now(),
        },
    }


def make_transaction(
    *,
    chain_id: int,
    block_number: int,
    block_hash_value: str,
    name: str,
) -> dict:
    tx_hash = block_hash(
        f"tx-{name}"
    )

    event_id = sha256(
        f"transaction|{chain_id}|"
        f"{block_hash_value}|{tx_hash}"
    )

    return {
        "schema_version": 1,
        "event_id": event_id,
        "event_type": "transaction",
        "chain_id": chain_id,
        "block_number": block_number,
        "block_hash": block_hash_value,
        "normalized_at": now(),
        "payload": {
            "transaction_hash": tx_hash,
            "transaction_index": 0,
            "from_address": (
                "0x1111111111111111111111111111111111111111"
            ),
            "to_address": (
                "0x2222222222222222222222222222222222222222"
            ),
            "value": "100",
            "gas": 21000,
            "gas_price": "1",
            "input": "0x",
        },
    }


async def send_event(
    producer: AIOKafkaProducer,
    event: dict,
) -> None:
    await producer.send_and_wait(
        INPUT_TOPIC,
        key=str(
            event["chain_id"]
        ).encode("utf-8"),
        value=json.dumps(
            event,
            separators=(",", ":"),
        ).encode("utf-8"),
    )


async def main() -> None:
    # Her test çalıştırmasında farklı chain_id.
    # Böylece gerçek BSC verisine ve eski teste dokunmuyoruz.
    chain_id = (
        900000
        + int(time.time()) % 90000
    )

    parent = block_hash(
        f"{chain_id}-parent"
    )

    block_a_hash = block_hash(
        f"{chain_id}-A"
    )

    block_b_hash = block_hash(
        f"{chain_id}-B"
    )

    block_c_hash = block_hash(
        f"{chain_id}-C"
    )

    block_a = make_block(
        chain_id=chain_id,
        number=100,
        hash_value=block_a_hash,
        parent_hash=parent,
    )

    tx_a = make_transaction(
        chain_id=chain_id,
        block_number=100,
        block_hash_value=block_a_hash,
        name="A",
    )

    block_b = make_block(
        chain_id=chain_id,
        number=101,
        hash_value=block_b_hash,
        parent_hash=block_a_hash,
    )

    tx_b = make_transaction(
        chain_id=chain_id,
        block_number=101,
        block_hash_value=block_b_hash,
        name="B",
    )

    # Aynı 101. blok yüksekliği,
    # fakat farklı hash.
    # Parent yine A.
    block_c = make_block(
        chain_id=chain_id,
        number=101,
        hash_value=block_c_hash,
        parent_hash=block_a_hash,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA,
        acks="all",
        enable_idempotence=True,
    )

    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA,
        enable_auto_commit=False,
    )

    await producer.start()
    await consumer.start()

    tp = TopicPartition(
        OUTPUT_TOPIC,
        0,
    )

    consumer.assign([tp])

    # Test başlamadan önce canonical-events
    # topic'inin sonuna git.
    await consumer.seek_to_end(tp)

    start_offset = await consumer.position(tp)

    print(
        f"Test chain_id: {chain_id}"
    )

    print("1) Block A gönderiliyor")
    await send_event(
        producer,
        block_a,
    )

    await asyncio.sleep(0.2)

    print("2) Transaction A gönderiliyor")
    await send_event(
        producer,
        tx_a,
    )

    await asyncio.sleep(0.2)

    print("3) Block B gönderiliyor")
    await send_event(
        producer,
        block_b,
    )

    await asyncio.sleep(0.2)

    print("4) Transaction B gönderiliyor")
    await send_event(
        producer,
        tx_b,
    )

    await asyncio.sleep(0.2)

    print(
        "5) Alternatif Block C gönderiliyor "
        "-> RE-ORG bekleniyor"
    )

    await send_event(
        producer,
        block_c,
    )

    await asyncio.sleep(0.5)

    print(
        "6) Block C tekrar gönderiliyor "
        "-> DUPLICATE bekleniyor"
    )

    await send_event(
        producer,
        block_c,
    )

    # Test çıktısını bizim başlangıç
    # offsetimizden itibaren oku.
    consumer.seek(
        tp,
        start_offset,
    )

    collected = []

    deadline = (
        asyncio.get_running_loop().time()
        + 10
    )

    while (
        asyncio.get_running_loop().time()
        < deadline
    ):
        batches = await consumer.getmany(
            timeout_ms=500,
            max_records=500,
        )

        for messages in batches.values():
            for message in messages:
                try:
                    data = json.loads(
                        message.value.decode(
                            "utf-8"
                        )
                    )
                except Exception:
                    continue

                if (
                    data.get("chain_id")
                    == chain_id
                ):
                    collected.append(data)

        if len(collected) >= 7:
            # Duplicate mesajın yanlışlıkla
            # çıktı üretmediğinden emin olmak
            # için biraz daha bekle.
            await asyncio.sleep(1)
            break

    await producer.stop()
    await consumer.stop()

    print()
    print(
        f"Canonical çıktı sayısı: "
        f"{len(collected)}"
    )

    for event in collected:
        print(
            event["event_type"],
            event["block_number"],
            event["block_hash"][:12],
            "canonical=",
            event["canonical"],
            "version=",
            event["version"],
        )

    def states(event_id: str):
        return [
            (
                event["canonical"],
                event["version"],
            )
            for event in collected
            if event["event_id"]
            == event_id
        ]

    assert states(
        block_a["event_id"]
    ) == [
        (True, 1)
    ]

    assert states(
        tx_a["event_id"]
    ) == [
        (True, 1)
    ]

    # Eski 101 numaralı blok önce canonical,
    # re-org sonrasında orphan olmalı.
    assert states(
        block_b["event_id"]
    ) == [
        (True, 1),
        (False, 2),
    ]

    # Eski bloktaki transaction da
    # canonical=false olmalı.
    assert states(
        tx_b["event_id"]
    ) == [
        (True, 1),
        (False, 2),
    ]

    # Yeni zincirdeki C yalnızca bir kere
    # üretilmeli. İkinci gönderim duplicate.
    assert states(
        block_c["event_id"]
    ) == [
        (True, 1)
    ]

    assert len(collected) == 7

    print()
    print(
        "✅ RE-ORG TEST BAŞARILI"
    )

    print(
        "✅ Eski block canonical=false version=2"
    )

    print(
        "✅ Eski transaction canonical=false version=2"
    )

    print(
        "✅ Yeni block canonical=true version=1"
    )

    print(
        "✅ Duplicate çıktı üretmedi"
    )


if __name__ == "__main__":
    asyncio.run(main())
