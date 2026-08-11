import json

from app.normalizer.exceptions import NormalizationError
from app.normalizer.service import normalize_raw_message


def test_block_normalization() -> None:
    raw_block = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "block",
        "block_number": 100,
        "block_hash": "0xaaa100",
        "parent_hash": "0xaaa099",
        "received_at": "2026-08-04T20:00:00Z",
        "payload": {
            "number": "0x64",
            "hash": "0xAAA100",
            "parentHash": "0xAAA099",
            "timestamp": "0x68925d40",
            "transactions": [
                {
                    "hash": "0xabc001",
                    "transactionIndex": "0x0",
                    "from": "0xabc002",
                    "to": "0xabc003",
                    "value": "0xde0b6b3a7640000",
                    "gas": "0x5208",
                    "gasPrice": "0xb2d05e00",
                    "input": "0x",
                }
            ],
        },
    }

    events = normalize_raw_message(raw_block)

    assert len(events) == 2

    block_event = events[0]
    transaction_event = events[1]

    assert block_event["event_type"] == "block"
    assert block_event["schema_version"] == 1
    assert block_event["chain_id"] == 56
    assert block_event["block_number"] == 100
    assert block_event["block_hash"] == "0xaaa100"
    assert len(block_event["event_id"]) == 64

    assert (
        block_event["payload"]["parent_hash"]
        == "0xaaa099"
    )
    assert (
        block_event["payload"]["transactions_count"]
        == 1
    )
    assert block_event["payload"]["timestamp"].endswith("Z")

    assert transaction_event["event_type"] == "transaction"
    assert transaction_event["schema_version"] == 1
    assert transaction_event["chain_id"] == 56
    assert transaction_event["block_number"] == 100
    assert transaction_event["block_hash"] == "0xaaa100"
    assert len(transaction_event["event_id"]) == 64

    transaction_payload = transaction_event["payload"]

    assert (
        transaction_payload["transaction_hash"]
        == "0xabc001"
    )
    assert transaction_payload["transaction_index"] == 0
    assert transaction_payload["from_address"] == "0xabc002"
    assert transaction_payload["to_address"] == "0xabc003"

    assert (
        transaction_payload["value"]
        == "1000000000000000000"
    )
    assert transaction_payload["gas"] == 21000
    assert transaction_payload["gas_price"] == "3000000000"
    assert transaction_payload["input"] == "0x"

    print("\nBLOCK TESTİ BAŞARILI")
    print(
        json.dumps(
            events,
            indent=2,
            ensure_ascii=False,
        )
    )


def test_contract_creation_transaction() -> None:
    """
    Contract deployment işleminde to alanı null olabilir.
    """

    raw_block = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "block",
        "block_number": 101,
        "block_hash": "0xaaa101",
        "parent_hash": "0xaaa100",
        "received_at": "2026-08-04T20:00:00Z",
        "payload": {
            "number": "0x65",
            "hash": "0xAAA101",
            "parentHash": "0xAAA100",
            "timestamp": "0x68925d41",
            "transactions": [
                {
                    "hash": "0xabc010",
                    "transactionIndex": "0x0",
                    "from": "0xabc011",
                    "to": None,
                    "value": "0x0",
                    "gas": "0x100000",
                    "gasPrice": "0xb2d05e00",
                    "input": "0x60606040",
                }
            ],
        },
    }

    events = normalize_raw_message(raw_block)

    transaction_event = events[1]

    assert transaction_event["event_type"] == "transaction"
    assert transaction_event["payload"]["to_address"] is None
    assert transaction_event["payload"]["value"] == "0"
    assert transaction_event["payload"]["input"] == "0x60606040"

    print("\nCONTRACT CREATION TESTİ BAŞARILI")


def test_log_normalization() -> None:
    raw_log = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "log",
        "block_number": 100,
        "block_hash": "0xAAA100",
        "transaction_hash": "0xabc001",
        "log_index": 2,
        "removed": False,
        "received_at": "2026-08-04T20:00:01Z",
        "payload": {
            "blockNumber": "0x64",
            "blockHash": "0xAAA100",
            "transactionHash": "0xabc001",
            "logIndex": "0x2",
            "address": "0xabc004",
            "topics": [
                "0xabc005",
                "0xabc006",
            ],
            "data": "0x1234",
            "removed": False,
        },
    }

    events = normalize_raw_message(raw_log)

    assert len(events) == 1

    log_event = events[0]

    assert log_event["event_type"] == "log"
    assert log_event["schema_version"] == 1
    assert log_event["chain_id"] == 56
    assert log_event["block_number"] == 100
    assert log_event["block_hash"] == "0xaaa100"
    assert len(log_event["event_id"]) == 64

    log_payload = log_event["payload"]

    assert log_payload["transaction_hash"] == "0xabc001"
    assert log_payload["log_index"] == 2
    assert log_payload["contract_address"] == "0xabc004"
    assert log_payload["topics"] == [
        "0xabc005",
        "0xabc006",
    ]
    assert log_payload["data"] == "0x1234"
    assert log_payload["removed"] is False

    print("\nLOG TESTİ BAŞARILI")
    print(
        json.dumps(
            events,
            indent=2,
            ensure_ascii=False,
        )
    )


def test_removed_log() -> None:
    """
    Re-org durumunda RPC sağlayıcısı removed=true log gönderebilir.
    """

    raw_log = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "log",
        "block_number": 102,
        "block_hash": "0xaaa102",
        "transaction_hash": "0xabc020",
        "log_index": 0,
        "removed": True,
        "received_at": "2026-08-04T20:00:02Z",
        "payload": {
            "blockNumber": "0x66",
            "blockHash": "0xAAA102",
            "transactionHash": "0xabc020",
            "logIndex": "0x0",
            "address": "0xabc021",
            "topics": [],
            "data": "0x",
            "removed": True,
        },
    }

    events = normalize_raw_message(raw_log)

    assert len(events) == 1
    assert events[0]["payload"]["removed"] is True
    assert events[0]["payload"]["topics"] == []

    print("\nREMOVED LOG TESTİ BAŞARILI")


def test_deterministic_event_id() -> None:
    """
    Aynı veri tekrar geldiğinde aynı event_id üretilmelidir.
    """

    raw_log = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "log",
        "block_number": 100,
        "block_hash": "0xAAA100",
        "transaction_hash": "0xabc001",
        "log_index": 2,
        "removed": False,
        "received_at": "2026-08-04T20:00:01Z",
        "payload": {
            "blockNumber": "0x64",
            "blockHash": "0xAAA100",
            "transactionHash": "0xabc001",
            "logIndex": "0x2",
            "address": "0xabc004",
            "topics": ["0xabc005"],
            "data": "0x1234",
            "removed": False,
        },
    }

    first_event = normalize_raw_message(raw_log)[0]
    second_event = normalize_raw_message(raw_log)[0]

    assert first_event["event_id"] == second_event["event_id"]

    print("\nDETERMİNİSTİK EVENT_ID TESTİ BAŞARILI")


def test_missing_block_hash() -> None:
    invalid_message = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "block",
        "payload": {
            "number": "0x64",
            # hash bilerek eksik bırakıldı.
            "parentHash": "0xaaa099",
            "timestamp": "0x68925d40",
            "transactions": [],
        },
    }

    try:
        normalize_raw_message(invalid_message)

    except NormalizationError as error:
        assert "payload.hash" in str(error)

        print("\nEKSİK BLOCK HASH TESTİ BAŞARILI")
        print("Yakalanan hata:", error)
        return

    raise AssertionError(
        "Eksik block hash NormalizationError üretmeliydi"
    )


def test_invalid_hexadecimal() -> None:
    invalid_message = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "log",
        "block_number": 100,
        "block_hash": "0xaaa100",
        "transaction_hash": "0xabc001",
        "log_index": 0,
        "removed": False,
        "payload": {
            "blockNumber": "0x64",
            "blockHash": "0xaaa100",
            "transactionHash": "0xTX001",
            "logIndex": "0x0",
            "address": "0xabc004",
            "topics": [],
            "data": "0x",
            "removed": False,
        },
    }

    try:
        normalize_raw_message(invalid_message)

    except NormalizationError as error:
        assert "hexadecimal" in str(error)

        print("\nGEÇERSİZ HEX TESTİ BAŞARILI")
        print("Yakalanan hata:", error)
        return

    raise AssertionError(
        "Geçersiz hexadecimal değer hata üretmeliydi"
    )


def test_unknown_event_type() -> None:
    invalid_message = {
        "schema_version": 1,
        "chain_id": 56,
        "data_type": "receipt",
        "payload": {},
    }

    try:
        normalize_raw_message(invalid_message)

    except NormalizationError as error:
        assert "Desteklenmeyen data_type" in str(error)

        print("\nDESTEKLENMEYEN EVENT TESTİ BAŞARILI")
        print("Yakalanan hata:", error)
        return

    raise AssertionError(
        "Bilinmeyen data_type hata üretmeliydi"
    )


def main() -> None:
    test_block_normalization()
    test_contract_creation_transaction()
    test_log_normalization()
    test_removed_log()
    test_deterministic_event_id()
    test_missing_block_hash()
    test_invalid_hexadecimal()
    test_unknown_event_type()

    print("\n======================================")
    print("TÜM NORMALIZER SMOKE TESTLERİ BAŞARILI")
    print("======================================")


if __name__ == "__main__":
    main()
