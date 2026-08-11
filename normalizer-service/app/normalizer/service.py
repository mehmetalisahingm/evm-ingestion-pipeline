from typing import Any
from pydantic import ValidationError as PydanticValidationError
from app.settings import settings
from .converters import (
    hex_to_int,
    integer_value_to_string,
    normalize_boolean,
    normalize_hex_string,
    timestamp_to_utc_iso,
)
from .event_id import (
    create_block_event_id,
    create_log_event_id,
    create_transaction_event_id,
)
from .exceptions import (
    UnsupportedEventTypeError,
    ValidationError,
)
from .schemas import (
    BlockPayload,
    LogPayload,
    NormalizedBlockEvent,
    NormalizedLogEvent,
    NormalizedTransactionEvent,
    TransactionPayload,
)

def _require_dict(
    value:Any,
    field_name:str,

)-> dict[str,Any]:
    if not isinstance(value,dict):
        raise ValidationError(
            f"{field_name} obje veya sözlük olmalıdır"
        )

    return value

def _require_list(
    value:Any,
    field_name:str,
)->list[Any]:

    if not isinstance(value,list):
        raise ValidationError(
            f"{field_name}liste olmalıdır"
        )

    return value

def _validate_raw_identity(
    raw_message: dict[str, Any],
    block_number: int,
    block_hash: str,
) -> None:
    raw_block_number=raw_message.get("block_number")

    if raw_block_number is not None:
        normalized_raw_number = hex_to_int(
            raw_block_number,
            "block_number",
        )
    if normalized_raw_number != block_number:
            raise ValidationError(
                "Envelope block_number ile "
                "payload.number birbiriyle uyuşmuyor"
            )
    raw_block_hash = raw_message.get("block_hash")

    if raw_block_hash is not None:
        normalized_raw_hash = normalize_hex_string(
            raw_block_hash,
            "block_hash",
        )

        if normalized_raw_hash != block_hash:
            raise ValidationError(
                "envelope block_hash ile "
                "payload.hash birbiriyle uyuşmuyor"
            )

def _normalize_transaction(
    transaction:Any,
    chain_id:int,
    block_number: int,
    block_hash: str,
)-> dict[str,Any]:
    transaction_data = _require_dict(
        transaction,
        "transaction",
    )

    transaction_hash = normalize_hex_string(
        transaction_data.get("hash"),
        "transaction.hash",
    )

    transaction_index = hex_to_int(
        transaction_data.get("transactionIndex"),
        "transaction.transactionIndex",
    )

    from_address = normalize_hex_string(
        transaction_data.get("from"),
        "transaction.from",
    )

    to_address = normalize_hex_string(
        transaction_data.get("to"),
        "transaction.to",
        allow_none=True,
    )

    value = integer_value_to_string(
        transaction_data.get("value"),
        "transaction.value",
    )

    gas = hex_to_int(
        transaction_data.get("gas"),
        "transaction.gas",
    )

    gas_price = integer_value_to_string(
        transaction_data.get("gasPrice"),
        "transaction.gasPrice",
    )

    input_data = normalize_hex_string(
        transaction_data.get("input"),
        "transaction.input",
    )

    event_id = create_transaction_event_id(
        chain_id=chain_id,
        block_hash=block_hash,
        transaction_hash=transaction_hash,
    )

    event = NormalizedTransactionEvent(
        schema_version=settings.schema_version,
        event_id=event_id,
        chain_id=chain_id,
        block_number=block_number,
        block_hash=block_hash,
        payload=TransactionPayload(
            transaction_hash=transaction_hash,
            transaction_index=transaction_index,
            from_address=from_address,
            to_address=to_address,
            value=value,
            gas=gas,
            gas_price=gas_price,
            input=input_data,
        ),
    )

    return event.model_dump(mode="json")

def _normalize_block_message(
    raw_message: dict[str, Any],
) -> list[dict[str, Any]]:
    payload = _require_dict(
        raw_message.get("payload"),
        "payload",
    )

    chain_id = hex_to_int(
        raw_message.get("chain_id"),
        "chain_id",
    )

    block_number = hex_to_int(
        payload.get("number"),
        "payload.number",
    )

    block_hash = normalize_hex_string(
        payload.get("hash"),
        "payload.hash",
    )

    parent_hash = normalize_hex_string(
        payload.get("parentHash"),
        "payload.parentHash",
    )

    timestamp = timestamp_to_utc_iso(
        payload.get("timestamp"),
        "payload.timestamp",
    )

    transactions = _require_list(
        payload.get("transactions"),
        "payload.transactions",
    )

    _validate_raw_identity(
        raw_message=raw_message,
        block_number=block_number,
        block_hash=block_hash,
    )

    block_event_id = create_block_event_id(
        chain_id=chain_id,
        block_hash=block_hash,
    )

    block_event = NormalizedBlockEvent(
        schema_version=settings.schema_version,
        event_id=block_event_id,
        chain_id=chain_id,
        block_number=block_number,
        block_hash=block_hash,
        payload=BlockPayload(
            parent_hash=parent_hash,
            transactions_count=len(transactions),
            timestamp=timestamp,
        ),
    )

    normalized_events: list[dict[str, Any]] = [
        block_event.model_dump(mode="json")
    ]

    for transaction in transactions:
        transaction_event = _normalize_transaction(
            transaction=transaction,
            chain_id=chain_id,
            block_number=block_number,
            block_hash=block_hash,
        )

        normalized_events.append(transaction_event)

    return normalized_events

def _normalize_log_message(
    raw_message: dict[str, Any],
) -> list[dict[str, Any]]:
    payload = _require_dict(
        raw_message.get("payload"),
        "payload",
    )

    chain_id = hex_to_int(
        raw_message.get("chain_id"),
        "chain_id",
    )

    block_number = hex_to_int(
        payload.get("blockNumber"),
        "payload.blockNumber",
    )

    block_hash = normalize_hex_string(
        payload.get("blockHash"),
        "payload.blockHash",
    )

    transaction_hash = normalize_hex_string(
        payload.get("transactionHash"),
        "payload.transactionHash",
    )

    log_index = hex_to_int(
        payload.get("logIndex"),
        "payload.logIndex",
    )

    contract_address = normalize_hex_string(
        payload.get("address"),
        "payload.address",
    )

    raw_topics = _require_list(
        payload.get("topics"),
        "payload.topics",
    )

    topics = [
        normalize_hex_string(
            topic,
            f"payload.topics[{index}]",
        )
        for index, topic in enumerate(raw_topics)
    ]

    data = normalize_hex_string(
        payload.get("data"),
        "payload.data",
    )

    removed = normalize_boolean(
        payload.get(
            "removed",
            raw_message.get("removed", False),
        ),
        "payload.removed",
    )

    _validate_raw_identity(
        raw_message=raw_message,
        block_number=block_number,
        block_hash=block_hash,
    )

    event_id = create_log_event_id(
        chain_id=chain_id,
        block_hash=block_hash,
        transaction_hash=transaction_hash,
        log_index=log_index,
    )

    event = NormalizedLogEvent(
        schema_version=settings.schema_version,
        event_id=event_id,
        chain_id=chain_id,
        block_number=block_number,
        block_hash=block_hash,
        payload=LogPayload(
            transaction_hash=transaction_hash,
            log_index=log_index,
            contract_address=contract_address,
            topics=topics,
            data=data,
            removed=removed,
        ),
    )

    return [event.model_dump(mode="json")]
def normalize_raw_message(
    raw_message: Any,
) -> list[dict[str, Any]]:
    message = _require_dict(
        raw_message,
        "raw_message",
    )

    data_type = message.get("data_type")

    try:
        if data_type == "block":
            return _normalize_block_message(message)

        if data_type == "log":
            return _normalize_log_message(message)

        raise UnsupportedEventTypeError(
            f"desteklenmeyen data type: {data_type}"
        )

    except PydanticValidationError as error:
        raise ValidationError(
            f"şema doğrulama başarısız oldu : {error}"
        ) from error
