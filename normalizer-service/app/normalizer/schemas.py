from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


def utc_now_iso()-> str:
    return(
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00","Z")
    )

class StrictModel(BaseModel):

    model_config= ConfigDict(extra="forbid")

class BaseNormalizedEvent(StrictModel):
    schema_version: int = Field(ge=1)
    event_id: str = Field(min_length=64, max_length=64)
    event_type: str
    chain_id: int = Field(ge=1)
    block_number: int = Field(ge=0)
    block_hash: str = Field(min_length=3)
    normalized_at: str = Field(default_factory=utc_now_iso)

class BlockPayload(StrictModel):

    parent_hash: str = Field(min_length=3)
    transactions_count: int = Field(ge=0)
    timestamp: str

class NormalizedBlockEvent(BaseNormalizedEvent):
    event_type: Literal["block"] = "block"
    payload: BlockPayload


class TransactionPayload(StrictModel):
    transaction_hash: str = Field(min_length=3)
    transaction_index: int = Field(ge=0)

    from_address: str = Field(min_length=3)
    to_address: str | None = None

    value: str
    gas: int = Field(ge=0)
    gas_price: str
    input: str

class NormalizedTransactionEvent(BaseNormalizedEvent):

    event_type: Literal["transaction"] = "transaction"
    payload: TransactionPayload

class LogPayload(StrictModel):
    transaction_hash: str = Field(min_length=3)
    log_index: int = Field(ge=0)

    contract_address: str = Field(min_length=3)
    topics: list[str]
    data: str
    removed: bool = False


class NormalizedLogEvent(BaseNormalizedEvent):

    event_type: Literal["log"] = "log"
    payload: LogPayload


class DLQMessage(StrictModel):

    schema_version: int = Field(ge=1)
    source_service: str = "normalizer-service"
    source_topic: str
    source_partition: int = Field(ge=0)
    source_offset: int = Field(ge=0)
    error_type: str
    error_reason: str
    failed_at: str = Field(default_factory=utc_now_iso)
    original_message: Any
