from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field

EventType = Literal["block", "transaction", "log"]

class StrictModel(BaseModel):
    model_config=ConfigDict(
        extra="forbid",
    )

class NormalizedEvent(StrictModel):
    schema_version:int=Field(ge=1)

    event_id:str=Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$"
    )
    event_type:EventType
    chain_id:int=Field(ge=1)
    block_number:int=Field(ge=0)
    block_hash:str=Field(min_length=3,pattern=r"^0x[0-9a-f]+$",)

    normalized_at:datetime
    payload:dict[str,Any]

class CanonicalEvent(NormalizedEvent):
    canonical: bool
    version: int = Field(ge=1)

    @classmethod
    def from_normalized(
        cls,
        event: NormalizedEvent,
        *,
        canonical: bool,
        version: int,
    ) -> "CanonicalEvent":
        return cls(
            **event.model_dump(),
            canonical=canonical,
            version=version,
        )

class StoredEventState(StrictModel):
    event: NormalizedEvent
    canonical: bool
    version: int = Field(ge=1)

class StoredBlockState(StrictModel):
    chain_id: int = Field(ge=1)
    block_number: int = Field(ge=0)

    block_hash: str = Field(
        min_length=3,
        pattern=r"^0x[0-9a-f]+$",
    )

    parent_hash: str = Field(
        min_length=3,
        pattern=r"^0x[0-9a-f]+$",
    )

    block_event_id: str = Field(
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )

    canonical: bool
    version: int = Field(ge=1)
