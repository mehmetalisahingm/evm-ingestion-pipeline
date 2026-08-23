from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

EventType = Literal[
    "block",
    "transaction",
    "log",
]

class CanonicalEvent(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
    )

    schema_version: int
    event_id: str
    event_type: EventType

    chain_id: int
    block_number: int
    block_hash: str

    normalized_at: datetime

    payload: dict[str, Any]

    canonical: bool
    version: int