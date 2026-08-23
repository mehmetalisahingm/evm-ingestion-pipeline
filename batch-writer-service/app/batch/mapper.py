import json
from typing import Any

from app.batch.models import CanonicalEvent


def _raw_payload(event: CanonicalEvent) -> str:
    return json.dumps(
        event.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def map_event_to_clickhouse(
    event: CanonicalEvent,
) -> tuple[str, dict[str, Any]]:
    common = {
        "event_id": event.event_id,
        "chain_id": event.chain_id,
        "block_number": event.block_number,
        "block_hash": event.block_hash,
        "canonical": int(event.canonical),
        "version": event.version,
        "raw_payload": _raw_payload(event),
    }

    if event.event_type == "block":
        return (
            "blocks",
            {
                **common,
                "parent_hash": event.payload["parent_hash"],
                "transactions_count": event.payload[
                    "transactions_count"
                ],
            },
        )

    if event.event_type == "transaction":
        return (
            "transactions",
            {
                **common,
                "transaction_hash": event.payload[
                    "transaction_hash"
                ],
                "transaction_index": event.payload[
                    "transaction_index"
                ],
                "from_address": event.payload["from_address"],
                "to_address": event.payload["to_address"],
                "value": event.payload["value"],
                "gas": event.payload["gas"],
                "gas_price": event.payload["gas_price"],
                "input": event.payload["input"],
            },
        )

    if event.event_type == "log":
        return (
            "logs",
            {
                **common,
                "transaction_hash": event.payload[
                    "transaction_hash"
                ],
                "log_index": event.payload["log_index"],
                "contract_address": event.payload[
                    "contract_address"
                ],
                "topics": event.payload["topics"],
                "data": event.payload["data"],
                "removed": int(event.payload["removed"]),
            },
        )

    raise ValueError(
        f"Desteklenmeyen event tipi: {event.event_type}"
    )