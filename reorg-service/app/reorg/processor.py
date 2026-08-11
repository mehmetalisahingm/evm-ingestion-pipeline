import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from app.kafka.producer import ReorgKafkaProducer
from app.reorg.exceptions import ReorgError
from app.reorg.models import NormalizedEvent
from app.reorg.service import ReorgService
from app.state.redis_store import RedisStateStore


@dataclass
class ProcessingResult:
    canonical_count: int = 0
    duplicate_count: int = 0
    sent_to_dlq: bool = False
    reorg_detected: bool = False

def decode_message(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode("utf-8"))

async def process_message(
    message: Any,
    *,
    reorg_service: ReorgService,
    producer: ReorgKafkaProducer,
    state_store: RedisStateStore,
) -> ProcessingResult:
    try:
        raw_event = decode_message(message.value)

        event = NormalizedEvent.model_validate(
            raw_event
        )

        plan = await reorg_service.create_plan(event)

        for canonical_event in plan.canonical_events:
            await producer.send_canonical(
                canonical_event.model_dump(
                    mode="json"
                )
            )

        await state_store.apply_changes(
            event_states=plan.event_states,
            block_states_to_save=plan.block_states_to_save,
            block_states_to_remove=plan.block_states_to_remove,
            head_state=plan.head_state,
            pending_events_to_add=plan.pending_events_to_add,
            pending_keys_to_delete=plan.pending_keys_to_delete,
        )

        if plan.head_state is not None:
            await state_store.prune_old_blocks(
                chain_id=plan.head_state.chain_id,
                head_block_number=(
                    plan.head_state.block_number
                ),
            )

        return ProcessingResult(
            canonical_count=len(
                plan.canonical_events
            ),
            duplicate_count=plan.duplicate_count,
            reorg_detected=plan.reorg_detected,
        )
    except (
        ReorgError,
        ValidationError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        dlq_message = {
            "source_service": "reorg-service",
            "source_topic": message.topic,
            "source_partition": message.partition,
            "source_offset": message.offset,
            "error_type": type(exc).__name__,
            "error_reason": str(exc),
            "failed_at": datetime.now(
                timezone.utc
            ).isoformat(),
        }
        try:
            raw = decode_message(message.value)

            if isinstance(raw, dict):
                dlq_message["event_id"] = raw.get(
                    "event_id"
                )

                dlq_message["original_message"] = raw
        except Exception:
            dlq_message["original_message"] = (
                message.value.decode(
                    "utf-8",
                    errors="replace",
                )
            )
        await producer.send_dlq(dlq_message)
        return ProcessingResult(
            sent_to_dlq=True
        )
