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


def decode_message(
    value: bytes,
) -> dict[str, Any]:
    return json.loads(
        value.decode("utf-8")
    )


async def process_message(
    message: Any,
    *,
    reorg_service: ReorgService,
    producer: ReorgKafkaProducer,
    state_store: RedisStateStore,
    raw_event: dict[str, Any] | None = None,
) -> ProcessingResult:
    try:
        # main.py mesajı zaten parse ettiyse
        # aynı JSON'u tekrar parse etmiyoruz.
        if raw_event is None:
            raw_event = decode_message(
                message.value
            )

        event = (
            NormalizedEvent.model_validate(
                raw_event
            )
        )

        plan = await reorg_service.create_plan(
            event
        )

        deliveries: list[Any] = []

        # Canonical eventleri Kafka producer
        # bufferına koy.
        for canonical_event in (
            plan.canonical_events
        ):
            delivery = (
                await producer.queue_canonical(
                    canonical_event.model_dump(
                        mode="json"
                    )
                )
            )

            deliveries.append(
                delivery
            )

        # Canonical eventlerin Kafka tarafından
        # başarıyla alındığını bekle.
        #
        # Kafka başarısızsa Redis state
        # değişikliklerine geçilmez.
        await producer.wait_for_deliveries(
            deliveries
        )

        # Kafka başarılı olduktan sonra
        # Redis state değişikliklerini uygula.
        await state_store.apply_changes(
            event_states=(
                plan.event_states
            ),
            block_states_to_save=(
                plan.block_states_to_save
            ),
            block_states_to_remove=(
                plan.block_states_to_remove
            ),
            head_state=(
                plan.head_state
            ),
            pending_events_to_add=(
                plan.pending_events_to_add
            ),
            pending_keys_to_delete=(
                plan.pending_keys_to_delete
            ),
        )

        if plan.head_state is not None:
            await state_store.prune_old_blocks(
                chain_id=(
                    plan.head_state.chain_id
                ),
                head_block_number=(
                    plan.head_state.block_number
                ),
            )

        return ProcessingResult(
            canonical_count=len(
                plan.canonical_events
            ),
            duplicate_count=(
                plan.duplicate_count
            ),
            reorg_detected=(
                plan.reorg_detected
            ),
        )

    except (
        ReorgError,
        ValidationError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        dlq_message: dict[str, Any] = {
            "source_service": (
                "reorg-service"
            ),
            "source_topic": (
                message.topic
            ),
            "source_partition": (
                message.partition
            ),
            "source_offset": (
                message.offset
            ),
            "error_type": (
                type(exc).__name__
            ),
            "error_reason": str(exc),
            "failed_at": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        # Eğer mesaj main.py tarafından zaten
        # parse edilmişse tekrar json.loads
        # yapmaya gerek yok.
        if isinstance(raw_event, dict):
            dlq_message[
                "event_id"
            ] = raw_event.get(
                "event_id"
            )

            dlq_message[
                "original_message"
            ] = raw_event

        else:
            try:
                decoded_event = (
                    decode_message(
                        message.value
                    )
                )

                if isinstance(
                    decoded_event,
                    dict,
                ):
                    dlq_message[
                        "event_id"
                    ] = decoded_event.get(
                        "event_id"
                    )

                    dlq_message[
                        "original_message"
                    ] = decoded_event

            except Exception:
                dlq_message[
                    "original_message"
                ] = (
                    message.value.decode(
                        "utf-8",
                        errors="replace",
                    )
                )

        delivery = (
            await producer.queue_dlq(
                dlq_message
            )
        )

        await producer.wait_for_deliveries(
            [delivery]
        )

        return ProcessingResult(
            sent_to_dlq=True
        )