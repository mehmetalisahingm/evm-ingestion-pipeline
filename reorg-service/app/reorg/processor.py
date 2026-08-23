import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from app.kafka.producer import ReorgKafkaProducer
from app.reorg.exceptions import ReorgError
from app.reorg.models import (
    NormalizedEvent,
    StoredEventState,
)
from app.reorg.service import (
    ReorgPlan,
    ReorgService,
)
from app.state.redis_store import RedisStateStore


@dataclass
class ProcessingResult:
    canonical_count: int = 0
    duplicate_count: int = 0
    sent_to_dlq: bool = False
    reorg_detected: bool = False


@dataclass
class PreparedProcessing:
    """
    Bir event için Kafka mesajları hazırlanmış,
    ancak Redis değişiklikleri henüz uygulanmamış
    işlem sonucudur.
    """

    result: ProcessingResult
    plan: ReorgPlan | None
    deliveries: list[Any]


def decode_message(
    value: bytes,
) -> dict[str, Any]:
    return json.loads(
        value.decode("utf-8")
    )


def _create_dlq_message(
    message: Any,
    exc: Exception,
    raw_event: dict[str, Any] | None,
) -> dict[str, Any]:
    dlq_message: dict[str, Any] = {
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

    if isinstance(raw_event, dict):
        dlq_message["event_id"] = raw_event.get(
            "event_id"
        )
        dlq_message["original_message"] = raw_event
        return dlq_message

    try:
        decoded_event = decode_message(
            message.value
        )

        if isinstance(decoded_event, dict):
            dlq_message["event_id"] = decoded_event.get(
                "event_id"
            )
            dlq_message["original_message"] = decoded_event

    except Exception:
        dlq_message["original_message"] = (
            message.value.decode(
                "utf-8",
                errors="replace",
            )
        )

    return dlq_message


async def prepare_message(
    message: Any,
    *,
    reorg_service: ReorgService,
    producer: ReorgKafkaProducer,
    raw_event: dict[str, Any] | None = None,
) -> PreparedProcessing:
    """
    Event'i parse eder ve Re-org planını üretir.

    Canonical/DLQ mesajlarını Kafka producer
    buffer'ına koyar fakat delivery ACK'lerini
    burada beklemez.

    Redis state değişiklikleri de burada
    uygulanmaz.
    """

    try:
        if raw_event is None:
            raw_event = decode_message(
                message.value
            )

        event = NormalizedEvent.model_validate(
            raw_event
        )

        plan = await reorg_service.create_plan(
            event
        )

        deliveries: list[Any] = []

        for canonical_event in plan.canonical_events:
            delivery = await producer.queue_canonical(
                canonical_event.model_dump(
                    mode="json"
                )
            )
            deliveries.append(delivery)

        return PreparedProcessing(
            result=ProcessingResult(
                canonical_count=len(
                    plan.canonical_events
                ),
                duplicate_count=plan.duplicate_count,
                reorg_detected=plan.reorg_detected,
            ),
            plan=plan,
            deliveries=deliveries,
        )

    except (
        ReorgError,
        ValidationError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        dlq_message = _create_dlq_message(
            message,
            exc,
            raw_event,
        )

        delivery = await producer.queue_dlq(
            dlq_message
        )

        return PreparedProcessing(
            result=ProcessingResult(
                sent_to_dlq=True
            ),
            plan=None,
            deliveries=[delivery],
        )


async def _apply_plan(
    plan: ReorgPlan,
    *,
    state_store: RedisStateStore,
) -> None:
    await state_store.apply_changes(
        event_states=plan.event_states,
        block_states_to_save=(
            plan.block_states_to_save
        ),
        block_states_to_remove=(
            plan.block_states_to_remove
        ),
        head_state=plan.head_state,
        pending_events_to_add=(
            plan.pending_events_to_add
        ),
        pending_keys_to_delete=(
            plan.pending_keys_to_delete
        ),
    )

    if plan.head_state is not None:
        await state_store.prune_old_blocks(
            chain_id=plan.head_state.chain_id,
            head_block_number=(
                plan.head_state.block_number
            ),
        )


async def _apply_child_plans_as_batch(
    plans: list[ReorgPlan],
    *,
    state_store: RedisStateStore,
) -> None:
    if not plans:
        return

    event_states: list[StoredEventState] = []
    pending_events_to_add: list[NormalizedEvent] = []
    pending_keys_to_delete: list[tuple[int, str]] = []

    for plan in plans:
        event_states.extend(plan.event_states)
        pending_events_to_add.extend(
            plan.pending_events_to_add
        )
        pending_keys_to_delete.extend(
            plan.pending_keys_to_delete
        )

    await state_store.apply_changes(
        event_states=event_states,
        block_states_to_save=[],
        block_states_to_remove=[],
        head_state=None,
        pending_events_to_add=pending_events_to_add,
        pending_keys_to_delete=pending_keys_to_delete,
    )


def _is_child_only_plan(
    plan: ReorgPlan,
) -> bool:
    return (
        not plan.block_states_to_save
        and not plan.block_states_to_remove
        and plan.head_state is None
    )


async def commit_prepared_batch(
    prepared_items: list[PreparedProcessing],
    *,
    producer: ReorgKafkaProducer,
    state_store: RedisStateStore,
) -> None:
    if not prepared_items:
        return

    all_deliveries: list[Any] = []

    for prepared in prepared_items:
        all_deliveries.extend(
            prepared.deliveries
        )

    await producer.wait_for_deliveries(
        all_deliveries
    )

    plans = [
        prepared.plan
        for prepared in prepared_items
        if prepared.plan is not None
    ]

    if not plans:
        return

    if all(
        _is_child_only_plan(plan)
        for plan in plans
    ):
        await _apply_child_plans_as_batch(
            plans,
            state_store=state_store,
        )
        return

    for plan in plans:
        await _apply_plan(
            plan,
            state_store=state_store,
        )


async def process_message(
    message: Any,
    *,
    reorg_service: ReorgService,
    producer: ReorgKafkaProducer,
    state_store: RedisStateStore,
    raw_event: dict[str, Any] | None = None,
) -> ProcessingResult:
    prepared = await prepare_message(
        message,
        reorg_service=reorg_service,
        producer=producer,
        raw_event=raw_event,
    )

    await commit_prepared_batch(
        [prepared],
        producer=producer,
        state_store=state_store,
    )

    return prepared.result
