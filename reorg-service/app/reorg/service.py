from dataclasses import dataclass, field
import re
from app.reorg.exceptions import (
    ChainGapError,
    DeepReorgError,
    EventValidationError,
    MissingBlockStateError,
)
from app.reorg.models import (
    CanonicalEvent,
    NormalizedEvent,
    StoredBlockState,
    StoredEventState,
)
from app.state.redis_store import RedisStateStore


HEX_PATTERN = re.compile(r"^0x[0-9a-f]+$")


@dataclass
class ReorgPlan:

    canonical_events: list[CanonicalEvent] = field(
        default_factory=list
    )

    event_states: list[StoredEventState] = field(
        default_factory=list
    )

    block_states_to_save: list[StoredBlockState] = field(
        default_factory=list
    )

    block_states_to_remove: list[StoredBlockState] = field(
        default_factory=list
    )

    pending_events_to_add: list[NormalizedEvent] = field(
        default_factory=list
    )

    pending_keys_to_delete: list[
        tuple[int, str]
    ] = field(default_factory=list)

    head_state: StoredBlockState | None = None

    duplicate_count: int = 0
    reorg_detected: bool = False
    orphaned_block_count: int = 0

class ReorgService:

    def __init__(
        self,
        state_store: RedisStateStore,
    ) -> None:
        self._state_store = state_store

    async def create_plan(
        self,
        event: NormalizedEvent,
    ) -> ReorgPlan:
        if event.event_type == "block":
            return await self._create_block_plan(event)

        return await self._create_child_event_plan(event)

    @staticmethod
    def _get_parent_hash(
        event: NormalizedEvent,
    ) -> str:
        parent_hash = event.payload.get("parent_hash")

        if not isinstance(parent_hash, str):
            raise EventValidationError(
                "Block payload.parent_hash alanı eksik"
            )

        parent_hash = parent_hash.lower()

        if HEX_PATTERN.fullmatch(parent_hash) is None:
            raise EventValidationError(
                "Block parent_hash alanı geçersiz"
            )

        return parent_hash

    @staticmethod
    def _event_identity_data(
        event: NormalizedEvent,
    ) -> tuple:
        payload = dict(event.payload)

        if event.event_type == "log":
            payload.pop("removed", None)

        return (
            event.event_type,
            event.chain_id,
            event.block_number,
            event.block_hash.lower(),
            payload,
        )

    def _validate_existing_event(
        self,
        existing: StoredEventState,
        incoming: NormalizedEvent,
    ) -> None:
        existing_identity = self._event_identity_data(
            existing.event
        )

        incoming_identity = self._event_identity_data(
            incoming
        )

        if existing_identity != incoming_identity:
            raise EventValidationError(
                "Aynı event_id ile farklı event içeriği geldi"
            )

    async def _add_event_transition(
        self,
        plan: ReorgPlan,
        event: NormalizedEvent,
        *,
        canonical: bool,
    ) -> int:
        existing_state = (
            await self._state_store.get_event_state(
                event.event_id
            )
        )

        if existing_state is not None:
            self._validate_existing_event(
                existing_state,
                event,
            )

            if existing_state.canonical == canonical:
                plan.duplicate_count += 1
                return existing_state.version

            version = existing_state.version + 1

        else:
            version = 1

        new_state = StoredEventState(
            event=event,
            canonical=canonical,
            version=version,
        )

        canonical_event = CanonicalEvent.from_normalized(
            event,
            canonical=canonical,
            version=version,
        )

        plan.event_states.append(new_state)
        plan.canonical_events.append(canonical_event)

        return version

    async def _add_pending_events_to_plan(
        self,
        plan: ReorgPlan,
        *,
        chain_id: int,
        block_hash: str,
    ) -> None:
        pending_events = (
            await self._state_store.get_pending_events(
                chain_id=chain_id,
                block_hash=block_hash,
            )
        )

        for pending_event in pending_events:
            canonical = True

            if (
                pending_event.event_type == "log"
                and pending_event.payload.get("removed")
                is True
            ):
                canonical = False

            await self._add_event_transition(
                plan,
                pending_event,
                canonical=canonical,
            )

        if pending_events:
            plan.pending_keys_to_delete.append(
                (
                    chain_id,
                    block_hash,
                )
            )

    async def _create_child_event_plan(
            self,
            event: NormalizedEvent,
    ) -> ReorgPlan:

            plan = ReorgPlan()

            if (
            event.event_type == "log"
            and event.payload.get("removed") is True
            ):
                await self._add_event_transition(
                    plan,
                    event,
                    canonical=False,
                )

                return plan

            block_state = (
                await self._state_store.get_block_by_hash(
                    chain_id=event.chain_id,
                    block_hash=event.block_hash,
                )
            )

            if block_state is None:
                existing_state = (
                    await self._state_store.get_event_state(
                       event.event_id
                    )
                )

                if existing_state is not None:
                    self._validate_existing_event(
                        existing_state,
                        event,
                    )

                    plan.duplicate_count += 1
                    return plan

                plan.pending_events_to_add.append(event)

                return plan


            await self._add_event_transition(
                plan,
                event,
                canonical=True,
            )

            return plan

    async def _orphan_blocks(
        self,
        plan: ReorgPlan,
        blocks: list[StoredBlockState],
    ) -> None:
        """
        Eski canonical zincirde kalan blokları ve onların
        event'lerini canonical=false yapar.
        """

        event_type_order = {
            "block": 0,
            "transaction": 1,
            "log": 2,
        }

        for block_state in reversed(blocks):
            event_states = (
                await self._state_store.get_block_event_states(
                    chain_id=block_state.chain_id,
                    block_hash=block_state.block_hash,
                )
            )

            event_states.sort(
                key=lambda state: (
                    event_type_order[
                        state.event.event_type
                    ],
                    state.event.event_id,
                )
            )

            for existing_state in event_states:
                if not existing_state.canonical:
                    continue

                version = existing_state.version + 1

                orphaned_state = StoredEventState(
                    event=existing_state.event,
                    canonical=False,
                    version=version,
                )

                orphaned_event = (
                    CanonicalEvent.from_normalized(
                        existing_state.event,
                        canonical=False,
                        version=version,
                    )
                )

                plan.event_states.append(
                    orphaned_state
                )

                plan.canonical_events.append(
                    orphaned_event
                )

        plan.block_states_to_remove.extend(blocks)

        if blocks:
            plan.reorg_detected = True
            plan.orphaned_block_count = len(blocks)

    async def _accept_block(
        self,
        plan: ReorgPlan,
        event: NormalizedEvent,
        parent_hash: str,
    ) -> None:
        version = await self._add_event_transition(
            plan,
            event,
            canonical=True,
        )

        block_state = StoredBlockState(
            chain_id=event.chain_id,
            block_number=event.block_number,
            block_hash=event.block_hash,
            parent_hash=parent_hash,
            block_event_id=event.event_id,
            canonical=True,
            version=version,
        )

        plan.block_states_to_save.append(block_state)
        plan.head_state = block_state

        await self._add_pending_events_to_plan(
            plan,
            chain_id=event.chain_id,
            block_hash=event.block_hash,
        )

    async def _create_block_plan(
        self,
        event: NormalizedEvent,
    ) -> ReorgPlan:
        plan = ReorgPlan()
        parent_hash = self._get_parent_hash(event)

        current_block = (
            await self._state_store.get_block_state(
                chain_id=event.chain_id,
                block_number=event.block_number,
            )
        )
        if (
            current_block is not None
            and current_block.block_hash.lower()
            == event.block_hash.lower()
        ):
            await self._add_event_transition(
                plan,
                event,
                canonical=True,
            )

            await self._add_pending_events_to_plan(
                plan,
                chain_id=event.chain_id,
                block_hash=event.block_hash,
            )

            return plan

        existing_event = (
            await self._state_store.get_event_state(
                event.event_id
            )
        )

        if (
            current_block is None
            and existing_event is not None
            and existing_event.canonical
        ):
            self._validate_existing_event(
                existing_event,
                event,
            )

            plan.duplicate_count += 1
            return plan

        head = await self._state_store.get_head(
            event.chain_id
        )

        if head is None:
            await self._accept_block(
                plan,
                event,
                parent_hash,
            )

            return plan

        if (
            event.block_number
            == head.block_number + 1
            and parent_hash
            == head.block_hash.lower()
        ):
            await self._accept_block(
                plan,
                event,
                parent_hash,
            )

            return plan


        if event.block_number > head.block_number + 1:
            raise ChainGapError(
                "Blok sırası eksik: "
                f"head={head.block_number}, "
                f"incoming={event.block_number}"
            )

        parent_state = (
            await self._state_store.get_block_by_hash(
                chain_id=event.chain_id,
                block_hash=parent_hash,
            )
        )

        if (
            parent_state is not None
            and parent_state.block_number
            == event.block_number - 1
        ):
            orphaned_blocks = (
                await self._state_store.get_blocks_after(
                    chain_id=event.chain_id,
                    block_number=parent_state.block_number,
                )
            )

            await self._orphan_blocks(
                plan,
                orphaned_blocks,
            )

            await self._accept_block(
                plan,
                event,
                parent_hash,
            )

            return plan

        expected_parent = None

        if event.block_number > 0:
            expected_parent = (
                await self._state_store.get_block_state(
                    chain_id=event.chain_id,
                    block_number=event.block_number - 1,
                )
            )

        if (
            event.block_number <= head.block_number
            and expected_parent is None
        ):
            raise DeepReorgError(
                "Re-org Redis penceresinden daha eski "
                f"bir bloğa ulaşıyor: {event.block_number}"
            )

        raise MissingBlockStateError(
            "Incoming block parent_hash değeri canonical "
            "zincirde bulunamadı"
        )
