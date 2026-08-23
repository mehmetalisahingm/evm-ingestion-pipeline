from redis.asyncio import Redis

from app.reorg.models import (
    NormalizedEvent,
    StoredBlockState,
    StoredEventState,
)


class RedisStateStore:
    def __init__(
        self,
        redis_url: str,
        window_size: int,
        pending_ttl_seconds: int,
    ) -> None:
        self._redis_url = redis_url
        self._window_size = window_size
        self._pending_ttl_seconds = pending_ttl_seconds
        self._redis: Redis | None = None
        self._block_cache_by_hash: dict[
            tuple[int, str],
            StoredBlockState,
        ] = {}
        self._prefetched_event_states: dict[
            str,
            StoredEventState | None,
        ] = {}

    @property
    def client(self) -> Redis:
        if self._redis is None:
            raise RuntimeError(
                "Redis bağlantısı henüz başlatılmadı"
            )
        return self._redis

    async def start(self) -> None:
        self._redis = Redis.from_url(
            self._redis_url,
            decode_responses=True,
        )
        await self._redis.ping()
        self._block_cache_by_hash.clear()
        self._prefetched_event_states.clear()

    async def stop(self) -> None:
        self._block_cache_by_hash.clear()
        self._prefetched_event_states.clear()
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

    async def ping(self) -> bool:
        return bool(await self.client.ping())

    @staticmethod
    def _event_key(event_id: str) -> str:
        return f"reorg:event:{event_id}"

    @staticmethod
    def _block_key(
        chain_id: int,
        block_number: int,
    ) -> str:
        return f"reorg:block:{chain_id}:{block_number}"

    @staticmethod
    def _block_hash_key(
        chain_id: int,
        block_hash: str,
    ) -> str:
        return (
            f"reorg:block-hash:"
            f"{chain_id}:{block_hash.lower()}"
        )

    @staticmethod
    def _block_events_key(
        chain_id: int,
        block_hash: str,
    ) -> str:
        return (
            f"reorg:block-events:"
            f"{chain_id}:{block_hash.lower()}"
        )

    @staticmethod
    def _head_key(chain_id: int) -> str:
        return f"reorg:head:{chain_id}"

    @staticmethod
    def _block_index_key(chain_id: int) -> str:
        return f"reorg:block-index:{chain_id}"

    @staticmethod
    def _orphan_block_index_key(
        chain_id: int,
    ) -> str:
        return f"reorg:orphan-block-index:{chain_id}"

    @staticmethod
    def _pending_key(
        chain_id: int,
        block_hash: str,
    ) -> str:
        return (
            f"reorg:pending:"
            f"{chain_id}:{block_hash.lower()}"
        )

    @staticmethod
    def _cache_key(
        chain_id: int,
        block_hash: str,
    ) -> tuple[int, str]:
        return chain_id, block_hash.lower()

    def _cache_block_state(
        self,
        state: StoredBlockState,
    ) -> None:
        self._block_cache_by_hash[
            self._cache_key(
                state.chain_id,
                state.block_hash,
            )
        ] = state

    def _remove_cached_block_state(
        self,
        state: StoredBlockState,
    ) -> None:
        self._block_cache_by_hash.pop(
            self._cache_key(
                state.chain_id,
                state.block_hash,
            ),
            None,
        )

    def _remove_cached_block_hash(
        self,
        chain_id: int,
        block_hash: str,
    ) -> None:
        self._block_cache_by_hash.pop(
            self._cache_key(
                chain_id,
                block_hash,
            ),
            None,
        )

    def _prune_block_cache(
        self,
        *,
        chain_id: int,
        head_block_number: int,
    ) -> None:
        cutoff = head_block_number - self._window_size
        if cutoff < 0:
            return

        keys_to_remove: list[tuple[int, str]] = []
        for cache_key, state in (
            self._block_cache_by_hash.items()
        ):
            if (
                state.chain_id == chain_id
                and state.block_number <= cutoff
            ):
                keys_to_remove.append(cache_key)

        for cache_key in keys_to_remove:
            self._block_cache_by_hash.pop(
                cache_key,
                None,
            )

    async def get_event_state(
        self,
        event_id: str,
    ) -> StoredEventState | None:
        if event_id in self._prefetched_event_states:
            return self._prefetched_event_states[event_id]

        raw_state = await self.client.get(
            self._event_key(event_id)
        )
        if raw_state is None:
            return None

        return StoredEventState.model_validate_json(
            raw_state
        )

    async def prefetch_event_states(
        self,
        event_ids: list[str],
    ) -> None:
        self._prefetched_event_states.clear()
        if not event_ids:
            return

        unique_event_ids = list(
            dict.fromkeys(event_ids)
        )
        event_keys = [
            self._event_key(event_id)
            for event_id in unique_event_ids
        ]
        raw_states = await self.client.mget(event_keys)

        for event_id, raw_state in zip(
            unique_event_ids,
            raw_states,
            strict=True,
        ):
            if raw_state is None:
                self._prefetched_event_states[
                    event_id
                ] = None
            else:
                self._prefetched_event_states[
                    event_id
                ] = (
                    StoredEventState.model_validate_json(
                        raw_state
                    )
                )

    def clear_prefetched_event_states(self) -> None:
        self._prefetched_event_states.clear()

    async def save_event_state(
        self,
        state: StoredEventState,
    ) -> None:
        event = state.event
        pipeline = self.client.pipeline(
            transaction=True
        )
        pipeline.set(
            self._event_key(event.event_id),
            state.model_dump_json(),
        )
        pipeline.sadd(
            self._block_events_key(
                event.chain_id,
                event.block_hash,
            ),
            event.event_id,
        )
        await pipeline.execute()

        if event.event_id in (
            self._prefetched_event_states
        ):
            self._prefetched_event_states[
                event.event_id
            ] = state

    async def get_block_event_states(
        self,
        chain_id: int,
        block_hash: str,
    ) -> list[StoredEventState]:
        event_ids = await self.client.smembers(
            self._block_events_key(
                chain_id,
                block_hash,
            )
        )
        if not event_ids:
            return []

        event_keys = [
            self._event_key(event_id)
            for event_id in event_ids
        ]
        raw_states = await self.client.mget(event_keys)

        return [
            StoredEventState.model_validate_json(
                raw_state
            )
            for raw_state in raw_states
            if raw_state is not None
        ]

    async def get_block_state(
        self,
        chain_id: int,
        block_number: int,
    ) -> StoredBlockState | None:
        raw_state = await self.client.get(
            self._block_key(
                chain_id,
                block_number,
            )
        )
        if raw_state is None:
            return None

        state = StoredBlockState.model_validate_json(
            raw_state
        )
        self._cache_block_state(state)
        return state

    async def get_block_by_hash(
        self,
        chain_id: int,
        block_hash: str,
    ) -> StoredBlockState | None:
        normalized_hash = block_hash.lower()
        cache_key = self._cache_key(
            chain_id,
            normalized_hash,
        )

        cached_state = self._block_cache_by_hash.get(
            cache_key
        )
        if cached_state is not None:
            return cached_state

        block_number = await self.client.get(
            self._block_hash_key(
                chain_id,
                normalized_hash,
            )
        )
        if block_number is None:
            return None

        state = await self.get_block_state(
            chain_id=chain_id,
            block_number=int(block_number),
        )
        if state is None:
            return None

        if state.block_hash.lower() != normalized_hash:
            return None

        self._cache_block_state(state)
        return state

    async def save_block_state(
        self,
        state: StoredBlockState,
    ) -> None:
        existing_state = await self.get_block_state(
            chain_id=state.chain_id,
            block_number=state.block_number,
        )

        pipeline = self.client.pipeline(
            transaction=True
        )

        if (
            existing_state is not None
            and existing_state.block_hash.lower()
            != state.block_hash.lower()
        ):
            pipeline.delete(
                self._block_hash_key(
                    existing_state.chain_id,
                    existing_state.block_hash,
                )
            )

        pipeline.set(
            self._block_key(
                state.chain_id,
                state.block_number,
            ),
            state.model_dump_json(),
        )
        pipeline.set(
            self._block_hash_key(
                state.chain_id,
                state.block_hash,
            ),
            str(state.block_number),
        )
        pipeline.zadd(
            self._block_index_key(state.chain_id),
            {
                str(state.block_number): float(
                    state.block_number
                )
            },
        )
        pipeline.zrem(
            self._orphan_block_index_key(
                state.chain_id
            ),
            state.block_hash.lower(),
        )
        await pipeline.execute()

        if (
            existing_state is not None
            and existing_state.block_hash.lower()
            != state.block_hash.lower()
        ):
            self._remove_cached_block_state(
                existing_state
            )

        self._cache_block_state(state)

    async def remove_block_state(
        self,
        state: StoredBlockState,
    ) -> None:
        pipeline = self.client.pipeline(
            transaction=True
        )
        pipeline.delete(
            self._block_key(
                state.chain_id,
                state.block_number,
            )
        )
        pipeline.delete(
            self._block_hash_key(
                state.chain_id,
                state.block_hash,
            )
        )
        pipeline.zrem(
            self._block_index_key(state.chain_id),
            str(state.block_number),
        )
        pipeline.zadd(
            self._orphan_block_index_key(
                state.chain_id
            ),
            {
                state.block_hash.lower(): float(
                    state.block_number
                )
            },
        )
        await pipeline.execute()
        self._remove_cached_block_state(state)

    async def get_head(
        self,
        chain_id: int,
    ) -> StoredBlockState | None:
        raw_state = await self.client.get(
            self._head_key(chain_id)
        )
        if raw_state is None:
            return None

        state = StoredBlockState.model_validate_json(
            raw_state
        )
        self._cache_block_state(state)
        return state

    async def set_head(
        self,
        state: StoredBlockState,
    ) -> None:
        await self.client.set(
            self._head_key(state.chain_id),
            state.model_dump_json(),
        )
        self._cache_block_state(state)
        self._prune_block_cache(
            chain_id=state.chain_id,
            head_block_number=state.block_number,
        )

    async def delete_head(
        self,
        chain_id: int,
    ) -> None:
        await self.client.delete(
            self._head_key(chain_id)
        )

    async def get_blocks_after(
        self,
        chain_id: int,
        block_number: int,
    ) -> list[StoredBlockState]:
        block_numbers = await self.client.zrangebyscore(
            self._block_index_key(chain_id),
            block_number + 1,
            "+inf",
        )
        if not block_numbers:
            return []

        ordered_numbers = sorted(
            int(number)
            for number in block_numbers
        )
        block_keys = [
            self._block_key(
                chain_id,
                number,
            )
            for number in ordered_numbers
        ]
        raw_states = await self.client.mget(
            block_keys
        )
        states = [
            StoredBlockState.model_validate_json(
                raw_state
            )
            for raw_state in raw_states
            if raw_state is not None
        ]

        for state in states:
            self._cache_block_state(state)

        return states

    async def add_pending_event(
        self,
        event: NormalizedEvent,
    ) -> None:
        pending_key = self._pending_key(
            event.chain_id,
            event.block_hash,
        )
        pipeline = self.client.pipeline(
            transaction=True
        )
        pipeline.hset(
            pending_key,
            event.event_id,
            event.model_dump_json(),
        )
        pipeline.expire(
            pending_key,
            self._pending_ttl_seconds,
        )
        await pipeline.execute()

    async def get_pending_events(
        self,
        chain_id: int,
        block_hash: str,
    ) -> list[NormalizedEvent]:
        raw_events = await self.client.hvals(
            self._pending_key(
                chain_id,
                block_hash,
            )
        )
        return [
            NormalizedEvent.model_validate_json(
                raw_event
            )
            for raw_event in raw_events
        ]

    async def delete_pending_events(
        self,
        chain_id: int,
        block_hash: str,
    ) -> None:
        await self.client.delete(
            self._pending_key(
                chain_id,
                block_hash,
            )
        )

    async def _get_block_event_ids(
        self,
        *,
        chain_id: int,
        block_hashes: list[str],
    ) -> dict[str, set[str]]:
        if not block_hashes:
            return {}

        pipeline = self.client.pipeline(
            transaction=False
        )
        for block_hash in block_hashes:
            pipeline.smembers(
                self._block_events_key(
                    chain_id,
                    block_hash,
                )
            )

        results = await pipeline.execute()
        return {
            block_hash: set(event_ids)
            for block_hash, event_ids in zip(
                block_hashes,
                results,
                strict=True,
            )
        }

    async def prune_old_blocks(
        self,
        chain_id: int,
        head_block_number: int,
    ) -> int:
        cutoff = head_block_number - self._window_size
        if cutoff < 0:
            return 0

        old_block_numbers = await self.client.zrangebyscore(
            self._block_index_key(chain_id),
            "-inf",
            cutoff,
        )

        old_states: list[StoredBlockState] = []
        if old_block_numbers:
            ordered_numbers = [
                int(number)
                for number in old_block_numbers
            ]
            block_keys = [
                self._block_key(
                    chain_id,
                    block_number,
                )
                for block_number in ordered_numbers
            ]
            raw_states = await self.client.mget(
                block_keys
            )
            old_states = [
                StoredBlockState.model_validate_json(
                    raw_state
                )
                for raw_state in raw_states
                if raw_state is not None
            ]

        old_orphan_hashes = [
            str(block_hash).lower()
            for block_hash in (
                await self.client.zrangebyscore(
                    self._orphan_block_index_key(
                        chain_id
                    ),
                    "-inf",
                    cutoff,
                )
            )
        ]

        canonical_hashes = [
            state.block_hash.lower()
            for state in old_states
        ]
        all_block_hashes = list(
            dict.fromkeys(
                [
                    *canonical_hashes,
                    *old_orphan_hashes,
                ]
            )
        )

        event_ids_by_block = (
            await self._get_block_event_ids(
                chain_id=chain_id,
                block_hashes=all_block_hashes,
            )
        )

        all_event_ids: set[str] = set()
        for event_ids in event_ids_by_block.values():
            all_event_ids.update(event_ids)

        pipeline = self.client.pipeline(
            transaction=True
        )

        for state in old_states:
            pipeline.delete(
                self._block_key(
                    state.chain_id,
                    state.block_number,
                )
            )
            pipeline.delete(
                self._block_hash_key(
                    state.chain_id,
                    state.block_hash,
                )
            )
            pipeline.delete(
                self._block_events_key(
                    state.chain_id,
                    state.block_hash,
                )
            )
            pipeline.zrem(
                self._block_index_key(
                    state.chain_id
                ),
                str(state.block_number),
            )

        for block_hash in old_orphan_hashes:
            pipeline.delete(
                self._block_events_key(
                    chain_id,
                    block_hash,
                )
            )
            pipeline.zrem(
                self._orphan_block_index_key(
                    chain_id
                ),
                block_hash,
            )

        event_keys = [
            self._event_key(event_id)
            for event_id in all_event_ids
        ]

        unlink_batch_size = 500
        for index in range(
            0,
            len(event_keys),
            unlink_batch_size,
        ):
            batch = event_keys[
                index:index + unlink_batch_size
            ]
            if batch:
                pipeline.unlink(*batch)

        await pipeline.execute()

        for state in old_states:
            self._remove_cached_block_state(state)

        for block_hash in old_orphan_hashes:
            self._remove_cached_block_hash(
                chain_id,
                block_hash,
            )

        for event_id in all_event_ids:
            self._prefetched_event_states.pop(
                event_id,
                None,
            )

        self._prune_block_cache(
            chain_id=chain_id,
            head_block_number=head_block_number,
        )

        return len(old_states)

    async def apply_changes(
        self,
        *,
        event_states: list[StoredEventState],
        block_states_to_save: list[StoredBlockState],
        block_states_to_remove: list[StoredBlockState],
        head_state: StoredBlockState | None,
        pending_events_to_add: list[NormalizedEvent],
        pending_keys_to_delete: list[
            tuple[int, str]
        ],
    ) -> None:
        pipeline = self.client.pipeline(
            transaction=True
        )

        for state in event_states:
            event = state.event
            pipeline.set(
                self._event_key(event.event_id),
                state.model_dump_json(),
            )
            pipeline.sadd(
                self._block_events_key(
                    event.chain_id,
                    event.block_hash,
                ),
                event.event_id,
            )

        for state in block_states_to_remove:
            pipeline.delete(
                self._block_key(
                    state.chain_id,
                    state.block_number,
                )
            )
            pipeline.delete(
                self._block_hash_key(
                    state.chain_id,
                    state.block_hash,
                )
            )
            pipeline.zrem(
                self._block_index_key(
                    state.chain_id
                ),
                str(state.block_number),
            )
            pipeline.zadd(
                self._orphan_block_index_key(
                    state.chain_id
                ),
                {
                    state.block_hash.lower(): float(
                        state.block_number
                    )
                },
            )

        for state in block_states_to_save:
            pipeline.set(
                self._block_key(
                    state.chain_id,
                    state.block_number,
                ),
                state.model_dump_json(),
            )
            pipeline.set(
                self._block_hash_key(
                    state.chain_id,
                    state.block_hash,
                ),
                str(state.block_number),
            )
            pipeline.zadd(
                self._block_index_key(
                    state.chain_id
                ),
                {
                    str(state.block_number): float(
                        state.block_number
                    )
                },
            )
            pipeline.zrem(
                self._orphan_block_index_key(
                    state.chain_id
                ),
                state.block_hash.lower(),
            )

        for event in pending_events_to_add:
            pending_key = self._pending_key(
                event.chain_id,
                event.block_hash,
            )
            pipeline.hset(
                pending_key,
                event.event_id,
                event.model_dump_json(),
            )
            pipeline.expire(
                pending_key,
                self._pending_ttl_seconds,
            )

        for chain_id_value, block_hash in (
            pending_keys_to_delete
        ):
            pipeline.delete(
                self._pending_key(
                    chain_id_value,
                    block_hash,
                )
            )

        if head_state is not None:
            pipeline.set(
                self._head_key(
                    head_state.chain_id
                ),
                head_state.model_dump_json(),
            )

        await pipeline.execute()

        for state in event_states:
            event_id = state.event.event_id
            if event_id in self._prefetched_event_states:
                self._prefetched_event_states[
                    event_id
                ] = state

        for state in block_states_to_remove:
            self._remove_cached_block_state(state)

        for state in block_states_to_save:
            self._cache_block_state(state)

        if head_state is not None:
            self._cache_block_state(head_state)
            self._prune_block_cache(
                chain_id=head_state.chain_id,
                head_block_number=(
                    head_state.block_number
                ),
            )
