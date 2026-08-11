from redis.asyncio import Redis

from app.reorg.models import (
    NormalizedEvent,
    StoredBlockState,
    StoredEventState,
)


class RedisStateStore:
    """
    Re-org ve idempotency durumlarını Redis'te saklar.
    """

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

    async def stop(self) -> None:
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
        return (
            f"reorg:block:{chain_id}:"
            f"{block_number}"
        )

    @staticmethod
    def _block_hash_key(
        chain_id: int,
        block_hash: str,
    ) -> str:
        return (
            f"reorg:block-hash:{chain_id}:"
            f"{block_hash.lower()}"
        )

    @staticmethod
    def _block_events_key(
        chain_id: int,
        block_hash: str,
    ) -> str:
        return (
            f"reorg:block-events:{chain_id}:"
            f"{block_hash.lower()}"
        )

    @staticmethod
    def _head_key(chain_id: int) -> str:
        return f"reorg:head:{chain_id}"

    @staticmethod
    def _block_index_key(chain_id: int) -> str:
        return f"reorg:block-index:{chain_id}"

    @staticmethod
    def _pending_key(
        chain_id: int,
        block_hash: str,
    ) -> str:
        return (
            f"reorg:pending:{chain_id}:"
            f"{block_hash.lower()}"
        )

    async def get_event_state(
        self,
        event_id: str,
    ) -> StoredEventState | None:
        raw_state = await self.client.get(
            self._event_key(event_id)
        )

        if raw_state is None:
            return None

        return StoredEventState.model_validate_json(
            raw_state
        )

    async def save_event_state(
        self,
        state: StoredEventState,
    ) -> None:
        """
        Event'in son durumunu saklar ve event'i ait olduğu
        blokla ilişkilendirir.
        """

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

    async def get_block_event_states(
        self,
        chain_id: int,
        block_hash: str,
    ) -> list[StoredEventState]:
        """
        Belirtilen bloğa ait bütün event durumlarını getirir.
        Re-org sırasında bunlar canonical=false yapılacaktır.
        """

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

        raw_states = await self.client.mget(
            event_keys
        )

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

        return StoredBlockState.model_validate_json(
            raw_state
        )

    async def get_block_by_hash(
        self,
        chain_id: int,
        block_hash: str,
    ) -> StoredBlockState | None:
        block_number = await self.client.get(
            self._block_hash_key(
                chain_id,
                block_hash,
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

        if state.block_hash.lower() != block_hash.lower():
            return None

        return state

    async def save_block_state(
        self,
        state: StoredBlockState,
    ) -> None:
        """
        Canonical blok durumunu, hash indeksini ve blok
        numarası indeksini birlikte saklar.
        """

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

        await pipeline.execute()

    async def remove_block_state(
        self,
        state: StoredBlockState,
    ) -> None:
        """
        Bloğu canonical blok indeksinden kaldırır.

        Event durumları silinmez; re-org sırasında onların
        canonical=false sürümleri oluşturulacaktır.
        """

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

        await pipeline.execute()

    async def get_head(
        self,
        chain_id: int,
    ) -> StoredBlockState | None:
        raw_state = await self.client.get(
            self._head_key(chain_id)
        )

        if raw_state is None:
            return None

        return StoredBlockState.model_validate_json(
            raw_state
        )

    async def set_head(
        self,
        state: StoredBlockState,
    ) -> None:
        await self.client.set(
            self._head_key(state.chain_id),
            state.model_dump_json(),
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
        """
        Verilen blok numarasından sonraki canonical blokları
        küçükten büyüğe getirir.
        """

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

        return [
            StoredBlockState.model_validate_json(
                raw_state
            )
            for raw_state in raw_states
            if raw_state is not None
        ]




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
        """
        Bekleyen event'leri silmeden getirir.
        """

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
        """
        Kafka gönderimleri başarıyla tamamlandıktan sonra
        pending event'leri siler.
        """

        await self.client.delete(
            self._pending_key(
                chain_id,
                block_hash,
            )
        )

    async def prune_old_blocks(
        self,
        chain_id: int,
        head_block_number: int,
    ) -> int:
        """
        Redis'te yalnızca son N canonical bloğu tutar.
        Event idempotency kayıtları korunur.
        """

        cutoff = (
            head_block_number
            - self._window_size
        )

        if cutoff < 0:
            return 0

        old_block_numbers = await self.client.zrangebyscore(
            self._block_index_key(chain_id),
            "-inf",
            cutoff,
        )

        if not old_block_numbers:
            return 0

        old_states: list[StoredBlockState] = []

        for block_number in old_block_numbers:
            state = await self.get_block_state(
                chain_id=chain_id,
                block_number=int(block_number),
            )

            if state is not None:
                old_states.append(state)

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

        await pipeline.execute()

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
        """
        Re-org işleminden oluşan tüm Redis değişikliklerini
        tek transaction içinde uygular.
        """

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

        for chain_id, block_hash in pending_keys_to_delete:
            pipeline.delete(
                self._pending_key(
                    chain_id,
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
