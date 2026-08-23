from dataclasses import dataclass

from app.clickhouse.client import ClickHouseReadClient
from app.rpc.client import BscRpcClient


@dataclass
class ReconciliationResult:
    rpc_head: int
    clickhouse_head: int
    pipeline_lag: int

    start_block: int
    end_block: int

    expected_block_count: int
    actual_block_count: int

    expected_log_count: int
    actual_log_count: int

    missing_blocks: list[int]

    @property
    def has_discrepancy(self) -> bool:
        return (
            self.expected_block_count
            != self.actual_block_count
            or self.expected_log_count
            != self.actual_log_count
            or bool(self.missing_blocks)
        )


class ReconciliationService:
    def __init__(
        self,
        *,
        rpc_client: BscRpcClient,
        clickhouse_client: ClickHouseReadClient,
        chain_id: int,
        window_size: int,
        safety_lag: int = 5,
    ) -> None:
        self._rpc_client = rpc_client
        self._clickhouse_client = clickhouse_client
        self._chain_id = chain_id
        self._window_size = window_size
        self._safety_lag = safety_lag

    async def reconcile(
        self,
    ) -> ReconciliationResult:
        rpc_head = (
            await self._rpc_client.get_latest_block_number()
        )

        clickhouse_head = (
            await self._clickhouse_client.get_latest_block_number(
                chain_id=self._chain_id,
                max_block_number=rpc_head,
            )
        )

        pipeline_lag = max(
            0,
            rpc_head - clickhouse_head,
        )

        if clickhouse_head <= 0:
            return ReconciliationResult(
                rpc_head=rpc_head,
                clickhouse_head=0,
                pipeline_lag=pipeline_lag,
                start_block=0,
                end_block=0,
                expected_block_count=0,
                actual_block_count=0,
                expected_log_count=0,
                actual_log_count=0,
                missing_blocks=[],
            )

        end_block = max(
            0,
            clickhouse_head - self._safety_lag,
        )

        start_block = max(
            0,
            end_block - self._window_size + 1,
        )

        expected_block_numbers = set(
            range(
                start_block,
                end_block + 1,
            )
        )

        actual_block_numbers = (
            await self._clickhouse_client.get_block_numbers(
                chain_id=self._chain_id,
                start_block=start_block,
                end_block=end_block,
            )
        )

        actual_block_count = (
            await self._clickhouse_client.get_block_count(
                chain_id=self._chain_id,
                start_block=start_block,
                end_block=end_block,
            )
        )

        expected_log_count = 0

        for block_number in range(
            start_block,
            end_block + 1,
        ):
            logs = (
                await self._rpc_client.get_logs_for_block(
                    block_number
                )
            )

            expected_log_count += len(logs)

        actual_log_count = (
            await self._clickhouse_client.get_log_count(
                chain_id=self._chain_id,
                start_block=start_block,
                end_block=end_block,
            )
        )

        missing_blocks = sorted(
            expected_block_numbers
            - actual_block_numbers
        )

        return ReconciliationResult(
            rpc_head=rpc_head,
            clickhouse_head=clickhouse_head,
            pipeline_lag=pipeline_lag,
            start_block=start_block,
            end_block=end_block,
            expected_block_count=len(
                expected_block_numbers
            ),
            actual_block_count=actual_block_count,
            expected_log_count=expected_log_count,
            actual_log_count=actual_log_count,
            missing_blocks=missing_blocks,
        )