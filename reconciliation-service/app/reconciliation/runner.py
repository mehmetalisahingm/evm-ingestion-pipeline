import asyncio
from time import perf_counter

from app.monitoring.metrics import (
    reconciliation_actual_blocks,
    reconciliation_actual_logs,
    reconciliation_clickhouse_head,
    reconciliation_discrepancies_total,
    reconciliation_duration_seconds,
    reconciliation_errors_total,
    reconciliation_expected_blocks,
    reconciliation_expected_logs,
    reconciliation_missing_blocks,
    reconciliation_pipeline_lag_blocks,
    reconciliation_rpc_head,
    reconciliation_runs_total,
)
from app.reconciliation.service import ReconciliationService


class ReconciliationRunner:
    def __init__(
        self,
        *,
        service: ReconciliationService,
        interval_seconds: float,
    ) -> None:
        self._service = service
        self._interval_seconds = interval_seconds

    async def run(self) -> None:
        while True:
            try:
                await self._run_once()

            except Exception as exc:
                reconciliation_errors_total.inc()

                print(
                    "Reconciliation hatası: "
                    f"{exc}",
                    flush=True,
                )

            await asyncio.sleep(
                self._interval_seconds
            )

    async def _run_once(self) -> None:
        started_at = perf_counter()

        try:
            result = await self._service.reconcile()

            reconciliation_runs_total.inc()

            reconciliation_rpc_head.set(
                result.rpc_head
            )

            reconciliation_clickhouse_head.set(
                result.clickhouse_head
            )

            reconciliation_pipeline_lag_blocks.set(
                result.pipeline_lag
            )

            reconciliation_expected_blocks.set(
                result.expected_block_count
            )

            reconciliation_actual_blocks.set(
                result.actual_block_count
            )

            reconciliation_expected_logs.set(
                result.expected_log_count
            )

            reconciliation_actual_logs.set(
                result.actual_log_count
            )

            reconciliation_missing_blocks.set(
                len(result.missing_blocks)
            )

            if (
                result.expected_block_count
                != result.actual_block_count
            ):
                reconciliation_discrepancies_total.labels(
                    type="block_count"
                ).inc()

            if (
                result.expected_log_count
                != result.actual_log_count
            ):
                reconciliation_discrepancies_total.labels(
                    type="log_count"
                ).inc()

            if result.missing_blocks:
                reconciliation_discrepancies_total.labels(
                    type="missing_blocks"
                ).inc()

            if result.has_discrepancy:
                print(
                    "RECONCILIATION ALERT | "
                    f"range={result.start_block}-"
                    f"{result.end_block} | "
                    f"blocks="
                    f"{result.actual_block_count}/"
                    f"{result.expected_block_count} | "
                    f"logs="
                    f"{result.actual_log_count}/"
                    f"{result.expected_log_count} | "
                    f"missing_blocks="
                    f"{result.missing_blocks} | "
                    f"pipeline_lag="
                    f"{result.pipeline_lag}",
                    flush=True,
                )

            else:
                print(
                    "Reconciliation başarılı | "
                    f"range={result.start_block}-"
                    f"{result.end_block} | "
                    f"rpc_head={result.rpc_head} | "
                    f"clickhouse_head="
                    f"{result.clickhouse_head} | "
                    f"pipeline_lag="
                    f"{result.pipeline_lag}",
                    flush=True,
                )

        finally:
            reconciliation_duration_seconds.observe(
                perf_counter() - started_at
            )