from collections import defaultdict
from time import perf_counter
from typing import Any

from app.batch.buffer import BufferedRecord
from app.batch.mapper import map_event_to_clickhouse
from app.batch.retry import wait_before_retry
from app.clickhouse.client import (
    ClickHouseClient,
    ClickHouseWriteError,
)
from app.kafka.consumer import BatchWriterKafkaConsumer
from app.kafka.producer import BatchWriterKafkaProducer
from app.monitoring.metrics import (
    batch_failed_total,
    batch_flush_duration_seconds,
    batch_flush_total,
    batch_retry_total,
    clickhouse_written_rows_total,
    retry_writer_batches_total,
)


class BatchFlushError(Exception):
    pass


class BatchWriterService:
    def __init__(
        self,
        *,
        clickhouse_client: ClickHouseClient,
        consumer: BatchWriterKafkaConsumer,
        producer: BatchWriterKafkaProducer,
        max_retries: int = 3,
    ) -> None:
        self._clickhouse_client = clickhouse_client
        self._consumer = consumer
        self._producer = producer
        self._max_retries = max_retries

    async def flush(
        self,
        records: list[BufferedRecord],
    ) -> None:
        if not records:
            return

        started_at = perf_counter()

        grouped_rows: dict[
            str,
            list[tuple[BufferedRecord, dict[str, Any]]],
        ] = defaultdict(list)

        for record in records:
            table, row = map_event_to_clickhouse(
                record.event
            )

            grouped_rows[table].append(
                (record, row)
            )

        failed_records: list[BufferedRecord] = []

        try:
            for table, table_records in grouped_rows.items():
                rows = [
                    row
                    for _, row in table_records
                ]

                success = await self._write_with_retry(
                    table=table,
                    rows=rows,
                )

                if success:
                    clickhouse_written_rows_total.labels(
                        table=table
                    ).inc(len(rows))

                else:
                    batch_failed_total.labels(
                        table=table
                    ).inc()

                    failed_records.extend(
                        record
                        for record, _ in table_records
                    )

            if failed_records:
                await self._send_failed_records_to_retry(
                    failed_records
                )

            kafka_messages = [
                record.kafka_message
                for record in records
            ]

            await self._consumer.commit_batch(
                kafka_messages
            )

            batch_flush_total.inc()

        finally:
            duration = perf_counter() - started_at

            batch_flush_duration_seconds.observe(
                duration
            )

    async def _write_with_retry(
        self,
        *,
        table: str,
        rows: list[dict[str, Any]],
    ) -> bool:
        for attempt in range(
            self._max_retries + 1
        ):
            try:
                await self._clickhouse_client.insert_rows(
                    table=table,
                    rows=rows,
                )

                return True

            except ClickHouseWriteError:
                if attempt >= self._max_retries:
                    return False

                batch_retry_total.labels(
                    table=table
                ).inc()

                await wait_before_retry(attempt)

        return False

    async def _send_failed_records_to_retry(
        self,
        records: list[BufferedRecord],
    ) -> None:
        retry_records = [
            record.event.model_dump(
                mode="json"
            )
            for record in records
        ]

        try:
            await self._producer.send_retry_batch(
                records=retry_records,
                error_type="ClickHouseWriteError",
                error_reason=(
                    "ClickHouse yazımı retry "
                    "denemelerinden sonra başarısız oldu."
                ),
                retry_count=self._max_retries,
            )

            retry_writer_batches_total.inc()

        except Exception as exc:
            raise BatchFlushError(
                "Başarısız batch "
                "evm.retry_writer topic'ine "
                "gönderilemedi. Kafka offset "
                "commit edilmeyecek."
            ) from exc