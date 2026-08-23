import asyncio

from app.batch.buffer import BatchBuffer
from app.batch.parser import (
    CanonicalEventParsingError,
    parse_canonical_event,
)
from app.batch.service import BatchWriterService
from app.kafka.consumer import BatchWriterKafkaConsumer
from app.kafka.producer import BatchWriterKafkaProducer
from app.monitoring.metrics import (
    batch_buffer_size,
    batch_writer_processing_errors_total,
)


class BatchProcessor:
    def __init__(
        self,
        *,
        consumer: BatchWriterKafkaConsumer,
        producer: BatchWriterKafkaProducer,
        service: BatchWriterService,
        buffer: BatchBuffer,
        flush_interval_seconds: float,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._service = service
        self._buffer = buffer
        self._flush_interval_seconds = (
            flush_interval_seconds
        )

    async def run(self) -> None:
        while True:
            try:
                if (
                    self._buffer.should_flush_by_size()
                    or self._buffer.should_flush_by_time()
                ):
                    await self._flush()
                    continue

                timeout = self._get_wait_timeout()

                message = await asyncio.wait_for(
                    self._consumer.getone(),
                    timeout=timeout,
                )

                await self._process_message(message)

            except asyncio.TimeoutError:
                if not self._buffer.is_empty():
                    await self._flush()

            except Exception:
                batch_writer_processing_errors_total.inc()
                raise

    def _get_wait_timeout(self) -> float:
        if self._buffer.is_empty():
            return self._flush_interval_seconds

        return self._buffer.seconds_until_flush()

    async def _process_message(
        self,
        message,
    ) -> None:
        try:
            event = parse_canonical_event(
                message.value
            )

        except CanonicalEventParsingError as exc:
            await self._producer.send_dlq(
                original_value=message.value,
                error_type=type(exc).__name__,
                error_reason=str(exc),
            )

            await self._consumer.commit_message(
                message
            )

            return

        self._buffer.add(
            kafka_message=message,
            event=event,
        )

        batch_buffer_size.set(
            self._buffer.size()
        )

    async def _flush(self) -> None:
        if self._buffer.is_empty():
            return

        records = self._buffer.take_all()

        batch_buffer_size.set(0)

        try:
            await self._service.flush(records)

        except Exception:
            self._buffer.restore(records)

            batch_buffer_size.set(
                self._buffer.size()
            )

            raise

        self._buffer.mark_flushed()