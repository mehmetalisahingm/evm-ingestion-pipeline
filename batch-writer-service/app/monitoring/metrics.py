from prometheus_client import Counter, Gauge, Histogram


clickhouse_written_rows_total = Counter(
    "evm_clickhouse_written_rows_total",
    "ClickHouse'a başarıyla yazılan toplam kayıt sayısı.",
    ["table"],
)

batch_flush_total = Counter(
    "evm_batch_flush_total",
    "Gerçekleştirilen toplam batch flush sayısı.",
)

batch_flush_duration_seconds = Histogram(
    "evm_batch_flush_duration_seconds",
    "Batch'in ClickHouse'a yazılma süresi.",
)

batch_buffer_size = Gauge(
    "evm_batch_buffer_size",
    "Batch Writer belleğindeki mevcut kayıt sayısı.",
)

batch_retry_total = Counter(
    "evm_batch_retry_total",
    "ClickHouse yazımı için yapılan toplam retry sayısı.",
    ["table"],
)

batch_failed_total = Counter(
    "evm_batch_failed_total",
    "Retry denemelerinden sonra başarısız olan batch sayısı.",
    ["table"],
)

retry_writer_batches_total = Counter(
    "evm_retry_writer_batches_total",
    "evm.retry_writer topic'ine gönderilen batch sayısı.",
)

batch_writer_processing_errors_total = Counter(
    "evm_batch_writer_processing_errors_total",
    "Batch Writer tarafındaki beklenmeyen hata sayısı.",
)

batch_writer_ready = Gauge(
    "evm_batch_writer_ready",
    "Batch Writer servisinin readiness durumu.",
)