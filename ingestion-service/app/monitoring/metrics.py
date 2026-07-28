from prometheus_client import Counter, Gauge


BLOCKS_PUBLISHED_TOTAL = Counter(
    "evm_blocks_published_total",
    "Kafka'ya başarıyla gönderilen toplam blok sayısı",
)

LOGS_PUBLISHED_TOTAL = Counter(
    "evm_logs_published_total",
    "Kafka'ya başarıyla gönderilen toplam log sayısı",
)

BACKFILL_BLOCKS_TOTAL = Counter(
    "evm_backfill_blocks_total",
    "HTTP backfill ile alınan toplam blok sayısı",
)

BLOCK_QUEUE_SIZE = Gauge(
    "evm_block_queue_size",
    "Blok kuyruğunda bekleyen event sayısı",
)

LOG_QUEUE_SIZE = Gauge(
    "evm_log_queue_size",
    "Log kuyruğunda bekleyen event sayısı",
)

CHECKPOINT_BLOCK = Gauge(
    "evm_checkpoint_block",
    "Redis'e kaydedilen son başarılı blok numarası",
)