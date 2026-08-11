from prometheus_client import Counter, Gauge, Histogram


RAW_MESSAGES_TOTAL = Counter(
    "evm_normalizer_raw_messages_total",
    "evm.raw topic'inden alınan toplam mesaj sayısı",
)

NORMALIZED_EVENTS_TOTAL = Counter(
    "evm_normalizer_events_total",
    "evm.normalized topic'ine gönderilen toplam event sayısı",
    ["event_type"],
)

DLQ_MESSAGES_TOTAL = Counter(
    "evm_normalizer_dlq_messages_total",
    "evm.dlq topic'ine gönderilen toplam hatalı mesaj sayısı",
    ["error_type"],
)

PROCESSING_ERRORS_TOTAL = Counter(
    "evm_normalizer_processing_errors_total",
    "DLQ dışındaki altyapı veya beklenmeyen hata sayısı",
    ["error_type"],
)

OFFSET_COMMITS_TOTAL = Counter(
    "evm_normalizer_offset_commits_total",
    "Başarıyla commit edilen Kafka offset sayısı",
)

PROCESSING_DURATION_SECONDS = Histogram(
    "evm_normalizer_processing_duration_seconds",
    "Bir evm.raw mesajının işlenme süresi",
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
    ),
)

NORMALIZER_READY = Gauge(
    "evm_normalizer_ready",
    "Normalizer servisinin mesaj işlemeye hazır olup olmadığı",
)
