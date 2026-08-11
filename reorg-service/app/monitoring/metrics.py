from prometheus_client import Counter, Gauge


NORMALIZED_MESSAGES_TOTAL = Counter(
    "reorg_normalized_messages_total",
    "Re-org Service tarafından okunan normalized mesaj sayısı",
)

CANONICAL_EVENTS_TOTAL = Counter(
    "reorg_canonical_events_total",
    "canonical-events topic'ine gönderilen event sayısı",
)

DUPLICATE_EVENTS_TOTAL = Counter(
    "reorg_duplicate_events_total",
    "Tespit edilen duplicate event sayısı",
)

REORG_DETECTED_TOTAL = Counter(
    "reorg_detected_total",
    "Tespit edilen re-org sayısı",
)

DLQ_MESSAGES_TOTAL = Counter(
    "reorg_dlq_messages_total",
    "DLQ'ya gönderilen mesaj sayısı",
)

OFFSET_COMMITS_TOTAL = Counter(
    "reorg_offset_commits_total",
    "Başarılı Kafka offset commit sayısı",
)

PROCESSING_ERRORS_TOTAL = Counter(
    "reorg_processing_errors_total",
    "Beklenmeyen işlem hatası sayısı",
)

SERVICE_READY = Gauge(
    "reorg_service_ready",
    "Servisin Kafka ve Redis bağlantılarının hazır olup olmadığı",
)
