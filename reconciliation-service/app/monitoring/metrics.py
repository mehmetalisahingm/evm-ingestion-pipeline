from prometheus_client import Counter, Gauge, Histogram


reconciliation_runs_total = Counter(
    "evm_reconciliation_runs_total",
    "Toplam reconciliation çalışma sayısı.",
)

reconciliation_discrepancies_total = Counter(
    "evm_reconciliation_discrepancies_total",
    "Tespit edilen toplam reconciliation uyuşmazlığı sayısı.",
    ["type"],
)

reconciliation_duration_seconds = Histogram(
    "evm_reconciliation_duration_seconds",
    "Bir reconciliation işleminin tamamlanma süresi.",
)

reconciliation_expected_blocks = Gauge(
    "evm_reconciliation_expected_blocks",
    "RPC tarafında beklenen blok sayısı.",
)

reconciliation_actual_blocks = Gauge(
    "evm_reconciliation_actual_blocks",
    "ClickHouse tarafında bulunan blok sayısı.",
)

reconciliation_expected_logs = Gauge(
    "evm_reconciliation_expected_logs",
    "RPC tarafında beklenen log sayısı.",
)

reconciliation_actual_logs = Gauge(
    "evm_reconciliation_actual_logs",
    "ClickHouse tarafında bulunan log sayısı.",
)

reconciliation_missing_blocks = Gauge(
    "evm_reconciliation_missing_blocks",
    "Son reconciliation işleminde eksik bulunan blok sayısı.",
)

reconciliation_errors_total = Counter(
    "evm_reconciliation_errors_total",
    "Reconciliation sırasında oluşan toplam hata sayısı.",
)

reconciliation_ready = Gauge(
    "evm_reconciliation_ready",
    "Reconciliation servisinin readiness durumu.",
)

reconciliation_rpc_head = Gauge(
    "evm_reconciliation_rpc_head",
    "RPC tarafındaki güncel blok numarası.",
)

reconciliation_clickhouse_head = Gauge(
    "evm_reconciliation_clickhouse_head",
    "ClickHouse tarafındaki son canonical blok numarası.",
)

reconciliation_pipeline_lag_blocks = Gauge(
    "evm_reconciliation_pipeline_lag_blocks",
    "RPC head ile ClickHouse head arasındaki blok farkı.",
)