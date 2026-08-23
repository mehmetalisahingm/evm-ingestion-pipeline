import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:19092",
    )

    canonical_topic: str = os.getenv(
        "KAFKA_CANONICAL_TOPIC",
        "canonical-events",
    )

    retry_writer_topic: str = os.getenv(
        "KAFKA_RETRY_WRITER_TOPIC",
        "evm.retry_writer",
    )

    dlq_topic: str = os.getenv(
        "KAFKA_DLQ_TOPIC",
        "evm.dlq",
    )

    consumer_group: str = os.getenv(
        "BATCH_WRITER_CONSUMER_GROUP",
        "batch-writer-service",
    )

    # Batch
    batch_size: int = int(
        os.getenv("BATCH_WRITER_BATCH_SIZE", "2000")
    )

    flush_interval_seconds: float = float(
        os.getenv(
            "BATCH_WRITER_FLUSH_INTERVAL_SECONDS",
            "3",
        )
    )

    # ClickHouse
    clickhouse_url: str = os.getenv(
        "CLICKHOUSE_URL",
        "http://clickhouse:8123",
    )

    clickhouse_database: str = os.getenv(
        "CLICKHOUSE_DATABASE",
        "evm",
    )

    clickhouse_user: str = os.getenv(
        "CLICKHOUSE_USER",
        "default",
    )

    clickhouse_password: str = os.getenv(
        "CLICKHOUSE_PASSWORD",
        "",
    )

    # Monitoring
    monitoring_port: int = int(
        os.getenv(
            "BATCH_WRITER_MONITORING_PORT",
            "8003",
        )
    )


settings = Settings()