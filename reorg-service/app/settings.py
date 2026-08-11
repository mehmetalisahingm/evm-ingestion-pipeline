from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    normalized_topic: str = os.getenv(
        "KAFKA_NORMALIZED_TOPIC",
        "evm.normalized",
    )

    canonical_topic: str = os.getenv(
        "KAFKA_CANONICAL_TOPIC",
        "canonical-events",
    )

    dlq_topic: str = os.getenv(
        "KAFKA_DLQ_TOPIC",
        "evm.dlq",
    )

    consumer_group: str = os.getenv(
        "REORG_CONSUMER_GROUP",
        "reorg-service",
    )

    redis_url: str = os.getenv(
        "REDIS_URL",
        "redis://localhost:6379/0",
    )

    redis_window_size: int = int(
        os.getenv("REORG_WINDOW_SIZE", "50")
    )

    pending_event_ttl_seconds: int = int(
        os.getenv("REORG_PENDING_TTL_SECONDS", "120")
    )

    schema_version: int = int(
        os.getenv("REORG_SCHEMA_VERSION", "1")
    )

    monitoring_port: int = int(
        os.getenv("REORG_MONITORING_PORT", "8002")
    )


settings = Settings()
