from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv



PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

# servis çalıştıktan sonra ayarlar değiştirilemez
@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    raw_topic: str = os.getenv(
        "KAFKA_RAW_TOPIC",
        "evm.raw",
    )

    normalized_topic: str = os.getenv(
        "KAFKA_NORMALIZED_TOPIC",
        "evm.normalized",
    )

    dlq_topic: str = os.getenv(
        "KAFKA_DLQ_TOPIC",
        "evm.dlq",
    )

    consumer_group: str = os.getenv(
        "NORMALIZER_CONSUMER_GROUP",
        "normalizer-service",
    )

    schema_version: int = int(
        os.getenv("NORMALIZER_SCHEMA_VERSION", "1")
    )

    monitoring_port: int = int(
        os.getenv("NORMALIZER_MONITORING_PORT", "8001")
    )


settings = Settings()
