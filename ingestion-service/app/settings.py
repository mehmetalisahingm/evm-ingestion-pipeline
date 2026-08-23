import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)

CHAIN_NAME = os.getenv("CHAIN_NAME", "bsc")
CHAIN_ID = int(os.getenv("CHAIN_ID", "56"))

HTTP_RPC_URL = os.getenv("HTTP_RPC_URL")
WS_RPC_URL = os.getenv("WS_RPC_URL")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "evm.raw",
)

BLOCK_QUEUE_MAX_SIZE = int(
    os.getenv("BLOCK_QUEUE_MAX_SIZE", "500")
)

LOG_QUEUE_MAX_SIZE = int(
    os.getenv("LOG_QUEUE_MAX_SIZE", "5000")
)

# Backfill sırasında RPC'den birkaç blok paralel çekilebilir,
# ancak Kafka'ya verilecek tahmini normalized event hızı ayrıca
# sınırlandırılır. Böylece Re-org servisi dev catch-up sırasında
# gereksiz yere yüz binlerce event geriye düşmez.
BACKFILL_CONCURRENCY = int(
    os.getenv("BACKFILL_CONCURRENCY", "5")
)

BACKFILL_TARGET_EVENTS_PER_SECOND = float(
    os.getenv(
        "BACKFILL_TARGET_EVENTS_PER_SECOND",
        "1000",
    )
)

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://localhost:6379/0",
)

MONITORING_PORT = int(
    os.getenv("MONITORING_PORT", "8000")
)
