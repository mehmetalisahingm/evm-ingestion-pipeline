import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # BSC RPC
    http_rpc_url: str = os.getenv(
        "HTTP_RPC_URL",
        "",
    )

    chain_id: int = int(
        os.getenv("CHAIN_ID", "56")
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

    # Reconciliation
    reconciliation_interval_seconds: float = float(
        os.getenv(
            "RECONCILIATION_INTERVAL_SECONDS",
            "30",
        )
    )

    reconciliation_window_size: int = int(
        os.getenv(
            "RECONCILIATION_WINDOW_SIZE",
            "20",
        )
    )

    # Monitoring
    monitoring_port: int = int(
        os.getenv(
            "RECONCILIATION_MONITORING_PORT",
            "8004",
        )
    )


settings = Settings()