import asyncio

from app.clickhouse.client import ClickHouseReadClient
from app.monitoring.server import MonitoringServer
from app.reconciliation.runner import ReconciliationRunner
from app.reconciliation.service import ReconciliationService
from app.rpc.client import BscRpcClient
from app.settings import settings


async def main() -> None:
    rpc_client = BscRpcClient(
        url=settings.http_rpc_url,
    )

    clickhouse_client = ClickHouseReadClient(
        url=settings.clickhouse_url,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )

    reconciliation_service = ReconciliationService(
        rpc_client=rpc_client,
        clickhouse_client=clickhouse_client,
        chain_id=settings.chain_id,
        window_size=settings.reconciliation_window_size,
    )

    reconciliation_runner = ReconciliationRunner(
        service=reconciliation_service,
        interval_seconds=(
            settings.reconciliation_interval_seconds
        ),
    )

    monitoring_server = MonitoringServer(
        port=settings.monitoring_port,
    )

    try:
        await monitoring_server.start()

        await rpc_client.start()
        await clickhouse_client.start()

        monitoring_server.set_ready(True)

        print("Reconciliation Service başladı.")

        await reconciliation_runner.run()

    finally:
        monitoring_server.set_ready(False)

        await rpc_client.stop()
        await clickhouse_client.stop()
        await monitoring_server.stop()


if __name__ == "__main__":
    asyncio.run(main())