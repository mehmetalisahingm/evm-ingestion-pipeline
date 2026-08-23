from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.monitoring.metrics import reconciliation_ready


class MonitoringServer:
    def __init__(
        self,
        *,
        port: int,
    ) -> None:
        self._port = port
        self._ready = False

        self._app = web.Application()
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

        self._app.router.add_get(
            "/health",
            self._health,
        )

        self._app.router.add_get(
            "/ready",
            self._readiness,
        )

        self._app.router.add_get(
            "/metrics",
            self._metrics,
        )

    async def start(self) -> None:
        self._runner = web.AppRunner(
            self._app
        )

        await self._runner.setup()

        self._site = web.TCPSite(
            self._runner,
            "0.0.0.0",
            self._port,
        )

        await self._site.start()

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    def set_ready(
        self,
        ready: bool,
    ) -> None:
        self._ready = ready

        reconciliation_ready.set(
            1 if ready else 0
        )

    async def _health(
        self,
        request: web.Request,
    ) -> web.Response:
        return web.json_response(
            {
                "status": "ok",
                "service": "reconciliation-service",
            }
        )

    async def _readiness(
        self,
        request: web.Request,
    ) -> web.Response:
        if self._ready:
            return web.json_response(
                {
                    "status": "ready",
                }
            )

        return web.json_response(
            {
                "status": "not_ready",
            },
            status=503,
        )

    async def _metrics(
        self,
        request: web.Request,
    ) -> web.Response:
        return web.Response(
            body=generate_latest(),
            headers={
                "Content-Type": CONTENT_TYPE_LATEST,
            },
        )