from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .metrics import NORMALIZER_READY


class MonitoringState:

    def __init__(self) -> None:
        self.ready = False

    def set_ready(self, ready: bool) -> None:
        self.ready = ready
        NORMALIZER_READY.set(1 if ready else 0)


monitoring_state = MonitoringState()


async def health_handler(
    request: web.Request,
) -> web.Response:
    return web.json_response(
        {
            "status": "ok",
            "service": "normalizer-service",
        }
    )


async def ready_handler(
    request: web.Request,
) -> web.Response:
    if monitoring_state.ready:
        return web.json_response(
            {
                "status": "ready",
                "service": "normalizer-service",
            }
        )

    return web.json_response(
        {
            "status": "not_ready",
            "service": "normalizer-service",
        },
        status=503,
    )


async def metrics_handler(
    request: web.Request,
) -> web.Response:
    return web.Response(
        body=generate_latest(),
        headers={
            "Content-Type": CONTENT_TYPE_LATEST,
        },
    )


async def start_monitoring_server(
    port: int,
) -> web.AppRunner:
    application = web.Application()

    application.router.add_get(
        "/health",
        health_handler,
    )

    application.router.add_get(
        "/ready",
        ready_handler,
    )

    application.router.add_get(
        "/metrics",
        metrics_handler,
    )

    runner = web.AppRunner(application)
    await runner.setup()

    site = web.TCPSite(
        runner,
        host="0.0.0.0",
        port=port,
    )

    await site.start()

    print(
        "Normalizer monitoring sunucusu çalışıyor: "
        f"http://0.0.0.0:{port}"
    )

    return runner
