from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.monitoring.metrics import SERVICE_READY


async def health_handler(
    request: web.Request,
) -> web.Response:
    return web.json_response(
        {
            "status": "ok",
            "service": "reorg-service",
        }
    )


async def ready_handler(
    request: web.Request,
) -> web.Response:
    ready = SERVICE_READY._value.get() == 1

    if ready:
        return web.json_response(
            {
                "status": "ready",
                "service": "reorg-service",
            }
        )

    return web.json_response(
        {
            "status": "not_ready",
            "service": "reorg-service",
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
    app = web.Application()

    app.router.add_get(
        "/health",
        health_handler,
    )

    app.router.add_get(
        "/ready",
        ready_handler,
    )

    app.router.add_get(
        "/metrics",
        metrics_handler,
    )

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        host="0.0.0.0",
        port=port,
    )

    await site.start()

    return runner
