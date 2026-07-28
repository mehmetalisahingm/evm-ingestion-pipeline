from aiohttp import web 
from prometheus_client import CONTENT_TYPE_LATEST,generate_latest

class MonitoringState:
    def __init__(self) -> None:
        self.ready=False


monitoring_state = MonitoringState()

async def health_handler(
    request :web.Request,


)-> web.Response:
    return web.json_response(
        {"status":"ok"}
    )


async def ready_handler(
    request: web.Request,
) -> web.Response:
    if monitoring_state.ready:
        return web.json_response(
            {"status": "ready"}
        )

    return web.json_response(
        {"status": "not_ready"},
        status=503,
    )


async def metrics_handler(
    request: web.Request,
) -> web.Response:
    return web.Response(
        body=generate_latest(),
        headers={
            "Content-Type": CONTENT_TYPE_LATEST
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

    print(
        f"Monitoring sunucusu çalışıyor: "
        f"http://0.0.0.0:{port}"
    )

    return runner