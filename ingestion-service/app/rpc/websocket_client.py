import asyncio
import json
from collections.abc import AsyncIterator

from websockets.asyncio.client import connect

from app.common.backoff import calculate_backoff
from app.settings import WS_RPC_URL


async def _stream_subscription(
    subscription_type: str,
    subscription_options: dict | None = None,
    request_id: int = 1,
    max_retries: int = 5,
) -> AsyncIterator[dict]:
    if not WS_RPC_URL:
        raise RuntimeError(
            "WS_RPC_URL .env dosyasında bulunamadı."
        )

    retry_number = 0

    while True:
        try:
            async with connect(
                WS_RPC_URL,
                open_timeout=10,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:
                params = [
                    subscription_type
                ]

                if (
                    subscription_options
                    is not None
                ):
                    params.append(
                        subscription_options
                    )

                request = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_subscribe",
                    "params": params,
                }

                await websocket.send(
                    json.dumps(request)
                )

                response = json.loads(
                    await websocket.recv()
                )

                if "error" in response:
                    raise RuntimeError(
                        response["error"]
                    )

                print(
                    f"{subscription_type} "
                    "aboneliği oluştu:",
                    response["result"],
                    flush=True,
                )

                retry_number = 0

                async for raw_message in websocket:
                    message = json.loads(
                        raw_message
                    )

                    result = (
                        message.get(
                            "params",
                            {},
                        ).get(
                            "result"
                        )
                    )

                    if result:
                        yield result

        except asyncio.CancelledError:
            raise

        except Exception as error:
            retry_number += 1

            if retry_number >= max_retries:
                raise RuntimeError(
                    f"{subscription_type} bağlantısı "
                    f"{max_retries} denemeden "
                    "sonra kurulamadı."
                ) from error

            delay = calculate_backoff(
                retry_number - 1
            )

            print(
                f"{subscription_type} "
                "bağlantısı kesildi: "
                f"{error}\n"
                f"{delay:.2f} saniye "
                "sonra yeniden denenecek. "
                f"Deneme: "
                f"{retry_number}/{max_retries}",
                flush=True,
            )

            await asyncio.sleep(
                delay
            )


async def stream_new_blocks() -> AsyncIterator[dict]:
    async for block in _stream_subscription(
        subscription_type="newHeads",
        request_id=1,
    ):
        yield block


async def stream_logs() -> AsyncIterator[dict]:
    async for log in _stream_subscription(
        subscription_type="logs",
        subscription_options={},
        request_id=2,
    ):
        yield log