import asyncio

import aiohttp

from app.common.backoff import calculate_backoff
from app.settings import HTTP_RPC_URL


async def rpc_call(
    session: aiohttp.ClientSession,
    method: str,
    params: list,
    max_retries: int = 5,
):
    if not HTTP_RPC_URL:
        raise RuntimeError("HTTP_RPC_URL .env dosyasında bulunamadı.")

    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    }

    for retry_number in range(max_retries + 1):
        retry_after = None

        async with session.post(
            HTTP_RPC_URL,
            json=payload,
        ) as response:

            if response.status == 429:
                retry_after = response.headers.get("Retry-After")

            else:
                response.raise_for_status()

                data = await response.json()

                if "error" in data:
                    raise RuntimeError(data["error"])

                return data["result"]

        if retry_number == max_retries:
            raise RuntimeError(
                "HTTP isteği 5 yeniden denemeden sonra başarısız oldu."
            )

        try:
            delay = (
                float(retry_after)
                if retry_after
                else calculate_backoff(retry_number)
            )
        except ValueError:
            delay = calculate_backoff(retry_number)

        print(
            f"HTTP 429 alındı. "
            f"{delay:.2f} saniye sonra yeniden denenecek."
        )

        await asyncio.sleep(delay)

    raise RuntimeError("HTTP isteği tamamlanamadı.")

async def get_block_by_number(
    session: aiohttp.ClientSession,
    block_number: int,
    max_retries: int = 3,
) -> dict:
    block_number_hex = hex(block_number)

    for retry_number in range(max_retries + 1):
        block = await rpc_call(
            session=session,
            method="eth_getBlockByNumber",
            params=[block_number_hex, True],
        )

        if block is not None:
            return block

        if retry_number == max_retries:
            break

        delay = calculate_backoff(
            retry_number=retry_number,
            base_delay=0.5,
        )

        print(
            f"{block_number} numaralı blok HTTP tarafında henüz hazır değil. "
            f"{delay:.2f} saniye sonra tekrar denenecek."
        )

        await asyncio.sleep(delay)

    raise RuntimeError(
        f"{block_number} numaralı blok "
        f"{max_retries} yeniden denemeden sonra bulunamadı."
    )

async def get_latest_block_number(
        session:aiohttp.ClientSession,

)-> int:
    block_number_hex= await rpc_call(
        session=session,
        method="eth_blockNumber",
        params=[],
    )

    return int(block_number_hex,16)