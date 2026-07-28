import asyncio

import aiohttp

from app.rpc.http_client import get_latest_block_number
from app.storage.checkpoint import CHECKPOINT_KEY
from app.storage.redis_client import create_redis_client


async def main() -> None:
    redis_client = create_redis_client()

    try:
        async with aiohttp.ClientSession() as session:
            latest_block = await get_latest_block_number(session)

        test_checkpoint = latest_block - 5

        await redis_client.set(
            CHECKPOINT_KEY,
            test_checkpoint,
        )

        print("Güncel blok:", latest_block)
        print("Test checkpoint:", test_checkpoint)

    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())