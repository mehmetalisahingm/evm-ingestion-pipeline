import asyncio
from uuid import uuid4

from app.storage import checkpoint
from app.storage.redis_client import create_redis_client


async def main() -> None:
    redis_client = create_redis_client()

    test_key = f"evm:test:checkpoint:{uuid4()}"
    checkpoint.CHECKPOINT_KEY = test_key

    try:
        first_value = await checkpoint.get_checkpoint(redis_client)
        print("İlk değer:", first_value)

        await checkpoint.save_checkpoint(
            redis_client=redis_client,
            block_number=100,
        )

        saved_value = await checkpoint.get_checkpoint(redis_client)
        print("Kaydedilen checkpoint:", saved_value)

        try:
            await checkpoint.save_checkpoint(
                redis_client=redis_client,
                block_number=99,
            )
        except ValueError as error:
            print("Geri gitme engellendi:", error)

    finally:
        await redis_client.delete(test_key)
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())