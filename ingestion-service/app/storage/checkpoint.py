from redis.asyncio import Redis
from app.settings import CHAIN_ID

CHECKPOINT_KEY = f"evm:{CHAIN_ID}:ingestion:checkpoint"

async def get_checkpoint(
        redis_client:Redis,

)-> int|None:
    value= await redis_client.get(CHECKPOINT_KEY)



    if value is None:
        return None

    return int(value)

async def save_checkpoint(
        redis_client:Redis,
        block_number:int,

)-> None:
    current_checkpoint= await get_checkpoint(redis_client)

    if(
        current_checkpoint is not None and block_number< current_checkpoint
    ):
        raise ValueError("checkpoint eskisinden geride olmaz")

    await redis_client.set(
        CHECKPOINT_KEY,block_number
    )