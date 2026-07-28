from redis.asyncio import Redis
from app.settings import REDIS_URL

def create_redis_client()-> Redis:
    if not REDIS_URL:
        raise RuntimeError(
            "redis url env dosyasında yok"
        )

    return Redis.from_url(
        REDIS_URL,
        decode_responses=True,
    )