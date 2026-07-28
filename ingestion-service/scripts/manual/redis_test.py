import asyncio 
from app.storage.redis_client import create_redis_client

async def main()-> None:
    redis_client=create_redis_client()

    try:
        result =await redis_client.ping()
        print("redis bağşlantısı",result)


    finally:
        await redis_client.aclose()


if __name__=="__main__":
    asyncio.run(main())