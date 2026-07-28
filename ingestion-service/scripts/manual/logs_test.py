import asyncio

from app.rpc.websocket_client import stream_logs


async def main() -> None:
    try:
        async for log in stream_logs():
            print("\nYeni log geldi")
            print("Blok numarası:", int(log["blockNumber"], 16))
            print("Transaction hash:", log["transactionHash"])
            print("Kontrat adresi:", log["address"])

    except KeyboardInterrupt:
        print("\nLog testi durduruldu.")


if __name__ == "__main__":
    asyncio.run(main())