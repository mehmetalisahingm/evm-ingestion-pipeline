# import asyncio
# import json
# import os 
# from pathlib import Path


# from dotenv import load_dotenv
# from websockets.asyncio.client import connect

# ENV_PATH= Path(__file__).resolve().parent.parent / ".env"
# load_dotenv(ENV_PATH)

# WS_RPC_URL = os.getenv("WS_RPC_URL")


# async def main():
#     if not WS_RPC_URL:
#         raise RuntimeError("ws_rpc_url .env dosyası bulunamadı")
    
#     async with connect (WS_RPC_URL) as websocket:
#         request = {
#             "jsonrpc" :"2.0",
#             "id": 1,
#             "method":"eth_subscribe",
#             "params" : ["newHeads"],

#         }


#         await websocket.send(json.dumps(request))

#         subscription_response = json.loads(await websocket.recv())
#         print("abonelik oluşturuldu",subscription_response)


#         while True:
#             message = json.loads(await websocket.recv())
#             block = message["params"]["result"]

#             block_number = int (block["number"],16)


#             print("\n Yeni blok geldi")
#             print(f"blok numarası : {block_number}")
#             print(f"blok hash{block["hash"]}")
#             print(f"parent hash:{block["parentHash"]}")

# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         print("\n web socket durduruldu")



import asyncio

from app.rpc.websocket_client import stream_new_blocks


async def main():
    async for block in stream_new_blocks():
        block_number = int(block["number"], 16)

        print("\nYeni blok geldi")
        print(f"Blok numarası: {block_number}")
        print(f"Blok hash: {block['hash']}")
        print(f"Parent hash: {block['parentHash']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nWebSocket testi durduruldu.")