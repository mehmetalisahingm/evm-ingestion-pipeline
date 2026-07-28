# import asyncio
# import aiohttp


# RPC_URL = "https://bsc-dataseed.bnbchain.org"


# async def rpc_call(
#     session: aiohttp.ClientSession,
#     method: str,
#     params: list,
# ):
#     payload = {
#         "jsonrpc": "2.0",
#         "method":method,
#         "params":params,
#         "id":1,

#     }

#     async with session.post(RPC_URL,json=payload) as response:
#         response.raise_for_status()

#         data=await response.json()

#         if "error" in data:
#             raise RuntimeError(data["error"])
#         return data["result"]

# async def main():
#     timeout = aiohttp.ClientTimeout(total=10)

#     async with aiohttp.ClientSession(timeout=timeout) as session:
#         chain_id_hex = await rpc_call(session, "eth_chainId", [])
#         block_number_hex = await rpc_call(session, "eth_blockNumber", [])

#         block = await rpc_call(
#             session,
#             "eth_getBlockByNumber",
#             [block_number_hex, False],
#         )

#         chain_id = int(chain_id_hex, 16)
#         block_number = int(block_number_hex, 16)

#         print(f"Chain ID: {chain_id}")
#         print(f"Son blok numarası: {block_number}")
#         print(f"Blok hash: {block['hash']}")
#         print(f"Parent hash: {block['parentHash']}")
#         print(f"Transaction sayısı: {len(block['transactions'])}")

# if __name__ == "__main__":
#     asyncio.run(main())



import asyncio

import aiohttp

from app.rpc.http_client import rpc_call


async def main():
    timeout = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        chain_id_hex = await rpc_call(session, "eth_chainId", [])
        block_number_hex = await rpc_call(session, "eth_blockNumber", [])

        block = await rpc_call(
            session,
            "eth_getBlockByNumber",
            [block_number_hex, False],
        )

        print(f"Chain ID: {int(chain_id_hex, 16)}")
        print(f"Son blok numarası: {int(block_number_hex, 16)}")
        print(f"Blok hash: {block['hash']}")
        print(f"Parent hash: {block['parentHash']}")
        print(f"Transaction sayısı: {len(block['transactions'])}")


if __name__ == "__main__":
    asyncio.run(main())