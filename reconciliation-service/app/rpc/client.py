from typing import Any

import aiohttp


class RpcError(Exception):
    pass


class BscRpcClient:
    def __init__(
        self,
        *,
        url: str,
    ) -> None:
        self._url = url
        self._session: aiohttp.ClientSession | None = None
        self._request_id = 0

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()

    async def stop(self) -> None:
        if self._session is not None:
            await self._session.close()

    async def get_latest_block_number(self) -> int:
        result = await self._rpc_call(
            method="eth_blockNumber",
            params=[],
        )

        return int(result, 16)

    async def get_block_by_number(
        self,
        block_number: int,
    ) -> dict[str, Any]:
        result = await self._rpc_call(
            method="eth_getBlockByNumber",
            params=[
                hex(block_number),
                False,
            ],
        )

        if result is None:
            raise RpcError(
                f"Blok bulunamadı: {block_number}"
            )

        return result

    async def get_logs_for_block(
        self,
        block_number: int,
    ) -> list[dict[str, Any]]:
        receipts = await self._rpc_call(
            method="eth_getBlockReceipts",
            params=[
                hex(block_number),
            ],
        )

        if not isinstance(receipts, list):
            raise RpcError(
                "eth_getBlockReceipts sonucu liste değil."
            )

        logs: list[dict[str, Any]] = []

        for receipt in receipts:
            receipt_logs = receipt.get(
                "logs",
                [],
            )

            if not isinstance(receipt_logs, list):
                continue

            logs.extend(receipt_logs)

        return logs

    async def _rpc_call(
        self,
        *,
        method: str,
        params: list[Any],
    ) -> Any:
        if self._session is None:
            raise RpcError(
                "RPC client başlatılmadı."
            )

        self._request_id += 1

        payload = {
            "jsonrpc": "2.0",
            "id": self._request_id,
            "method": method,
            "params": params,
        }

        try:
            async with self._session.post(
                self._url,
                json=payload,
                timeout=aiohttp.ClientTimeout(
                    total=20
                ),
            ) as response:
                data = await response.json()

        except (
            aiohttp.ClientError,
            TimeoutError,
        ) as exc:
            raise RpcError(
                f"RPC bağlantı hatası: {exc}"
            ) from exc

        if response.status != 200:
            raise RpcError(
                "RPC HTTP hatası: "
                f"{response.status}"
            )

        if "error" in data:
            raise RpcError(
                f"RPC hata döndürdü: {data['error']}"
            )

        if "result" not in data:
            raise RpcError(
                "RPC cevabında result alanı yok."
            )

        return data["result"]