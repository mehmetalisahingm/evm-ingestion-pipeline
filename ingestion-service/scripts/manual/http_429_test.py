import asyncio
from unittest.mock import AsyncMock, patch

from app.rpc.http_client import rpc_call


class FakeResponse:
    def __init__(self, status: int, data: dict):
        self.status = status
        self.headers = {}
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    def raise_for_status(self):
        pass

    async def json(self):
        return self._data


class FakeSession:
    def __init__(self):
        self.request_count = 0

    def post(self, *args, **kwargs):
        self.request_count += 1

        if self.request_count <= 2:
            return FakeResponse(status=429, data={})

        return FakeResponse(
            status=200,
            data={"result": "Başarılı cevap"},
        )


async def main():
    session = FakeSession()

    with patch(
        "app.rpc.http_client.asyncio.sleep",
        new=AsyncMock(),
    ):
        result = await rpc_call(
            session=session,
            method="eth_blockNumber",
            params=[],
        )

    print("Toplam istek:", session.request_count)
    print("Sonuç:", result)


asyncio.run(main())