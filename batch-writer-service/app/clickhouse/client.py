import json
from typing import Any

import aiohttp


ALLOWED_TABLES = {
    "blocks",
    "transactions",
    "logs",
}


class ClickHouseWriteError(Exception):
    pass


class ClickHouseClient:
    def __init__(
        self,
        *,
        url: str,
        database: str,
        user: str,
        password: str,
    ) -> None:
        self._url = url.rstrip("/")
        self._database = database
        self._user = user
        self._password = password

        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()

    async def stop(self) -> None:
        if self._session is not None:
            await self._session.close()

    async def insert_rows(
        self,
        *,
        table: str,
        rows: list[dict[str, Any]],
    ) -> None:
        if not rows:
            return

        if table not in ALLOWED_TABLES:
            raise ClickHouseWriteError(
                f"Geçersiz ClickHouse tablosu: {table}"
            )

        if self._session is None:
            raise ClickHouseWriteError(
                "ClickHouse client başlatılmadı."
            )

        query = (
            f"INSERT INTO {self._database}.{table} "
            "FORMAT JSONEachRow"
        )

        body = "\n".join(
            json.dumps(
                row,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for row in rows
        )

        try:
            async with self._session.post(
                self._url,
                params={
                    "query": query,
                    "user": self._user,
                    "password": self._password,
                },
                data=body.encode("utf-8"),
                headers={
                    "Content-Type": "application/x-ndjson",
                },
            ) as response:
                response_text = await response.text()

                if response.status != 200:
                    raise ClickHouseWriteError(
                        "ClickHouse insert başarısız. "
                        f"HTTP {response.status}: "
                        f"{response_text}"
                    )

        except aiohttp.ClientError as exc:
            raise ClickHouseWriteError(
                f"ClickHouse bağlantı hatası: {exc}"
            ) from exc