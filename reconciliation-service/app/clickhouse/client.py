from typing import Any

import aiohttp


class ClickHouseReadError(Exception):
    pass


class ClickHouseReadClient:
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

    async def get_latest_block_number(
        self,
        *,
        chain_id: int,
        max_block_number: int,
    ) -> int:
        query = f"""
        SELECT max(block_number)
        FROM {self._database}.blocks FINAL
        WHERE chain_id = {chain_id}
          AND canonical = 1
          AND block_number <= {max_block_number}
        """

        result = await self._execute_scalar(query)

        return int(result)

    async def get_block_count(
        self,
        *,
        chain_id: int,
        start_block: int,
        end_block: int,
    ) -> int:
        query = f"""
        SELECT count()
        FROM {self._database}.blocks FINAL
        WHERE chain_id = {chain_id}
          AND canonical = 1
          AND block_number BETWEEN {start_block} AND {end_block}
        """

        result = await self._execute_scalar(query)

        return int(result)

    async def get_log_count(
        self,
        *,
        chain_id: int,
        start_block: int,
        end_block: int,
    ) -> int:
        query = f"""
        SELECT count()
        FROM {self._database}.logs FINAL
        WHERE chain_id = {chain_id}
          AND canonical = 1
          AND removed = 0
          AND block_number BETWEEN {start_block} AND {end_block}
        """

        result = await self._execute_scalar(query)

        return int(result)

    async def get_block_numbers(
        self,
        *,
        chain_id: int,
        start_block: int,
        end_block: int,
    ) -> set[int]:
        query = f"""
        SELECT block_number
        FROM {self._database}.blocks FINAL
        WHERE chain_id = {chain_id}
          AND canonical = 1
          AND block_number BETWEEN {start_block} AND {end_block}
        FORMAT JSON
        """

        data = await self._execute_json(query)

        return {
            int(row["block_number"])
            for row in data.get("data", [])
        }

    async def _execute_scalar(
        self,
        query: str,
    ) -> str:
        text = await self._execute_text(query)

        return text.strip()

    async def _execute_json(
        self,
        query: str,
    ) -> dict[str, Any]:
        if self._session is None:
            raise ClickHouseReadError(
                "ClickHouse client başlatılmadı."
            )

        try:
            async with self._session.post(
                self._url,
                params={
                    "query": query,
                    "user": self._user,
                    "password": self._password,
                },
            ) as response:
                text = await response.text()

                if response.status != 200:
                    raise ClickHouseReadError(
                        "ClickHouse sorgusu başarısız. "
                        f"HTTP {response.status}: {text}"
                    )

                return await response.json()

        except aiohttp.ClientError as exc:
            raise ClickHouseReadError(
                f"ClickHouse bağlantı hatası: {exc}"
            ) from exc

    async def _execute_text(
        self,
        query: str,
    ) -> str:
        if self._session is None:
            raise ClickHouseReadError(
                "ClickHouse client başlatılmadı."
            )

        try:
            async with self._session.post(
                self._url,
                params={
                    "query": query,
                    "user": self._user,
                    "password": self._password,
                },
            ) as response:
                text = await response.text()

                if response.status != 200:
                    raise ClickHouseReadError(
                        "ClickHouse sorgusu başarısız. "
                        f"HTTP {response.status}: {text}"
                    )

                return text

        except aiohttp.ClientError as exc:
            raise ClickHouseReadError(
                f"ClickHouse bağlantı hatası: {exc}"
            ) from exc