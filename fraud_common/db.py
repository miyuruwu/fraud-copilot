"""Kết nối PostgreSQL qua PgBouncer: pool ghi (Primary) và pool đọc (Replica)."""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import asyncpg

log = logging.getLogger(__name__)


async def _init_conn(conn: asyncpg.Connection) -> None:
    # jsonb <-> dict/list tự động
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


class Database:
    def __init__(self, write_url: str, read_url: str) -> None:
        self.write_url = write_url
        self.read_url = read_url
        self.write: asyncpg.Pool | None = None
        self.read: asyncpg.Pool | None = None

    async def connect(self, retries: int = 30, delay_s: float = 1.0) -> None:
        # statement_cache_size=0 là bắt buộc khi đi qua PgBouncer (transaction pooling)
        for attempt in range(1, retries + 1):
            try:
                self.write = await asyncpg.create_pool(
                    self.write_url, min_size=1, max_size=10, statement_cache_size=0, init=_init_conn)
                self.read = await asyncpg.create_pool(
                    self.read_url, min_size=1, max_size=10, statement_cache_size=0, init=_init_conn)
                return
            except (OSError, asyncpg.PostgresError) as exc:
                log.warning("DB chưa sẵn sàng (lần %d/%d): %s", attempt, retries, exc)
                await asyncio.sleep(delay_s)
        raise RuntimeError("Không kết nối được PostgreSQL")

    async def close(self) -> None:
        for pool in (self.write, self.read):
            if pool:
                await pool.close()

    # --- ghi (Primary)
    async def wexec(self, sql: str, *args: Any) -> str:
        return await self.write.execute(sql, *args)

    async def wfetchrow(self, sql: str, *args: Any):
        return await self.write.fetchrow(sql, *args)

    @asynccontextmanager
    async def write_tx(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.write.acquire() as conn:
            async with conn.transaction():
                yield conn

    # --- đọc (Replica)
    async def rfetch(self, sql: str, *args: Any):
        return await self.read.fetch(sql, *args)

    async def rfetchrow(self, sql: str, *args: Any):
        return await self.read.fetchrow(sql, *args)

    async def rfetchval(self, sql: str, *args: Any):
        return await self.read.fetchval(sql, *args)
