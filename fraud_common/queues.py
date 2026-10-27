"""Hàng đợi tin cậy trên Redis (at-least-once), dùng chung cho cả 3 hàng đợi.

Cấu trúc Redis cho mỗi queue tên <name>:
  <name>:ready       ZSET  member = JSON của JobEnvelope, score = priority*1e13 + epoch_ms
                           -> job priority nhỏ (High) được lấy trước, cùng priority thì FIFO
  <name>:processing  HASH  job_id -> "<deadline_epoch_s>|<JSON>"  (job đang được xử lý)
  <name>:dead        LIST  job lỗi quá số lần thử (dead-letter)

Luồng: publish -> consume (atomic: ready -> processing) -> ack | nack.
Worker chết giữa chừng: reaper phát hiện quá deadline và đưa job về ready (hoặc dead).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable

from pydantic import ValidationError
from redis.asyncio import Redis

from .metrics import JOBS, QUEUE_DEPTH
from .schemas import JobEnvelope

log = logging.getLogger(__name__)

QUEUE_ML_JOBS = "ml_jobs"            # Analyst API  -> Scoring Worker
QUEUE_CASE_EVENTS = "case_events"    # Scoring Worker -> Analyst API
QUEUE_LLM_JOBS = "llm_jobs"          # Analyst API  -> LLM Worker (có ưu tiên)

PRIORITY_STEP = 10**13

_POP_LUA = """
local r = redis.call('ZPOPMIN', KEYS[1])
if #r == 0 then return false end
local job = cjson.decode(r[1])
redis.call('HSET', KEYS[2], job['job_id'], ARGV[1] .. '|' .. r[1])
return r[1]
"""

DeadHandler = Callable[[JobEnvelope, str], Awaitable[None]]


class ReliableQueue:
    def __init__(
        self,
        redis: Redis,
        name: str,
        *,
        visibility_timeout_s: float = 30.0,
        max_attempts: int = 3,
        on_dead: DeadHandler | None = None,
    ) -> None:
        self.redis = redis
        self.name = name
        self.visibility_timeout_s = visibility_timeout_s
        self.max_attempts = max_attempts
        self.on_dead = on_dead
        self.ready = f"{name}:ready"
        self.processing = f"{name}:processing"
        self.dead = f"{name}:dead"
        self._pop = redis.register_script(_POP_LUA)

    # ------------------------------------------------------------- producer
    async def publish(self, env: JobEnvelope) -> None:
        score = env.priority * PRIORITY_STEP + int(time.time() * 1000)
        await self.redis.zadd(self.ready, {env.model_dump_json(): score})

    # ------------------------------------------------------------- consumer
    async def consume(self) -> JobEnvelope | None:
        """Lấy 1 job (không chặn). Trả None nếu hàng đợi trống."""
        deadline = time.time() + self.visibility_timeout_s
        raw = await self._pop(keys=[self.ready, self.processing], args=[deadline])
        if raw is None:
            return None
        try:
            return JobEnvelope.model_validate_json(raw)
        except ValidationError:
            # message hỏng: không thử lại, đưa thẳng vào dead-letter
            job_id = json.loads(raw).get("job_id", "unknown")
            await self.redis.hdel(self.processing, job_id)
            await self.redis.lpush(self.dead, json.dumps({"raw": raw, "error": "invalid envelope"}))
            JOBS.labels(self.name, "poison").inc()
            return None

    async def ack(self, env: JobEnvelope) -> None:
        await self.redis.hdel(self.processing, env.job_id)

    async def nack(self, env: JobEnvelope, error: str) -> bool:
        """Báo lỗi. Trả True nếu sẽ thử lại, False nếu đã vào dead-letter."""
        if not await self.redis.hdel(self.processing, env.job_id):
            return True  # reaper đã xử lý job này rồi
        return await self._retry_or_dead(env, error)

    async def _retry_or_dead(self, env: JobEnvelope, error: str) -> bool:
        env = env.model_copy(update={"attempts": env.attempts + 1})
        if env.attempts >= self.max_attempts:
            await self.redis.lpush(self.dead, json.dumps({"job": env.model_dump(mode="json"), "error": error}))
            JOBS.labels(self.name, "dead").inc()
            log.error("job vào dead-letter", extra={"ctx": {
                "queue": self.name, "job_id": env.job_id, "trace_id": env.trace_id, "error": error}})
            if self.on_dead:
                try:
                    await self.on_dead(env, error)
                except Exception:
                    log.exception("on_dead handler lỗi")
            return False
        await asyncio.sleep(min(0.2 * 2 ** env.attempts, 3.0))   # backoff ngắn
        await self.publish(env)
        JOBS.labels(self.name, "retry").inc()
        return True

    # ------------------------------------------------------ vận hành / giám sát
    async def reap_expired(self) -> int:
        """Đưa các job quá deadline (worker chết/treo) về lại hàng đợi hoặc dead-letter."""
        now, n = time.time(), 0
        for job_id, val in (await self.redis.hgetall(self.processing)).items():
            deadline_s, raw = val.split("|", 1)
            if float(deadline_s) < now and await self.redis.hdel(self.processing, job_id):
                await self._retry_or_dead(JobEnvelope.model_validate_json(raw), "visibility timeout expired")
                n += 1
        return n

    async def depth(self) -> dict[str, int]:
        return {
            "ready": await self.redis.zcard(self.ready),
            "processing": await self.redis.hlen(self.processing),
            "dead": await self.redis.llen(self.dead),
        }

    async def update_gauges(self) -> None:
        for state, value in (await self.depth()).items():
            QUEUE_DEPTH.labels(self.name, state).set(value)

    async def reaper_loop(self, stop: asyncio.Event, interval_s: float = 5.0) -> None:
        while not stop.is_set():
            try:
                await self.reap_expired()
                await self.update_gauges()
            except Exception:
                log.exception("reaper lỗi")
            await _sleep_or_stop(stop, interval_s)

    async def gauge_loop(self, stop: asyncio.Event, interval_s: float = 5.0) -> None:
        while not stop.is_set():
            try:
                await self.update_gauges()
            except Exception:
                log.exception("gauge lỗi")
            await _sleep_or_stop(stop, interval_s)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
