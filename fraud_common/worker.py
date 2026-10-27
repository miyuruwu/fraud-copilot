"""Vòng lặp consumer dùng chung cho Scoring Worker, LLM Worker và consumer sự kiện trong API."""
from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Awaitable, Callable

from .metrics import JOB_SECONDS, JOBS
from .queues import ReliableQueue
from .schemas import JobEnvelope

log = logging.getLogger(__name__)

Handler = Callable[[JobEnvelope], Awaitable[None]]


class NonRetryableError(Exception):
    """Lỗi do dữ liệu job, thử lại cũng vô ích -> ack và bỏ qua (đã được ghi nhận bởi handler)."""


async def run_consumer(
    queue: ReliableQueue,
    handler: Handler,
    *,
    concurrency: int,
    stop: asyncio.Event,
    poll_interval_s: float = 0.05,
) -> None:
    sem = asyncio.Semaphore(concurrency)
    running: set[asyncio.Task] = set()

    async def process(env: JobEnvelope) -> None:
        t0 = time.perf_counter()
        try:
            await handler(env)
            await queue.ack(env)
            JOBS.labels(queue.name, "ok").inc()
        except NonRetryableError as exc:
            await queue.ack(env)
            JOBS.labels(queue.name, "non_retryable").inc()
            log.error("job bỏ qua: %s", exc, extra={"ctx": {"queue": queue.name, "trace_id": env.trace_id}})
        except Exception as exc:  # kể cả asyncio.TimeoutError
            JOBS.labels(queue.name, "error").inc()
            log.warning("job lỗi: %r", exc, extra={"ctx": {
                "queue": queue.name, "job_id": env.job_id, "trace_id": env.trace_id, "attempt": env.attempts}})
            await queue.nack(env, repr(exc) or type(exc).__name__)
        finally:
            JOB_SECONDS.labels(queue.name).observe(time.perf_counter() - t0)
            sem.release()

    while not stop.is_set():
        await sem.acquire()
        try:
            env = await queue.consume()
        except Exception:
            sem.release()
            log.exception("consume lỗi (Redis?)")
            await asyncio.sleep(1)
            continue
        if env is None:
            sem.release()
            await asyncio.sleep(poll_interval_s)
            continue
        task = asyncio.create_task(process(env))
        running.add(task)
        task.add_done_callback(running.discard)

    if running:  # graceful shutdown: chờ job đang chạy
        await asyncio.wait(running, timeout=10)


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:   # Windows: dùng KeyboardInterrupt thay thế
            pass
