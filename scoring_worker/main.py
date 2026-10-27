from __future__ import annotations

import asyncio
import logging

from prometheus_client import start_http_server
from redis.asyncio import Redis

from fraud_common.config import get_settings
from fraud_common.db import Database
from fraud_common.logging_utils import setup_logging
from fraud_common.queues import QUEUE_CASE_EVENTS, QUEUE_ML_JOBS, ReliableQueue
from fraud_common.worker import install_signal_handlers, run_consumer

from .processor import ScoringProcessor

log = logging.getLogger(__name__)


async def main() -> None:
    s = get_settings()
    setup_logging("scoring-worker")
    start_http_server(s.metrics_port)

    redis = Redis.from_url(s.redis_url, decode_responses=True)
    db = Database(s.db_write_url, s.db_read_url)
    await db.connect()

    events_queue = ReliableQueue(redis, QUEUE_CASE_EVENTS)
    processor = ScoringProcessor(db, events_queue)
    ml_queue = ReliableQueue(
        redis, QUEUE_ML_JOBS, visibility_timeout_s=s.ml_visibility_timeout_s,
        max_attempts=s.ml_max_attempts, on_dead=processor.handle_dead)

    stop = asyncio.Event()
    install_signal_handlers(stop)
    log.info("scoring-worker sẵn sàng")
    await asyncio.gather(
        run_consumer(ml_queue, processor.handle, concurrency=s.ml_concurrency, stop=stop),
        ml_queue.reaper_loop(stop),
    )
    await db.close()
    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
