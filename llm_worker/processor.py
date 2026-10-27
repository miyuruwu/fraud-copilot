"""Luồng 2 (Cold Path): lấy job theo ưu tiên -> RAG -> LLM -> grounding -> ghi brief vào DB."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from fraud_common.config import get_settings
from fraud_common.db import Database
from fraud_common.metrics import BREAKER_STATE, BRIEFS, STAGE_SECONDS
from fraud_common.resilience import CircuitBreaker
from fraud_common.schemas import BriefStatus, GenerateBriefPayload, JobEnvelope
from fraud_common.worker import NonRetryableError

from .fallback import rule_based_brief
from .grounding import check_grounding
from .llm_client import PROMPT_VERSION, get_llm_client
from .rag import get_retriever

log = logging.getLogger(__name__)


class CaseNotReady(Exception):
    """Case chưa thấy trên Replica (trễ đồng bộ) -> thử lại sau."""


class BriefProcessor:
    def __init__(self, db: Database) -> None:
        s = get_settings()
        self.s = s
        self.db = db
        self.retriever = get_retriever()
        self.llm = get_llm_client()
        self.breaker = CircuitBreaker(s.breaker_failure_threshold, s.breaker_reset_s)

    async def handle(self, env: JobEnvelope) -> None:
        p = GenerateBriefPayload.model_validate(env.payload)
        await self._set_status(p.case_id, BriefStatus.RUNNING, attempts=env.attempts + 1)

        ctx = await self._load_context(p.case_id)
        signals = [e["feature"] for e in ctx["explanations"]]

        with STAGE_SECONDS.labels("rag").time():
            chunks = await self.retriever.retrieve(signals, ctx["risk_level"])

        # Timeout + circuit breaker bao quanh lời gọi LLM
        async def _call_llm():
            return await asyncio.wait_for(self.llm.generate(ctx, chunks), self.s.llm_job_timeout_s)

        with STAGE_SECONDS.labels("llm").time():
            try:
                result = await self.breaker.call(_call_llm)
            finally:
                BREAKER_STATE.set(1 if self.breaker.is_open else 0)

        if result.tokens_used > self.s.brief_token_budget:
            await self._set_status(p.case_id, BriefStatus.FAILED, attempts=env.attempts + 1)
            BRIEFS.labels(BriefStatus.FAILED.value).inc()
            raise NonRetryableError(f"vượt ngân sách token: {result.tokens_used}")
        check_grounding(result.brief, chunks)       # GroundingError -> job lỗi -> retry

        await self.db.wexec(
            """INSERT INTO case_briefs (case_id, status, brief, citations, attempts, model_name,
                                        prompt_version, tokens_used, last_error, updated_at)
               VALUES ($1,'READY',$2,$3,$4,$5,$6,$7,NULL,now())
               ON CONFLICT (case_id) DO UPDATE SET status='READY', brief=EXCLUDED.brief,
                 citations=EXCLUDED.citations, attempts=EXCLUDED.attempts, model_name=EXCLUDED.model_name,
                 prompt_version=EXCLUDED.prompt_version, tokens_used=EXCLUDED.tokens_used,
                 last_error=NULL, updated_at=now()""",
            p.case_id, result.brief.model_dump(), [c.model_dump() for c in result.brief.citations],
            env.attempts + 1, result.model_name, PROMPT_VERSION, result.tokens_used)
        BRIEFS.labels("READY").inc()
        log.info("brief READY", extra={"ctx": {"case_id": p.case_id, "trace_id": env.trace_id,
                                               "tokens": result.tokens_used}})

    async def handle_dead(self, env: JobEnvelope, error: str) -> None:
        """Hết số lần thử: đánh dấu FAILED_TIMEOUT, ghi brief rule-based. Case + kết quả ML giữ nguyên (FR-07)."""
        p = GenerateBriefPayload.model_validate(env.payload)
        status = BriefStatus.FAILED_TIMEOUT if ("Timeout" in error or "CircuitOpen" in error) else BriefStatus.FAILED
        try:
            ctx = await self._load_context(p.case_id)
            fb = rule_based_brief(ctx).model_dump()
        except Exception:
            fb = None
        await self.db.wexec(
            """INSERT INTO case_briefs (case_id, status, brief, attempts, last_error, updated_at)
               VALUES ($1,$2,$3,$4,$5,now())
               ON CONFLICT (case_id) DO UPDATE SET status=EXCLUDED.status, brief=EXCLUDED.brief,
                 attempts=EXCLUDED.attempts, last_error=EXCLUDED.last_error, updated_at=now()""",
            p.case_id, status.value, fb, env.attempts, error[:500])
        BRIEFS.labels(status.value).inc()

    # ------------------------------------------------------------------ helpers
    async def _set_status(self, case_id: int, status: BriefStatus, attempts: int) -> None:
        await self.db.wexec(
            """INSERT INTO case_briefs (case_id, status, attempts) VALUES ($1,$2,$3)
               ON CONFLICT (case_id) DO UPDATE SET status=EXCLUDED.status, attempts=EXCLUDED.attempts,
                 updated_at=now()""", case_id, status.value, attempts)

    async def _load_context(self, case_id: int) -> dict[str, Any]:
        """Chỉ lấy dữ liệu ĐÃ che PII (payload_masked) để đưa vào prompt (FR-09)."""
        row = await self.db.rfetchrow(
            """SELECT c.case_id, c.txn_id, c.risk_level, s.calibrated_prob, s.raw_model_score,
                      s.explanations, s.features, t.payload_masked AS transaction
               FROM cases c
               JOIN scoring_results s ON s.txn_id = c.txn_id
               JOIN transactions t ON t.txn_id = c.txn_id
               WHERE c.case_id = $1""", case_id)
        if row is None:
            raise CaseNotReady(f"case {case_id} chưa có trên replica")
        return dict(row)
