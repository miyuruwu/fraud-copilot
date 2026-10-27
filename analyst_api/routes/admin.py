"""Quản trị: cấu hình ngưỡng phân luồng (có version) và theo dõi hàng đợi."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, model_validator

from fraud_common.schemas import Role

from ..audit import write_audit
from ..auth import User, require_roles
from ..state import state

router = APIRouter(prefix="/admin", tags=["admin"])
admin_only = require_roles(Role.ADMIN)


class ThresholdUpdate(BaseModel):
    low_max: float
    high_min: float

    @model_validator(mode="after")
    def _check(self):
        if not (0 < self.low_max <= self.high_min < 1):
            raise ValueError("Yêu cầu 0 < low_max <= high_min < 1")
        return self


@router.get("/threshold-config")
async def get_threshold(_: User = Depends(admin_only)):
    row = await state.db.rfetchrow(
        "SELECT version, low_max, high_min, created_by, created_at FROM threshold_configs WHERE active")
    return dict(row) if row else {}


@router.put("/threshold-config")
async def put_threshold(body: ThresholdUpdate, user: User = Depends(admin_only)):
    """Tạo version mới và kích hoạt. Scoring Worker nhận version mới sau tối đa THRESHOLD_CACHE_TTL_S giây."""
    async with state.db.write_tx() as conn:
        await conn.execute("UPDATE threshold_configs SET active=false WHERE active")
        row = await conn.fetchrow(
            """INSERT INTO threshold_configs (low_max, high_min, active, created_by)
               VALUES ($1,$2,true,$3) RETURNING version""", body.low_max, body.high_min, user.username)
        await write_audit(conn, actor=user.username, event="threshold.update", entity="threshold_config",
                          entity_id=row["version"], detail=body.model_dump())
    return {"version": row["version"], **body.model_dump()}


@router.get("/queues")
async def queue_stats(_: User = Depends(admin_only)):
    if state.ml_queue is None:
        raise HTTPException(503, "Chưa sẵn sàng")
    return {
        "ml_jobs": await state.ml_queue.depth(),
        "llm_jobs": await state.llm_queue.depth(),
        "case_events": await state.events_queue.depth(),
    }
