from __future__ import annotations

from typing import Any

import asyncpg


async def write_audit(
    conn: asyncpg.Connection, *, actor: str, event: str, entity: str,
    entity_id: str | int, detail: dict[str, Any] | None = None, trace_id: str | None = None,
) -> None:
    await conn.execute(
        "INSERT INTO audit_log (actor, event, entity, entity_id, detail, trace_id) VALUES ($1,$2,$3,$4,$5,$6)",
        actor, event, entity, str(entity_id), detail or {}, trace_id)
