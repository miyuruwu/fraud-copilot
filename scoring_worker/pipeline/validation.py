"""Bước 1 - Data Validation (FR-01). Không hợp lệ -> MANUAL_FALLBACK_UNSCORED, TUYỆT ĐỐI không gán Low."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from fraud_common.schemas import TransactionValidated


@dataclass
class ValidationResult:
    ok: bool
    tx: TransactionValidated | None = None
    errors: list[dict[str, str]] = field(default_factory=list)


def validate(raw: dict[str, Any]) -> ValidationResult:
    try:
        return ValidationResult(ok=True, tx=TransactionValidated.model_validate(raw))
    except ValidationError as exc:
        errors = [{"field": ".".join(str(p) for p in e["loc"]), "msg": e["msg"]} for e in exc.errors()]
        return ValidationResult(ok=False, errors=errors)
