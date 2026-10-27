"""Bước 2 - PII masking: băm định danh, che số thẻ, loại bỏ trường nhạy cảm (FR-09)."""
from __future__ import annotations

import hashlib
from typing import Any

from fraud_common.config import get_settings

DROP_KEYS = {"first", "last", "street", "dob", "is_fraud"}


def hash_account(cc_num: str) -> str:
    return hashlib.sha256((get_settings().pii_salt + cc_num).encode()).hexdigest()[:32]


def mask_pii(raw: dict[str, Any]) -> dict[str, Any]:
    """Chạy được cả trên dữ liệu không hợp lệ (best-effort). Kết quả an toàn để lưu DB / gửi LLM / ghi log."""
    out = {k: v for k, v in raw.items() if k not in DROP_KEYS}
    cc = out.pop("cc_num", None)
    if cc is not None:
        s = str(cc)
        out["masked_cc_num"] = "*" * max(len(s) - 4, 0) + s[-4:]
        out["account_hash_id"] = hash_account(s)
    return out
