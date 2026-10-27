"""Test các hàm thuần (không cần Redis/Postgres): pytest tests -q"""
import asyncio

import pytest

from fraud_common.resilience import CircuitBreaker, CircuitOpenError
from fraud_common.schemas import RiskLevel, ThresholdConfig
from scoring_worker.pipeline.pii import mask_pii
from scoring_worker.pipeline.routing import route
from scoring_worker.pipeline.validation import validate

GOOD = {
    "trans_num": "t1", "trans_date_trans_time": "2024-01-01 10:00:00", "cc_num": "4111111111111111",
    "first": "A", "last": "B", "street": "x", "merchant": "m", "category": "c", "amt": 12.5,
    "lat": 10.0, "long": 20.0, "merch_lat": 10.1, "merch_long": 20.1, "is_fraud": 1,
}
CFG = ThresholdConfig(version=1, low_max=0.3, high_min=0.7)


def test_routing_thresholds():
    assert route(0.10, CFG) == RiskLevel.LOW
    assert route(0.30, CFG) == RiskLevel.MEDIUM
    assert route(0.69, CFG) == RiskLevel.MEDIUM
    assert route(0.70, CFG) == RiskLevel.HIGH


def test_validation_ok():
    assert validate(GOOD).ok


@pytest.mark.parametrize("patch", [
    {"amt": -5}, {"amt": "abc"}, {"lat": 999}, {"merchant": None},
])
def test_validation_rejects_bad_data(patch):
    r = validate({**GOOD, **patch})
    assert not r.ok and r.errors


def test_validation_missing_field():
    bad = {k: v for k, v in GOOD.items() if k != "cc_num"}
    assert not validate(bad).ok


def test_pii_masking():
    out = mask_pii(GOOD)
    assert "cc_num" not in out and "first" not in out and "is_fraud" not in out
    assert out["masked_cc_num"].endswith("1111") and out["masked_cc_num"].startswith("*")
    assert len(out["account_hash_id"]) == 32


def test_circuit_breaker_opens_then_blocks():
    async def boom():
        raise RuntimeError("x")

    async def run():
        cb = CircuitBreaker(failure_threshold=2, reset_after_s=60)
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await cb.call(boom)
        with pytest.raises(CircuitOpenError):
            await cb.call(boom)

    asyncio.run(run())
