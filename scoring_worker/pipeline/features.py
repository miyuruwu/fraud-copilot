"""Bước 3 - Temporal feature engineering. Chỉ dùng lịch sử với t_history < t_current (chống leakage)."""
from __future__ import annotations

import math

from fraud_common.db import Database
from fraud_common.schemas import TransactionValidated


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(a))


async def compute_features(db: Database, tx: TransactionValidated, account_hash_id: str) -> dict[str, float]:
    # Đọc từ Replica qua PgBouncer-read. Điều kiện txn_time < thời điểm hiện tại là bắt buộc.
    hist = await db.rfetchrow(
        """SELECT count(*) AS n, COALESCE(avg((payload_masked->>'amt')::float), 0) AS avg_amt
           FROM transactions
           WHERE account_hash_id = $1 AND txn_time < $2 AND txn_time >= $2 - interval '24 hours'""",
        account_hash_id, tx.trans_date_trans_time)
    n, avg_amt = float(hist["n"]), float(hist["avg_amt"])
    return {
        "amt": tx.amt,
        "dist_home_merchant_km": haversine_km(tx.lat, tx.long, tx.merch_lat, tx.merch_long),
        "txn_count_24h": n,
        "avg_amt_24h": avg_amt,
        "amt_vs_avg_ratio": tx.amt / avg_amt if avg_amt > 0 else 1.0,
        "hour_of_day": float(tx.trans_date_trans_time.hour),
    }
    # TODO: thêm các đặc trưng thật (tốc độ di chuyển, số merchant mới, ...) khi có mô hình.
