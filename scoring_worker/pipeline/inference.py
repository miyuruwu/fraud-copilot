"""Bước 4 - ML inference. >>> HÀM GIẢ <<<  Thay bằng: nạp model + calibrator từ Model Store."""
from __future__ import annotations

import math

from fraud_common.schemas import Explanation, ScoringOutput

MODEL_VERSION = "stub-0.0.1"


def predict(features: dict[str, float]) -> ScoringOutput:
    # Giả lập: giao dịch lớn + xa nhà + đêm khuya -> rủi ro cao. Chỉ để demo đủ 3 mức Low/Medium/High.
    contrib = {
        "amt": min(features["amt"] / 500.0, 3.0),
        "dist_home_merchant_km": min(features["dist_home_merchant_km"] / 300.0, 3.0),
        "txn_count_24h": min(features["txn_count_24h"] / 5.0, 2.0),
        "night_hour": 0.8 if features["hour_of_day"] < 5 else 0.0,
    }
    z = -3.0 + sum(contrib.values())
    raw = z
    prob = 1.0 / (1.0 + math.exp(-z))
    top = sorted(contrib.items(), key=lambda kv: kv[1], reverse=True)[:3]
    return ScoringOutput(
        raw_model_score=raw,
        calibrated_prob=prob,
        model_version=MODEL_VERSION,
        explanations=[Explanation(feature=k, value=features.get(k, features["hour_of_day"]), contribution=v)
                      for k, v in top],
    )
