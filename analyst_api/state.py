"""Tài nguyên dùng chung của tiến trình API, được khởi tạo trong lifespan (main.py)."""
from types import SimpleNamespace

state = SimpleNamespace(
    settings=None,
    redis=None,
    db=None,
    ml_queue=None,       # đẩy job chấm điểm cho Scoring Worker
    llm_queue=None,      # đẩy job tạo brief cho LLM Worker
    events_queue=None,   # nhận sự kiện case_created từ Scoring Worker
)
