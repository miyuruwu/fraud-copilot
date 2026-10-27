"""Grounding check (FR-05): brief chỉ được trích dẫn nguồn đã được RAG truy xuất. >>> bản tối thiểu <<<"""
from __future__ import annotations

from fraud_common.schemas import BriefContent

from .rag import Chunk


class GroundingError(Exception):
    pass


def check_grounding(brief: BriefContent, chunks: list[Chunk]) -> None:
    allowed = {(c.section_id, c.rule_code) for c in chunks}
    for cit in brief.citations:
        if (cit.section_id, cit.rule_code) not in allowed:
            raise GroundingError(f"citation ngoài ngữ cảnh RAG: {cit.section_id}/{cit.rule_code}")
    for item in brief.checklist:
        if item.rule_code and item.rule_code not in {c.rule_code for c in chunks}:
            raise GroundingError(f"checklist tham chiếu rule không có trong ngữ cảnh: {item.rule_code}")
    if not brief.citations:
        raise GroundingError("brief không có trích dẫn nguồn")
    # TODO: kiểm tra số liệu trong summary khớp dữ liệu giao dịch.
