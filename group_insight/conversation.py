"""消息处理通用工具与兼容层。

本模块保留跨领域复用的消息分类、JSON 工具及基础文本处理。
原先的数据库拉取、富媒体解析、分片、统计等逻辑已拆分到：
- fetching.py
- rich_content.py
- chunking.py
- stats.py
- cache_utils.py
- common.py（最底层通用工具）
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .common import *  # noqa: F401,F403  重新导出常用基础工具
from .models import StructuredMessage


def is_substantive_message(message: StructuredMessage) -> bool:
    """判断消息是否属于有内容价值的对话消息。"""
    if message.sender == "unknown":
        return False
    if message.msg_type in {"系统"}:
        return False
    return True


def classify_message_category(message: StructuredMessage) -> str:
    """把结构化消息归入文本、回复、系统、红包等分析类别。"""
    from .rich_content import has_meaningful_rich_content

    text = message.text or ""
    metadata = message.metadata or {}
    rich_kind = metadata.get("rich_kind", "")
    if message.msg_type == "系统":
        return "system"
    if message.msg_type == "语音":
        return "voice"
    if message.msg_type == "视频":
        return "video"
    if message.msg_type == "表情":
        return "emoji"
    if message.msg_type == "图片":
        return "image"
    if message.msg_type == "文本":
        return "text"
    if message.msg_type == "链接/文件":
        if "拍了拍" in text:
            return "pat"
        if "微信红包" in text:
            return "redpacket"
        if rich_kind == "merged_chat":
            return "merged_chat"
        if rich_kind == "link_card":
            return "link_card"
        if "聊天记录" in text:
            return "merged_chat"
        if text.strip() in {"[链接/文件]", "[链接]"}:
            return "bare_link_file"
        if "↳ 回复 " in text:
            return "reply"
        if text.startswith("[链接/文件]") or text.startswith("[链接]"):
            return "link_card"
    return "other"


def get_message_category_labels() -> dict[str, str]:
    """返回内部消息类别到中文标签的映射。"""
    return {
        "text": "文本",
        "reply": "回复",
        "emoji": "表情",
        "image": "图片",
        "voice": "语音",
        "video": "视频",
        "pat": "拍一拍",
        "system": "系统消息",
        "redpacket": "红包",
        "merged_chat": "合并聊天记录",
        "bare_link_file": "占位链接/文件",
        "link_card": "链接卡片",
        "other": "其他",
    }


def is_effective_conversation_message(message: StructuredMessage) -> bool:
    """判断消息是否进入本地统计和模型分析口径。"""
    if message.sender == "unknown":
        return False
    category = classify_message_category(message)
    if category in {"link_card", "merged_chat"}:
        from .rich_content import has_meaningful_rich_content
        return has_meaningful_rich_content(message)
    return category in {"text", "reply", "emoji", "image", "voice", "video"}


def is_analysis_message(message: StructuredMessage) -> bool:
    """返回消息是否进入 LLM 分析输入。"""
    return is_effective_conversation_message(message)


def serialize_messages(messages: list[StructuredMessage]) -> list[dict[str, Any]]:
    """把结构化消息转换为可写入 JSON 的字典列表。"""
    return [asdict(message) for message in messages]


def compact_prompt_stats(stats: dict[str, Any]) -> dict[str, Any]:
    """压缩统计字段，减少 prompt 中的非必要体积。"""
    return {
        "message_count": stats.get("message_count", 0),
        "effective_message_count": stats.get("effective_message_count", 0),
        "excluded_message_count": stats.get("excluded_message_count", 0),
        "participant_count": stats.get("participant_count", 0),
        "raw_char_count": stats.get("raw_char_count", 0),
        "effective_char_count": stats.get("effective_char_count", 0),
        "first_message_time": stats.get("first_message_time", ""),
        "last_message_time": stats.get("last_message_time", ""),
        "top_speakers": stats.get("top_speakers", [])[:10],
        "interaction_rankings": stats.get("interaction_rankings", {}),
        "effective_breakdown": stats.get("effective_breakdown", []),
        "time_segment_breakdown": stats.get("time_segment_breakdown", []),
        "word_cloud": stats.get("word_cloud", [])[:20],
    }
