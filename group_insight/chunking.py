"""消息分片与载荷构造。

把结构化消息按数量、字符数、时间跨度和话题连续性切成分析片段，
并生成 map/reduce/final 各阶段需要的 prompt 载荷。
"""

from __future__ import annotations

import json
import math
from collections import Counter
from typing import Any

from .common import extract_topic_tokens, format_ts, make_user_placeholder, topic_similarity
from .conversation import is_analysis_message
from .models import MessageChunk, StructuredMessage
from .settings import (
    DEFAULT_CHUNK_MAX_CHARS,
    DEFAULT_CHUNK_MAX_MESSAGES,
    DEFAULT_CHUNK_MAX_MINUTES,
    DEFAULT_HARD_GAP_MINUTES,
    DEFAULT_SOFT_GAP_MINUTES,
    DEFAULT_TOPIC_MIN_CHUNK_MESSAGES,
    DEFAULT_TOPIC_SIM_THRESHOLD,
)


def build_chunk(index: int, messages: list[StructuredMessage]) -> MessageChunk:
    """把一组消息封装成带范围和计数的 MessageChunk。"""
    if not messages:
        raise ValueError("messages must not be empty")
    start_ts = messages[0].timestamp
    end_ts = messages[-1].timestamp
    char_count = sum(len(message.sender) + len(message.text) + 16 for message in messages)
    return MessageChunk(
        id=f"shard-{index:03d}",
        index=index,
        start_ts=start_ts,
        end_ts=end_ts,
        start_time=format_ts(start_ts),
        end_time=format_ts(end_ts),
        message_count=len(messages),
        char_count=char_count,
        messages=messages[:],
    )


def build_chunks(
    messages: list[StructuredMessage],
    max_messages: int,
    max_chars: int,
    max_minutes: int,
    hard_gap_minutes: int = 90,
    soft_gap_minutes: int = 18,
    low_similarity_threshold: float = 0.08,
    min_chunk_messages: int = 24,
) -> list[MessageChunk]:
    """按消息数、字符数、时间跨度和话题变化切分分析片段。"""
    if not messages:
        return []

    chunks: list[MessageChunk] = []
    current: list[StructuredMessage] = []
    current_chars = 0
    chunk_index = 1
    chunk_start_ts = messages[0].timestamp
    chunk_topic_tokens: set[str] = set()
    low_similarity_streak = 0
    prev_message: StructuredMessage | None = None

    for message in messages:
        next_chars = current_chars + len(message.sender) + len(message.text) + 16
        elapsed_minutes = ((message.timestamp - chunk_start_ts) / 60 if current else 0)
        gap_minutes = ((message.timestamp - prev_message.timestamp) / 60 if prev_message else 0)
        message_tokens = extract_topic_tokens(message.text)
        similarity = topic_similarity(message_tokens, chunk_topic_tokens)

        topic_break = (
            bool(current)
            and len(current) >= min_chunk_messages
            and gap_minutes >= soft_gap_minutes
            and similarity < low_similarity_threshold
            and low_similarity_streak >= 1
        )
        hard_gap_break = bool(current) and gap_minutes >= hard_gap_minutes
        # 分片既要控制 prompt 体积，也要尽量在长时间停顿或话题明显变化处切开。
        should_flush = bool(
            current
            and (
                len(current) >= max_messages
                or next_chars > max_chars
                or elapsed_minutes > max_minutes
                or hard_gap_break
                or topic_break
            )
        )
        if should_flush:
            chunks.append(build_chunk(chunk_index, current))
            chunk_index += 1
            current = []
            current_chars = 0
            chunk_start_ts = message.timestamp
            chunk_topic_tokens = set()
            low_similarity_streak = 0

        if not current:
            chunk_start_ts = message.timestamp
        current.append(message)
        current_chars += len(message.sender) + len(message.text) + 16
        chunk_topic_tokens.update(message_tokens)
        if similarity < low_similarity_threshold and gap_minutes >= soft_gap_minutes:
            low_similarity_streak += 1
        else:
            low_similarity_streak = 0
        prev_message = message

    if current:
        chunks.append(build_chunk(chunk_index, current))

    return chunks


def build_analysis_chunks(
    messages: list[StructuredMessage],
    max_messages: int,
    max_chars: int,
    max_minutes: int,
    hard_gap_minutes: int,
    soft_gap_minutes: int,
    low_similarity_threshold: float,
    min_chunk_messages: int,
) -> tuple[list[MessageChunk], dict[str, Any]]:
    """按固定 map-reduce 策略构造消息分片与分析计划。"""
    analysis_messages = [message for message in messages if is_analysis_message(message)]
    if not analysis_messages:
        return [], {
            "strategy": "map-reduce",
            "analysis_message_count": 0,
            "estimated_tokens": 0,
            "mode": "empty",
            "shard_count": 0,
        }

    from .common import estimate_message_tokens

    estimated_tokens = sum(estimate_message_tokens(message) for message in analysis_messages)
    chunks = build_chunks(
        analysis_messages,
        max_messages=max_messages,
        max_chars=max_chars,
        max_minutes=max_minutes,
        hard_gap_minutes=hard_gap_minutes,
        soft_gap_minutes=soft_gap_minutes,
        low_similarity_threshold=low_similarity_threshold,
        min_chunk_messages=min_chunk_messages,
    )
    for index, chunk in enumerate(chunks, start=1):
        chunk.id = f"shard-{index:03d}"
        chunk.index = index

    return chunks, {
        "strategy": "map-reduce",
        "analysis_message_count": len(analysis_messages),
        "estimated_tokens": estimated_tokens,
        "mode": "map_reduce",
        "shard_count": len(chunks),
        "range": {
            "start": analysis_messages[0].time,
            "end": analysis_messages[-1].time,
        },
    }


def get_chunk_topic_keywords(messages: list[StructuredMessage], top_n: int = 12) -> list[str]:
    """提取分片中的高频主题关键词。"""
    counter: Counter[str] = Counter()
    for message in messages:
        if not is_analysis_message(message):
            continue
        counter.update(extract_topic_tokens(message.text))
    return [token for token, _ in counter.most_common(top_n)]


def chunk_payload(chunk: MessageChunk) -> dict[str, Any]:
    """生成 map 阶段使用的完整分片 JSON 输入。"""
    analysis_messages = [message for message in chunk.messages if is_analysis_message(message)]
    member_directory = []
    seen_members: set[str] = set()
    for message in analysis_messages:
        sender_id = message.sender_username or ""
        if not sender_id or sender_id in seen_members:
            continue
        seen_members.add(sender_id)
        member_directory.append(
            {
                "sender_id": sender_id,
                "sender_name": message.sender,
                "mention_token": make_user_placeholder(sender_id),
            }
        )
    return {
        "shard_id": chunk.id,
        "time_range": {
            "start": chunk.start_time,
            "end": chunk.end_time,
        },
        "message_count": chunk.message_count,
        "analysis_message_count": len(analysis_messages),
        "topic_keywords": get_chunk_topic_keywords(chunk.messages),
        "member_directory": member_directory,
        "messages": [
            {
                "id": message.id,
                "time": message.time,
                "sender_id": message.sender_username or "",
                "sender": message.sender,
                "type": message.msg_type,
                "text": message.text,
                "metadata": (
                    {
                        "rich_kind": message.metadata.get("rich_kind", ""),
                        "title": message.metadata.get("title", ""),
                        "summary": message.metadata.get("summary", ""),
                        "source": message.metadata.get("source", ""),
                        "items": message.metadata.get("items", [])[:4],
                    }
                    if message.metadata
                    else {}
                ),
            }
            for message in analysis_messages
        ],
    }


def estimate_chunk_payload_bytes(chunk: MessageChunk) -> int:
    """估算分片 JSON 载荷的 UTF-8 字节数。"""
    return len(json.dumps(chunk_payload(chunk), ensure_ascii=False).encode("utf-8"))


def compact_direct_chunk_payload(chunk: MessageChunk) -> dict[str, Any]:
    """生成 direct-final 模式使用的紧凑消息文本载荷。"""
    analysis_messages = [message for message in chunk.messages if is_analysis_message(message)]
    message_lines = []
    for message in analysis_messages:
        sender_ref = make_user_placeholder(message.sender_username) or message.sender
        message_lines.append(f"{message.time}|{sender_ref}|{message.msg_type}|{message.text}")

    return {
        "shard_id": chunk.id,
        "time_range": f"{chunk.start_time} -> {chunk.end_time}",
        "message_count": chunk.message_count,
        "analysis_message_count": len(analysis_messages),
        "messages_text": "\n".join(message_lines),
    }


def indexed_analysis_messages(chunk: MessageChunk) -> list[tuple[int, StructuredMessage]]:
    """为有效分析消息生成从 1 开始的索引。"""
    return [
        (index, message)
        for index, message in enumerate(
            [item for item in chunk.messages if is_analysis_message(item)],
            start=1,
        )
    ]


def compact_topic_index_payload(chunk: MessageChunk) -> dict[str, Any]:
    """生成 topic-first 主题规划阶段的索引化消息载荷。"""
    message_lines = []
    for index, message in indexed_analysis_messages(chunk):
        sender_ref = make_user_placeholder(message.sender_username) or message.sender
        message_lines.append(f"{index}|{message.time}|{sender_ref}|{message.msg_type}|{message.text}")
    return {
        "time_range": f"{chunk.start_time} -> {chunk.end_time}",
        "analysis_message_count": len(message_lines),
        "messages_text": "\n".join(message_lines),
    }


def compact_topic_section_payload(topic: dict[str, Any], messages: list[StructuredMessage]) -> dict[str, Any]:
    """生成 topic-first 单主题 section 分析载荷。"""
    message_lines = []
    for message in messages:
        sender_ref = make_user_placeholder(message.sender_username) or message.sender
        message_lines.append(f"{message.id}|{message.time}|{sender_ref}|{message.msg_type}|{message.text}")
    return {
        "topic": {
            "topic_id": topic.get("topic_id", ""),
            "title": topic.get("title", ""),
            "summary": topic.get("summary", ""),
            "start_time": topic.get("start_time", ""),
            "end_time": topic.get("end_time", ""),
            "priority": topic.get("priority", ""),
        },
        "message_count": len(messages),
        "messages_text": "\n".join(message_lines),
    }


def estimate_reduce_call_count(item_count: int, fan_in: int) -> int:
    """估算按 fan-in 逐轮 reduce 需要的模型调用次数。"""
    fan_in = max(2, fan_in)
    total = 0
    current = item_count
    while current > fan_in:
        groups = math.ceil(current / fan_in)
        total += groups
        current = groups
    return total
