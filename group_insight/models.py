"""日报分析流程使用的轻量领域模型。

这里集中放置消息与分片的数据结构，供清洗、切片、汇总和渲染阶段共享。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .settings import *


@dataclass
class StructuredMessage:
    """归一化后的单条微信消息。

    这个结构体把数据库或 MCP 返回的原始消息压成后续分析可直接消费的字段。
    """

    id: str
    local_id: int
    timestamp: int
    time: str
    sender_username: str
    sender: str
    text: str
    msg_type: str
    chat_id: str
    chat_name: str
    table_name: str
    metadata: dict[str, Any]


@dataclass
class MessageChunk:
    """分析流程中的消息分片。

    分片用于控制单次 LLM 输入规模，并保留每个分片覆盖的时间和消息范围。
    """

    id: str
    index: int
    start_ts: int
    end_ts: int
    start_time: str
    end_time: str
    message_count: int
    char_count: int
    messages: list[StructuredMessage]
