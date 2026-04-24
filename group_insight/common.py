"""跨模块通用工具函数。

放置不依赖任何业务子模块的基础工具，避免循环导入。
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from .settings import (
    MAX_LINE_TEXT_LEN,
    SINGLE_CJK_PATTERN,
    TOKEN_PATTERN,
    WORD_TOKEN_PATTERN,
    WECHAT_EMOJI_SHORTCODE_PATTERN,
)


def slugify(value: str) -> str:
    """把群名等任意文本转换为适合目录名的短标识。"""
    value = value.strip()
    value = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "group-insight"


def format_ts(ts: int) -> str:
    """将秒级时间戳格式化为报表使用的分钟级时间。"""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def normalize_text(value: str, max_len: int | None = None) -> str:
    """折叠连续空白，并在需要时按最大长度截断文本。"""
    value = re.sub(r"\s+", " ", value or "").strip()
    if max_len and len(value) > max_len:
        cut = max(0, max_len - 3)
        open_pos = value.rfind("[[user:", 0, cut)
        close_pos = value.find("]]", open_pos + 7) if open_pos >= 0 else -1
        if open_pos >= 0 and (close_pos < 0 or close_pos + 2 > cut):
            cut = open_pos
        return value[:cut].rstrip() + "..."
    return value


def strip_wechat_emoji_shortcodes(value: str) -> str:
    """移除微信表情短码，避免低信息噪声进入词频统计。"""
    return WECHAT_EMOJI_SHORTCODE_PATTERN.sub(" ", value or "")


def collapse_text(value: str, max_len: int | None = None) -> str:
    """统一调用 normalize_text，保留旧命名下的文本压缩入口。"""
    return normalize_text(value, max_len=max_len)


def parse_int(value: Any, fallback: int = 0) -> int:
    """容错地把输入转换为整数，失败时返回 fallback。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def ensure_dir(path: Path) -> Path:
    """创建目录并返回 Path，供各阶段输出目录复用。"""
    path.mkdir(parents=True, exist_ok=True)
    return path


def make_user_placeholder(sender_username: str) -> str:
    """把成员账号转换为模型可保留的成员占位符。"""
    sender_username = (sender_username or "").strip()
    return f"[[user:{sender_username}]]" if sender_username else ""


def safe_json_loads(payload: str) -> Any:
    """解析模型返回的 JSON，并在前后夹杂文本时尝试抽取对象。"""
    payload = (payload or "").strip()
    if not payload:
        raise ValueError("empty json payload")
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload)
        payload = re.sub(r"\s*```$", "", payload)

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        candidate = extract_json_object(payload)
        if candidate:
            return json.loads(candidate)
        raise


def extract_json_object(payload: str) -> str:
    """从任意文本中提取第一个括号配平的 JSON 对象片段。"""
    start = payload.find("{")
    if start < 0:
        return ""

    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(payload)):
        char = payload[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return payload[start : index + 1]
    return ""


def write_json(path: Path, payload: Any) -> None:
    """以 UTF-8 和缩进格式写入 JSON 文件。"""
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def extract_topic_tokens(text: str) -> set[str]:
    """抽取用于分片相似度和词云统计的主题 token。"""
    normalized = normalize_text(text)
    normalized = re.sub(r"\[(图片|表情|语音|链接/文件|通话)[^\]]*\]", " ", normalized)
    tokens: set[str] = set()
    for match in TOKEN_PATTERN.findall(normalized):
        token = match.lower()
        if len(token) >= 2:
            tokens.add(token)
        if re.fullmatch(r"[\u4e00-\u9fff]{4,}", token):
            for index in range(len(token) - 1):
                tokens.add(token[index : index + 2])
    return tokens


def estimate_text_tokens(text: str) -> int:
    """用字符数粗略估算 prompt token 数。"""
    text = (text or "").strip()
    if not text:
        return 0

    cjk_chars = len(SINGLE_CJK_PATTERN.findall(text))
    words = WORD_TOKEN_PATTERN.findall(text)
    word_token_estimate = sum(max(1, math.ceil(len(word) / 4)) for word in words)
    consumed = cjk_chars + sum(len(word) for word in words)
    remaining_chars = max(0, len(text) - consumed)
    remaining_estimate = math.ceil(remaining_chars * 0.5)
    cjk_estimate = math.ceil(cjk_chars * 0.65)
    return max(1, cjk_estimate + word_token_estimate + remaining_estimate)


def estimate_message_tokens(message) -> int:
    """估算单条结构化消息进入模型时占用的 token。"""
    base = estimate_text_tokens(message.text)
    sender_cost = max(1, math.ceil(len(message.sender) / 3))
    return base + sender_cost + 8


def estimate_prompt_tokens(system_prompt: str, user_prompt: str) -> int:
    """估算 system 与 user prompt 的总 token。"""
    return estimate_text_tokens(system_prompt) + estimate_text_tokens(user_prompt)


def log_llm_request_estimate(
    stage: str,
    client: Any | None,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int | None,
) -> None:
    """打印一次 LLM 请求的 token 和成本估算日志。"""
    input_tokens = estimate_prompt_tokens(system_prompt, user_prompt)
    provider = client.provider if client else "none"
    model = client.model if client else ""
    output_budget = f"output<= {max_tokens} tokens" if max_tokens is not None else "output budget=default"
    message = (
        f"[LLMEstimate] {stage} provider={provider}"
        f"{('/' + model) if model else ''} "
        f"input~{input_tokens} tokens, {output_budget}"
    )
    print(message, flush=True)


def topic_similarity(a: set[str], b: set[str]) -> float:
    """计算两个主题 token 集合的 Jaccard 相似度。"""
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0
