"""本地统计与词云。

不依赖 LLM 的纯本地计算：发言排行、互动榜单、时段分布、词频统计等。
"""

from __future__ import annotations

import math
import re
from collections import Counter
from datetime import datetime
from typing import Any

from .common import extract_topic_tokens, make_user_placeholder, normalize_text, strip_wechat_emoji_shortcodes
from .conversation import (
    classify_message_category,
    get_message_category_labels,
    is_analysis_message,
    is_effective_conversation_message,
    is_substantive_message,
)
from .fetching import collect_member_aliases_from_messages, is_resolved_member_display
from .models import StructuredMessage
from .rich_content import parse_pat_title_names
from .settings import jieba, WORD_CLOUD_STOPWORDS


def normalize_rank_name(name: str) -> str:
    """清洗排行榜中的成员名，过滤 unknown 等无效值。"""
    name = normalize_text(name or "", max_len=80)
    return "" if name in {"", "unknown"} else name


def ranked_counter(counter: Counter[str], limit: int = 10) -> list[dict[str, Any]]:
    """把计数器转换为带排名的字典列表。"""
    items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [
        {"rank": index, "name": name, "count": count}
        for index, (name, count) in enumerate(items, start=1)
    ]


def build_interaction_rankings(messages: list[StructuredMessage]) -> dict[str, list[dict[str, Any]]]:
    """统计拍一拍、定向红包和回复等互动榜单。"""
    pat_sender_counts: Counter[str] = Counter()
    pat_target_counts: Counter[str] = Counter()
    redpacket_receiver_counts: Counter[str] = Counter()
    reply_sender_counts: Counter[str] = Counter()

    for message in messages:
        category = classify_message_category(message)
        metadata = message.metadata or {}

        if category == "pat":
            pat_sender = normalize_rank_name(metadata.get("pat_from_name", "") or metadata.get("pat_from_username", ""))
            pat_target = normalize_rank_name(metadata.get("pat_to_name", "") or metadata.get("pat_to_username", ""))
            if not pat_sender:
                pat_sender, fallback_target = parse_pat_title_names(message.text)
                pat_sender = normalize_rank_name(message.sender if pat_sender == "我" else pat_sender)
                pat_target = pat_target or normalize_rank_name(fallback_target)
            if not pat_sender:
                pat_sender = normalize_rank_name(message.sender)
            if pat_sender:
                pat_sender_counts[pat_sender] += 1
            if pat_target:
                pat_target_counts[pat_target] += 1

        if metadata.get("interaction_kind") == "direct_redpacket":
            receiver = normalize_rank_name(
                metadata.get("redpacket_receiver_name", "") or metadata.get("redpacket_receiver_username", "")
            )
            if receiver:
                redpacket_receiver_counts[receiver] += 1

        if category == "reply":
            reply_to_username = metadata.get("reply_to_username", "")
            reply_to_name = normalize_rank_name(metadata.get("reply_to_name", ""))
            is_self_reply = bool(reply_to_username and message.sender_username and reply_to_username == message.sender_username)
            if not is_self_reply and reply_to_name and reply_to_name == message.sender:
                is_self_reply = True
            sender = normalize_rank_name(message.sender)
            if sender and not is_self_reply:
                reply_sender_counts[sender] += 1

    return {
        "pat_sender": ranked_counter(pat_sender_counts),
        "pat_target": ranked_counter(pat_target_counts),
        "direct_redpacket_receiver": ranked_counter(redpacket_receiver_counts),
        "reply_sender": ranked_counter(reply_sender_counts),
    }


def build_time_segment_breakdown(messages: list[StructuredMessage]) -> list[dict[str, Any]]:
    """按早中晚等本地时间段统计有效消息分布。"""
    segments = [
        ("凌晨", 0, 5),
        ("早晨", 6, 9),
        ("上午", 10, 11),
        ("下午", 12, 17),
        ("傍晚", 18, 20),
        ("夜间", 21, 23),
    ]
    counts = {label: 0 for label, _, _ in segments}
    for message in messages:
        if not is_analysis_message(message):
            continue
        hour = datetime.fromtimestamp(message.timestamp).hour
        for label, start_hour, end_hour in segments:
            if start_hour <= hour <= end_hour:
                counts[label] += 1
                break
    return [{"label": label, "count": counts[label]} for label, _, _ in segments]


def extract_word_cloud_terms(messages: list[StructuredMessage], top_n: int = 40) -> list[dict[str, Any]]:
    """从有效消息中提取高频词云条目。"""
    counter: Counter[str] = Counter()
    for message in messages:
        if not is_analysis_message(message):
            continue
        text = normalize_text(message.text)
        text = strip_wechat_emoji_shortcodes(text)
        text = re.sub(r"↳\s*回复\s*[^:：]+[:：]", " ", text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"&[a-zA-Z]+;", " ", text)
        text = re.sub(r"\[(图片|表情|语音|链接/文件|系统|视频)[^\]]*\]", " ", text)
        if jieba is not None:
            tokens = [token.strip().lower() for token in jieba.lcut(text)]
        else:
            tokens = [token.lower() for token in extract_topic_tokens(text)]
        for token in tokens:
            token = token.strip()
            if len(token) < 2:
                continue
            if token in WORD_CLOUD_STOPWORDS:
                continue
            if re.fullmatch(r"[\W_]+", token):
                continue
            if token.startswith("http"):
                continue
            if token in {"有氧运动聊天", "回复有氧运动聊天"}:
                continue
            if re.fullmatch(r"\d+", token):
                continue
            if re.fullmatch(r"[a-z0-9_]{2,}", token):
                continue
            if re.fullmatch(r"([\u4e00-\u9fff])\1{1,}", token):
                continue
            counter[token] += 1
    return [
        {"word": word, "count": count}
        for word, count in counter.most_common(top_n)
    ]


def build_local_stats(messages: list[StructuredMessage]) -> dict[str, Any]:
    """生成报表所需的本地精确统计数据。"""
    category_labels = get_message_category_labels()
    category_counts = Counter(classify_message_category(message) for message in messages)
    substantive_messages = [message for message in messages if is_substantive_message(message)]
    effective_messages = [message for message in messages if is_effective_conversation_message(message)]
    member_aliases = collect_member_aliases_from_messages(messages)

    def message_sender_display(message: StructuredMessage) -> str:
        """为统计输出选择成员当前最合适的显示名。"""
        sender_id = message.sender_username or ""
        alias = member_aliases.get(sender_id, "")
        if alias and not is_resolved_member_display(sender_id, message.sender):
            return alias
        return message.sender

    sender_counts = Counter(message_sender_display(message) for message in effective_messages)
    type_counts = Counter(message.msg_type for message in messages)
    hour_counts = Counter(
        datetime.fromtimestamp(message.timestamp).strftime("%H:00")
        for message in effective_messages
        if is_analysis_message(message)
    )

    top_speakers = [
        {"rank": index, "name": name, "message_count": count}
        for index, (name, count) in enumerate(sender_counts.most_common(10), start=1)
    ]
    speaker_directory = []
    seen_speaker_ids: set[str] = set()
    for message in effective_messages:
        sender_id = message.sender_username or ""
        if not sender_id or sender_id in seen_speaker_ids:
            continue
        seen_speaker_ids.add(sender_id)
        speaker_directory.append(
            {
                "sender_id": sender_id,
                "sender_name": message_sender_display(message),
                "mention_token": make_user_placeholder(sender_id),
            }
        )
    excluded_categories = ["pat", "system", "bare_link_file", "redpacket", "other"]
    excluded_count = sum(category_counts.get(category, 0) for category in excluded_categories)
    raw_char_count = sum(len(message.text or "") for message in messages)
    effective_char_count = sum(len(message.text or "") for message in effective_messages)
    return {
        "message_count": len(messages),
        "analysis_message_count": len(effective_messages),
        "substantive_message_count": len(substantive_messages),
        "effective_message_count": len(effective_messages),
        "excluded_message_count": excluded_count,
        "raw_char_count": raw_char_count,
        "effective_char_count": effective_char_count,
        "participant_count": len(sender_counts),
        "known_speakers": [name for name, _ in sender_counts.most_common()],
        "member_aliases": [
            {
                "sender_id": sender_id,
                "sender_name": sender_name,
                "mention_token": make_user_placeholder(sender_id),
            }
            for sender_id, sender_name in sorted(member_aliases.items())
        ],
        "speaker_directory": speaker_directory,
        "type_breakdown": [
            {"type": message_type, "count": count}
            for message_type, count in type_counts.most_common()
        ],
        "category_breakdown": [
            {"type": category_labels[category], "count": count}
            for category, count in category_counts.most_common()
        ],
        "effective_breakdown": [
            {"type": category_labels[category], "count": category_counts[category]}
            for category in ["text", "reply", "emoji", "image", "voice", "video", "link_card", "merged_chat"]
            if category_counts.get(category, 0)
        ],
        "excluded_breakdown": [
            {"type": category_labels[category], "count": category_counts[category]}
            for category in excluded_categories
            if category_counts.get(category, 0)
        ],
        "top_speakers": top_speakers,
        "interaction_rankings": build_interaction_rankings(messages),
        "busiest_hours": [
            {"hour": hour, "count": count}
            for hour, count in hour_counts.most_common(6)
        ],
        "time_segment_breakdown": build_time_segment_breakdown(messages),
        "word_cloud": extract_word_cloud_terms(messages),
        "unknown_message_count": sum(1 for message in messages if message.sender == "unknown"),
        "system_message_count": sum(1 for message in messages if message.msg_type == "系统"),
        "first_message_time": messages[0].time if messages else "",
        "last_message_time": messages[-1].time if messages else "",
    }
