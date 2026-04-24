"""最终日报结构的修复、去重和本地 fallback 生成。

LLM 输出进入渲染前会在这里被规范化，避免缺字段、重复主题、时间线覆盖不足或
dry-run 无模型时无法生成报表。
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from .common import extract_topic_tokens, make_user_placeholder, normalize_text, topic_similarity
from .models import MessageChunk
from .settings import MAX_REPORT_SECTIONS, SECTION_TOPIC_COVERAGE_THRESHOLD


def parse_report_time(value: str) -> datetime | None:
    """解析报表 section 中允许的时间字符串。"""
    value = (value or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def section_sort_key(section: dict[str, Any]) -> tuple[float, float, str]:
    """生成 section 按时间线排序时使用的键。"""
    start_dt = parse_report_time(section.get("start_time", ""))
    end_dt = parse_report_time(section.get("end_time", ""))
    start_ts = start_dt.timestamp() if start_dt else float("inf")
    end_ts = end_dt.timestamp() if end_dt else float("inf")
    return start_ts, end_ts, section.get("title", "")


def dedupe_theme_cards(cards: list[dict[str, Any]], limit: int = 4) -> list[dict[str, Any]]:
    """清洗并去重主题卡片。"""
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for card in cards:
        title = normalize_text(card.get("title", ""), max_len=120)
        summary = normalize_text(card.get("summary", ""), max_len=420)
        if not title and not summary:
            continue
        key = (title.lower(), summary.lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append({"title": title or "主题", "summary": summary})
        if len(deduped) >= limit:
            break
    return deduped


def dedupe_sections(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """清洗、排序并去重报表主体 section。"""
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for section in sorted(sections, key=section_sort_key):
        title = normalize_text(section.get("title", ""), max_len=140)
        start_time = (section.get("start_time", "") or "").strip()
        end_time = (section.get("end_time", "") or "").strip()
        summary = normalize_text(section.get("summary", ""), max_len=700)
        bullets = [
            normalize_text(item, max_len=320)
            for item in section.get("bullets", [])
            if normalize_text(item, max_len=320)
        ][:4]
        takeaway = normalize_text(section.get("takeaway", ""), max_len=320)
        if not title and not summary:
            continue
        key = (title.lower(), start_time, end_time)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(
            {
                "title": title or "讨论片段",
                "start_time": start_time,
                "end_time": end_time,
                "summary": summary,
                "bullets": bullets,
                "takeaway": takeaway,
            }
        )
    return deduped



def select_timeline_sections(sections: list[dict[str, Any]], limit: int = MAX_REPORT_SECTIONS) -> list[dict[str, Any]]:
    """在 section 过多时按时间线均匀保留代表片段。"""
    if len(sections) <= limit:
        return sections
    if limit <= 1:
        return sections[:1]
    selected_indexes = {0, len(sections) - 1}
    for slot in range(1, limit - 1):
        index = round(slot * (len(sections) - 1) / (limit - 1))
        selected_indexes.add(index)
    index = 0
    while len(selected_indexes) < limit and index < len(sections):
        selected_indexes.add(index)
        index += 1
    return [sections[i] for i in sorted(selected_indexes)[:limit]]


def build_report_sections_from_bundles(bundles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """从 reduce bundles 中恢复可兜底使用的主体 section。"""
    sections: list[dict[str, Any]] = []
    for bundle in bundles:
        for section in bundle.get("highlight_sections", []):
            sections.append(
                {
                    "title": section.get("title", "讨论片段"),
                    "start_time": section.get("start_time", ""),
                    "end_time": section.get("end_time", ""),
                    "summary": section.get("summary", ""),
                    "bullets": section.get("bullets", [])[:2],
                    "takeaway": normalize_text(section.get("summary", ""), max_len=120),
                }
            )
    return select_timeline_sections(dedupe_sections(sections), limit=MAX_REPORT_SECTIONS)



def section_topic_tokens(section: dict[str, Any]) -> set[str]:
    """抽取 section 文本中的主题 token，用于覆盖度判断。"""
    parts = [
        normalize_text(section.get("title", ""), max_len=120),
        normalize_text(section.get("summary", ""), max_len=240),
        normalize_text(section.get("takeaway", ""), max_len=160),
    ]
    bullets = section.get("bullets", [])
    for bullet in bullets[:3]:
        parts.append(normalize_text(bullet, max_len=120))

    tokens: set[str] = set()
    for part in parts:
        if part:
            tokens.update(extract_topic_tokens(part))
    return tokens


def bundle_section_is_covered(
    report_sections: list[dict[str, Any]],
    bundle_section: dict[str, Any],
) -> bool:
    """判断 bundle 中的一个 section 是否已被最终报表覆盖。"""
    candidate_tokens = section_topic_tokens(bundle_section)
    candidate_start = parse_report_time(bundle_section.get("start_time", ""))
    candidate_end = parse_report_time(bundle_section.get("end_time", ""))
    if not candidate_start or not candidate_end or candidate_end <= candidate_start:
        candidate_title = normalize_text(bundle_section.get("title", "")).lower()
        for item in report_sections:
            if normalize_text(item.get("title", "")).lower() != candidate_title:
                continue
            if candidate_tokens and topic_similarity(candidate_tokens, section_topic_tokens(item)) < SECTION_TOPIC_COVERAGE_THRESHOLD:
                continue
            return True
        return False

    midpoint = candidate_start.timestamp() + (candidate_end.timestamp() - candidate_start.timestamp()) / 2
    candidate_duration = max(60.0, candidate_end.timestamp() - candidate_start.timestamp())
    for section in report_sections:
        report_start = parse_report_time(section.get("start_time", ""))
        report_end = parse_report_time(section.get("end_time", ""))
        if not report_start or not report_end or report_end <= report_start:
            continue
        if candidate_tokens and topic_similarity(candidate_tokens, section_topic_tokens(section)) < SECTION_TOPIC_COVERAGE_THRESHOLD:
            continue
        report_start_ts = report_start.timestamp()
        report_end_ts = report_end.timestamp()
        if report_start_ts <= midpoint <= report_end_ts:
            return True
        overlap = min(candidate_end.timestamp(), report_end_ts) - max(candidate_start.timestamp(), report_start_ts)
        if overlap > 0 and (overlap / candidate_duration) >= 0.5:
            return True
    return False


def final_sections_need_repair(
    report_sections: list[dict[str, Any]],
    bundle_sections: list[dict[str, Any]],
) -> bool:
    """判断最终报表 section 是否需要用 bundle 内容补齐。"""
    if not bundle_sections:
        return False
    if not report_sections:
        return True
    if len(report_sections) < min(6, len(bundle_sections)):
        return True

    uncovered = [section for section in bundle_sections if not bundle_section_is_covered(report_sections, section)]
    for section in uncovered:
        start_dt = parse_report_time(section.get("start_time", ""))
        end_dt = parse_report_time(section.get("end_time", ""))
        if start_dt and end_dt and (end_dt.timestamp() - start_dt.timestamp()) >= 7200:
            return True
    if len(uncovered) > max(1, len(bundle_sections) // 3):
        return True

    first_bundle = parse_report_time(bundle_sections[0].get("start_time", ""))
    last_bundle = parse_report_time(bundle_sections[-1].get("end_time", ""))
    first_report = parse_report_time(report_sections[0].get("start_time", ""))
    last_report = parse_report_time(report_sections[-1].get("end_time", ""))
    if first_bundle and first_report and (first_report.timestamp() - first_bundle.timestamp()) > 5400:
        return True
    if last_bundle and last_report and (last_bundle.timestamp() - last_report.timestamp()) > 5400:
        return True
    return False


def merge_repaired_sections(
    report_sections: list[dict[str, Any]],
    bundle_sections: list[dict[str, Any]],
    limit: int = MAX_REPORT_SECTIONS,
) -> list[dict[str, Any]]:
    """把缺失的 bundle section 合并回最终报表并重新去重。"""
    merged = dedupe_sections(report_sections)
    for section in bundle_sections:
        if not bundle_section_is_covered(merged, section):
            merged.append(section)
    return select_timeline_sections(dedupe_sections(merged), limit=limit)


def build_theme_cards_from_bundles(bundles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """从 bundles 中收集主题卡片作为兜底摘要。"""
    cards: list[dict[str, Any]] = []
    for bundle in bundles:
        cards.extend(bundle.get("theme_cards", []))
    return dedupe_theme_cards(cards, limit=4)


def repair_final_report(
    report: dict[str, Any],
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    bundles: list[dict[str, Any]],
) -> dict[str, Any]:
    """规范化最终报表结构，并修复缺字段或覆盖不足的问题。"""
    repaired = {
        "headline": normalize_text(report.get("headline", ""), max_len=120) or f"{chat_name} 群洞察报表",
        "tagline": normalize_text(report.get("tagline", ""), max_len=180) or f"{start_time} - {end_time}",
        "lead_summary": normalize_text(report.get("lead_summary", ""), max_len=1600),
        "theme_cards": dedupe_theme_cards(report.get("theme_cards", []), limit=4),
        "sections": dedupe_sections(report.get("sections", [])),
        "participant_insights": report.get("participant_insights", [])[:6],
        "quotes": report.get("quotes", [])[:4],
        "decisions": report.get("decisions", [])[:6],
        "action_items": report.get("action_items", [])[:6],
        "open_questions": report.get("open_questions", [])[:6],
        "risk_flags": report.get("risk_flags", [])[:6],
        "mood": report.get("mood", {}) if isinstance(report.get("mood"), dict) else {},
    }

    bundle_sections = build_report_sections_from_bundles(bundles)
    if final_sections_need_repair(repaired["sections"], bundle_sections):
        repaired["sections"] = merge_repaired_sections(repaired["sections"], bundle_sections)
        bundle_theme_cards = build_theme_cards_from_bundles(bundles)
        if bundle_theme_cards:
            repaired["theme_cards"] = bundle_theme_cards

    if not repaired["lead_summary"]:
        repaired["lead_summary"] = (
            f"本次统计区间内原始消息 {stats.get('message_count', 0)} 条，"
            f"有效对话 {stats.get('effective_message_count', 0)} 条，"
            f"参与成员 {stats.get('participant_count', 0)} 位。"
        )
    if not repaired["theme_cards"]:
        repaired["theme_cards"] = build_theme_cards_from_bundles(bundles) or [
            {
                "title": "消息概览",
                "summary": (
                    f"原始消息 {stats.get('message_count', 0)} 条，"
                    f"有效对话 {stats.get('effective_message_count', 0)} 条。"
                ),
            }
        ]
    return repaired


def fallback_map_analysis(chunk: MessageChunk) -> dict[str, Any]:
    """在 dry-run 或 map 失败时生成本地片段分析结果。"""
    speaker_counts = Counter(message.sender for message in chunk.messages)
    top_names = [name for name, _ in speaker_counts.most_common(3)]
    top_line_ids = [message.id for message in chunk.messages[:3]]
    speaker_placeholders = {
        message.sender: make_user_placeholder(message.sender_username) or message.sender
        for message in chunk.messages
        if message.sender
    }
    highlight_title = f"{chunk.start_time} - {chunk.end_time} 讨论片段"
    return {
        "shard_id": chunk.id,
        "time_range": {"start": chunk.start_time, "end": chunk.end_time},
        "summary": f"该时间片共 {chunk.message_count} 条消息，主要发言者为 {'、'.join(top_names) if top_names else '未知成员'}。",
        "theme_cards": [
            {
                "title": "时间片概览",
                "summary": f"本片段覆盖 {chunk.start_time} 至 {chunk.end_time}，共 {chunk.message_count} 条消息。",
                "evidence_ids": top_line_ids,
            }
        ],
        "highlight_sections": [
            {
                "title": highlight_title,
                "start_time": chunk.start_time,
                "end_time": chunk.end_time,
                "summary": f"主要发言者为 {'、'.join(top_names) if top_names else '未知成员'}。",
                "bullets": [
                    f"消息量 {chunk.message_count} 条",
                    f"涉及 {len(speaker_counts)} 位发言者",
                ],
                "evidence_ids": top_line_ids,
            }
        ],
        "participant_notes": [
            {
                "name": speaker_placeholders.get(name, name),
                "observation": f"在该时间片发言 {count} 条。",
                "evidence_ids": top_line_ids[:1],
            }
            for name, count in speaker_counts.most_common(3)
        ],
        "quotes": [
            {
                "speaker": make_user_placeholder(message.sender_username) or message.sender,
                "time": message.time,
                "quote": message.text,
                "message_id": message.id,
                "why_it_matters": "作为该时间片的代表性原话。",
            }
            for message in chunk.messages[:2]
        ],
        "decisions": [],
        "action_items": [],
        "open_questions": [],
        "mood": {
            "label": "活跃",
            "reason": "使用本地 dry-run，未调用外部模型，仅基于消息量做概览。",
            "evidence_ids": top_line_ids,
        },
    }


def fallback_reduce_bundle(bundle_id: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    """在 dry-run 或 reduce 失败时本地合并分析结果。"""
    theme_cards = []
    highlight_sections = []
    participant_notes = []
    quotes = []
    action_items = []
    decisions = []
    open_questions = []
    risk_flags = []
    source_refs = []

    for item in items:
        ref = item.get("shard_id") or item.get("bundle_id") or "unknown"
        source_refs.append(ref)
        theme_cards.extend(item.get("theme_cards", [])[:1])
        highlight_sections.extend(item.get("highlight_sections", [])[:2])
        participant_notes.extend(item.get("participant_notes", [])[:2])
        quotes.extend(item.get("quotes", [])[:2])
        action_items.extend(item.get("action_items", []))
        decisions.extend(item.get("decisions", []))
        open_questions.extend(item.get("open_questions", []))
        risk_flags.extend(item.get("risk_flags", []))

    summary = items[0].get("summary", "") if items else ""
    return {
        "bundle_id": bundle_id,
        "summary": summary or f"{len(items)} 个片段的合并摘要。",
        "theme_cards": [
            {
                "title": card.get("title", "主题"),
                "summary": card.get("summary", ""),
                "source_refs": source_refs,
            }
            for card in theme_cards[:4]
        ],
        "highlight_sections": [
            {
                "title": section.get("title", "讨论片段"),
                "start_time": section.get("start_time", ""),
                "end_time": section.get("end_time", ""),
                "summary": section.get("summary", ""),
                "bullets": section.get("bullets", [])[:3],
                "source_refs": source_refs,
            }
            for section in highlight_sections[:6]
        ],
        "participant_notes": [
            {
                "name": note.get("name", ""),
                "observation": note.get("observation", ""),
                "source_refs": source_refs,
            }
            for note in participant_notes[:6]
        ],
        "quotes": [
            {
                "speaker": quote.get("speaker", ""),
                "time": quote.get("time", ""),
                "quote": quote.get("quote", ""),
                "source_refs": source_refs,
            }
            for quote in quotes[:6]
        ],
        "decisions": [
            {
                "content": decision.get("content", ""),
                "source_refs": source_refs,
            }
            for decision in decisions[:6]
        ],
        "action_items": [
            {
                "owner": action.get("owner", ""),
                "task": action.get("task", ""),
                "deadline": action.get("deadline", ""),
                "status_hint": action.get("status_hint", ""),
                "source_refs": source_refs,
            }
            for action in action_items[:6]
        ],
        "open_questions": [
            {"question": question.get("question", ""), "source_refs": source_refs}
            for question in open_questions[:6]
        ],
        "risk_flags": risk_flags[:6],
        "mood": {
            "label": "概览",
            "reason": "本地 dry-run 合并结果。",
            "source_refs": source_refs,
        },
    }


def fallback_final_report(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    bundles: list[dict[str, Any]],
) -> dict[str, Any]:
    """在 dry-run 或 final 失败时生成可渲染的本地日报。"""
    sections = []
    for bundle in bundles:
        for section in bundle.get("highlight_sections", [])[:8]:
            sections.append(
                {
                    "title": section.get("title", "讨论片段"),
                    "start_time": section.get("start_time", ""),
                    "end_time": section.get("end_time", ""),
                    "summary": section.get("summary", ""),
                    "bullets": section.get("bullets", [])[:3],
                    "takeaway": "本地 dry-run 输出，建议接入 DeepSeek 获取更强语义总结。",
                }
            )
    sections.sort(key=lambda item: (item["start_time"], item["end_time"]))
    theme_cards = []
    for bundle in bundles:
        for card in bundle.get("theme_cards", []):
            theme_cards.append(
                {
                    "title": card.get("title", "主题"),
                    "summary": card.get("summary", ""),
                }
            )
    theme_cards = theme_cards[:4] or [
        {
            "title": "消息概览",
            "summary": (
                f"原始消息 {stats['message_count']} 条，"
                f"有效对话 {stats['effective_message_count']} 条，"
                f"{stats['participant_count']} 位参与者。"
            ),
        }
    ]
    ranking_labels = {
        "pat_sender": "拍一拍最多",
        "pat_target": "被拍最多",
        "direct_redpacket_receiver": "定向红包收到最多",
        "reply_sender": "回复他人最多",
    }
    interaction_bits = []
    for key, label in ranking_labels.items():
        top_items = stats.get("interaction_rankings", {}).get(key, [])
        if top_items:
            top_item = top_items[0]
            interaction_bits.append(f"{label}：{top_item.get('name', '')} {top_item.get('count', 0)} 次")
    interaction_summary = f"互动榜单：{'；'.join(interaction_bits)}。" if interaction_bits else ""

    return {
        "headline": f"{chat_name} 群洞察报表",
        "tagline": f"{start_time} - {end_time}",
        "lead_summary": (
            f"本次统计区间内原始消息 {stats['message_count']} 条，"
            f"其中有效对话 {stats['effective_message_count']} 条，"
            f"已剔除拍一拍、系统消息、红包、占位链接/文件等 {stats['excluded_message_count']} 条非对话消息；"
            f"有效参与者 {stats['participant_count']} 位。"
            f"{interaction_summary}"
            "当前为本地 dry-run 结果，已完成导出、分片、汇总和报表渲染链路验证。"
        ),
        "theme_cards": theme_cards,
        "sections": sections[:MAX_REPORT_SECTIONS],
        "participant_insights": [
            {
                "name": speaker["name"],
                "insight": f"在有效对话口径下发言 {speaker['message_count']} 条。",
            }
            for speaker in stats["top_speakers"][:5]
        ],
        "quotes": [],
        "decisions": [],
        "action_items": [],
        "open_questions": [],
        "risk_flags": ["当前为 dry-run，未接入外部语义分析。"],
        "mood": {
            "label": "活跃",
            "reason": "基于消息量与参与人数的本地判断。",
        },
    }
