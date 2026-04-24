"""微信富媒体消息解析。

负责从 appmsg XML 中提取链接卡片、合并聊天、回复、拍一拍、红包等
结构化元数据，供后续分析和渲染使用。
"""

from __future__ import annotations

import re
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Any

from .common import collapse_text, parse_int
from .settings import APPMSG_XML_MAX_LEN, RECORDITEM_XML_MAX_LEN, wechat_mcp


def parse_xml_root_with_limit(content: str, max_len: int) -> ET.Element | None:
    """在长度和安全字符检查通过后解析 XML 根节点。"""
    content = (content or "").strip()
    if not content or len(content) > max_len:
        return None
    unsafe_re = getattr(wechat_mcp, "_XML_UNSAFE_RE", None)
    if unsafe_re is not None and unsafe_re.search(content):
        return None
    try:
        return ET.fromstring(content)
    except ET.ParseError:
        return None


def summarize_record_items(items: list[dict[str, str]], max_items: int = 4) -> str:
    """把合并聊天记录中的若干条目压缩成一行摘要。"""
    snippets = []
    for item in items[:max_items]:
        name = collapse_text(item.get("name", ""), max_len=24)
        text = collapse_text(item.get("text", ""), max_len=60)
        if not text:
            continue
        snippets.append(f"{name}: {text}" if name else text)
    return "；".join(snippets)


def build_rich_card_preview(metadata: dict[str, Any], fallback_text: str) -> str:
    """根据富媒体元数据生成适合进入 LLM 的可读预览文本。"""
    kind = metadata.get("rich_kind", "")
    title = collapse_text(metadata.get("title", ""), max_len=90)
    summary = collapse_text(metadata.get("summary", ""), max_len=120)
    source = collapse_text(metadata.get("source", ""), max_len=30)
    if kind == "link_card":
        parts = [f"[链接] {title}" if title else "[链接]"]
        if summary:
            parts.append(f"摘要：{summary}")
        if source:
            parts.append(f"来源：{source}")
        return "；".join(parts)
    if kind == "merged_chat":
        parts = [f"[聊天记录] {title}" if title else "[聊天记录]"]
        if summary:
            parts.append(f"摘要：{summary}")
        item_summary = summarize_record_items(metadata.get("items", []))
        if item_summary:
            parts.append(f"包含：{item_summary}")
        return "；".join(parts)
    return fallback_text


def parse_pat_title_names(title: str) -> tuple[str, str]:
    """从拍一拍标题里解析发起人与目标显示名。"""
    match = re.search(r'(?:"([^"]+)"|(我))\s*拍了拍\s*"([^"]+)"', title or "")
    if not match:
        return "", ""
    return (match.group(1) or match.group(2) or "").strip(), match.group(3).strip()


def parse_query_param(url: str, name: str) -> str:
    """从 URL 查询串中读取指定参数的第一个值。"""
    if not url:
        return ""
    try:
        values = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get(name, [])
    except ValueError:
        return ""
    return values[0].strip() if values else ""


def extract_rich_message_metadata(
    raw_content: str,
    local_type: int,
    is_group: bool,
) -> dict[str, Any]:
    """解析微信 appmsg XML，提取链接、合并聊天、回复、拍一拍和红包等元数据。"""
    if not raw_content:
        return {}
    base_type, sub_type = wechat_mcp._split_msg_type(local_type)
    if base_type != 49:
        return {}

    _, message_content = wechat_mcp._parse_message_content(raw_content, local_type, is_group)
    if "<appmsg" not in (message_content or ""):
        return {}

    root = parse_xml_root_with_limit(message_content, APPMSG_XML_MAX_LEN)
    if root is None:
        return {}
    appmsg = root.find(".//appmsg")
    if appmsg is None:
        return {}

    title = collapse_text(appmsg.findtext("title") or "", max_len=120)
    summary = collapse_text(appmsg.findtext("des") or "", max_len=180)
    source = collapse_text(appmsg.findtext("sourcedisplayname") or "", max_len=40)
    url = collapse_text((appmsg.findtext("url") or "").replace("&amp;", "&"), max_len=400)
    app_type = parse_int(appmsg.findtext("type") or sub_type, parse_int(sub_type, 0))

    if app_type == 57:
        ref = appmsg.find(".//refermsg")
        reply_to_username = ""
        reply_to_display_name = ""
        reply_to_content = ""
        if ref is not None:
            reply_to_username = (ref.findtext("chatusr") or "").strip()
            if not reply_to_username:
                reply_to_username = (ref.findtext("fromusr") or "").strip()
            reply_to_display_name = collapse_text(ref.findtext("displayname") or "", max_len=60)
            reply_to_content = collapse_text(ref.findtext("content") or "", max_len=160)
        return {
            "interaction_kind": "reply",
            "reply_to_username": reply_to_username,
            "reply_to_name": reply_to_display_name,
            "reply_to_content": reply_to_content,
            "app_type": app_type,
        }

    if app_type == 62:
        pat_actor_name, pat_target_name = parse_pat_title_names(title)
        patinfo = appmsg.find(".//patinfo")
        return {
            "interaction_kind": "pat",
            "title": title,
            "pat_from_username": (patinfo.findtext("fromusername") or "").strip() if patinfo is not None else "",
            "pat_to_username": (patinfo.findtext("pattedusername") or "").strip() if patinfo is not None else "",
            "pat_from_name": pat_actor_name,
            "pat_to_name": pat_target_name,
            "app_type": app_type,
        }

    if app_type == 2001:
        native_url = appmsg.findtext(".//wcpayinfo/nativeurl") or ""
        sender_username = (root.findtext("fromusername") or "").strip()
        if not sender_username:
            sender_username = parse_query_param(native_url, "sendusername")
        receiver_username = (appmsg.findtext(".//wcpayinfo/exclusive_recv_username") or "").strip()
        return {
            "interaction_kind": "direct_redpacket" if receiver_username else "redpacket",
            "title": title,
            "summary": summary,
            "redpacket_sender_username": sender_username,
            "redpacket_receiver_username": receiver_username,
            "redpacket_memo": collapse_text(appmsg.findtext(".//wcpayinfo/receivertitle") or "", max_len=80),
            "app_type": app_type,
        }

    if app_type == 5:
        metadata = {
            "rich_kind": "link_card",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "app_type": app_type,
        }
        metadata["analysis_text"] = build_rich_card_preview(metadata, f"[链接] {title}" if title else "[链接]")
        return metadata

    if app_type == 19:
        items: list[dict[str, str]] = []
        recorditem = appmsg.findtext("recorditem") or ""
        if recorditem:
            ri_root = parse_xml_root_with_limit(recorditem, RECORDITEM_XML_MAX_LEN)
            if ri_root is not None:
                for dataitem in ri_root.findall(".//dataitem"):
                    name = collapse_text(dataitem.findtext("sourcename") or "", max_len=24)
                    text = collapse_text(dataitem.findtext("datadesc") or "", max_len=80)
                    if not text:
                        continue
                    items.append({"name": name, "text": text})
                    if len(items) >= 8:
                        break
        metadata = {
            "rich_kind": "merged_chat",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "items": items,
            "app_type": app_type,
        }
        metadata["analysis_text"] = build_rich_card_preview(metadata, f"[聊天记录] {title}" if title else "[聊天记录]")
        return metadata

    if app_type in (33, 36, 44):
        metadata = {
            "rich_kind": "link_card",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "app_type": app_type,
        }
        metadata["analysis_text"] = build_rich_card_preview(metadata, f"[小程序] {title}" if title else "[小程序]")
        return metadata

    if app_type == 6:
        return {
            "rich_kind": "file_card",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "app_type": app_type,
        }

    return {}


def has_meaningful_rich_content(message) -> bool:
    """判断富媒体消息是否有足够内容进入语义分析。"""
    from .models import StructuredMessage

    metadata = message.metadata or {}
    kind = metadata.get("rich_kind", "")
    if kind == "link_card":
        return bool(metadata.get("title") or metadata.get("summary"))
    if kind == "merged_chat":
        return bool(metadata.get("title") or metadata.get("summary") or metadata.get("items"))
    return False
