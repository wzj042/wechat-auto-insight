#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate a structured group chat insight report from decrypted WeChat messages.

常用运行方式:
1. 直接编辑下方“可直接改这里”的默认常量后运行:
   python group_insight_report.py
2. 通过命令行覆盖默认值:
   python group_insight_report.py --chat "有氧运动聊天" --start "2026-04-09 06:20" --end "2026-04-10 08:46"
3. 生成后立即发送到一个或多个指定会话:
   python group_insight_report.py --chat "有氧运动聊天" --send-after-run --send-target "文件传输助手" --send-target "有氧运动聊天"
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import html
import json
import logging
import math
import os
import re
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from subprocess import DEVNULL, CalledProcessError, run
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent
SCRIPT_DIR = ROOT_DIR 
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import mcp_server as wechat_mcp  # noqa: E402
try:
    import jieba
except Exception:  # pragma: no cover - optional dependency behavior
    jieba = None
else:
    try:
        jieba.setLogLevel(logging.WARNING)
    except Exception:
        pass


# 可直接改这里:
# 默认 LLM 提供方；可选 "zhipu" / "deepseek"。命令行 --provider 会覆盖它。
DEFAULT_PROVIDER = "deepseek"
DEFAULT_API_URL = "https://api.deepseek.com/chat/completions"
# deepseek provider 的默认模型；命令行 --model 会覆盖它。
DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"
# zhipu provider 的默认模型；命令行 --model 会覆盖它。
DEFAULT_ZHIPU_MODEL = "glm-4.5-flash"
# 默认要分析的群聊名称或 chatroom id；留空时必须通过 --chat 传入。
DEFAULT_ANALYZE_CHAT = "有氧运动聊天"
# 默认自动时间窗。True 时自动分析“昨日 DEFAULT_AUTO_TIME_CUTOFF 到今日 DEFAULT_AUTO_TIME_CUTOFF”。
DEFAULT_AUTO_TIME = True
# AUTO_TIME 的日切时间；默认分析昨日 03:59 到今日 03:59。
DEFAULT_AUTO_TIME_CUTOFF = "23:59"
# DEFAULT_AUTO_TIME=False 时使用的默认开始时间；格式 YYYY-MM-DD HH:MM[:SS]。
DEFAULT_ANALYZE_START = ""
# DEFAULT_AUTO_TIME=False 时使用的默认结束时间；格式 YYYY-MM-DD HH:MM[:SS]。
DEFAULT_ANALYZE_END = ""
# 设为 True 时，脚本生成 PNG 后会自动尝试发送。
DEFAULT_SEND_AFTER_RUN = True
# 默认发送目标会话列表；可以包含“文件传输助手”、好友或群聊名称。
DEFAULT_SEND_TARGET_CHATS = [
    "有氧运动聊天",
    # "文件传输助手",
    ]
# 默认附带文本；留空时使用脚本自动生成的摘要。
DEFAULT_SEND_MESSAGE =  datetime.now().strftime("%m-%d") + "日报已发送"  
DEFAULT_OUTPUT_ROOT = SCRIPT_DIR / "reports" / "group_insight"
STAGE_CACHE_VERSION = 3
MAX_LINE_TEXT_LEN = 280
APPMSG_XML_MAX_LEN = 120000
RECORDITEM_XML_MAX_LEN = 240000
DEFAULT_REPORT_IMAGE_WIDTH = 760
DEFAULT_REPORT_IMAGE_TIMEOUT_MS = 20000
DEFAULT_DIRECT_MAX_BYTES = 0
DEFAULT_FILEHELPER_NAME = "文件传输助手"
DEFAULT_DIRECT_FINAL_MAX_TOKENS = 8192
DEFAULT_DIRECT_FINAL_MIN_TOKENS = 2048
DEFAULT_TOPIC_FIRST = False
DEFAULT_TOPIC_FIRST_MAX_TOPICS = 16
DEFAULT_TOPIC_SECTION_MAX_TOKENS = 3072
DEEPSEEK_CONTEXT_WINDOW_TOKENS = 131072
DEEPSEEK_INPUT_CACHE_HIT_USD_PER_M_TOKEN = 0.028
DEEPSEEK_INPUT_CACHE_MISS_USD_PER_M_TOKEN = 0.28
DEEPSEEK_OUTPUT_USD_PER_M_TOKEN = 0.42
TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_+-]{1,}|[\u4e00-\u9fff]{2,}")
WORD_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+(?:[._'/-][A-Za-z0-9]+)*")
SINGLE_CJK_PATTERN = re.compile(r"[\u3400-\u9fff]")
WECHAT_EMOJI_SHORTCODE_PATTERN = re.compile(r"\[[\u3400-\u9fff]{1,12}\]")
_GROUP_NICKNAME_CACHE: dict[str, dict[str, str]] = {}
_ZHIPU_RATE_LOCK = threading.Lock()
_ZHIPU_LAST_CALL_AT = 0.0
WORD_CLOUD_STOPWORDS = {
    "我们", "你们", "他们", "这个", "那个", "真的", "感觉", "今天", "昨天", "现在", "就是",
    "然后", "因为", "所以", "还是", "已经", "一个", "一下", "没有", "不是", "怎么", "什么",
    "大家", "自己", "可以", "一下子", "哈哈", "哈哈哈", "啊啊", "一下儿", "还有", "觉得", "有点",
    "如果", "但是", "而且", "以及", "进行", "表示", "其实", "问题", "事情", "时候", "知道",
    "比较", "这种", "那个", "一下", "一下吧", "然后再", "一下呢", "真的很", "有氧运动聊天",
    "回复", "有氧", "运动", "聊天", "title", "msg", "xml", "appmsg", "des", "content",
    "quot", "amp", "lt", "gt", "nbsp", "http", "https",
    "摘要", "来源", "链接", "聊天记录", "转发", "分享", "内容", "包含",
}


def load_local_env() -> None:
    candidates = [
        Path.cwd() / ".env",
        SCRIPT_DIR / ".env",
        SCRIPT_DIR.parent / ".env",
    ]
    for env_path in candidates:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        break


load_local_env()


@dataclass
class StructuredMessage:
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
    id: str
    index: int
    start_ts: int
    end_ts: int
    start_time: str
    end_time: str
    message_count: int
    char_count: int
    messages: list[StructuredMessage]


def slugify(value: str) -> str:
    value = value.strip()
    value = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "group-insight"


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def normalize_text(value: str, max_len: int | None = None) -> str:
    value = re.sub(r"\s+", " ", value or "").strip()
    if max_len and len(value) > max_len:
        return value[: max_len - 3] + "..."
    return value


def strip_wechat_emoji_shortcodes(value: str) -> str:
    return WECHAT_EMOJI_SHORTCODE_PATTERN.sub(" ", value or "")


def collapse_text(value: str, max_len: int | None = None) -> str:
    return normalize_text(value, max_len=max_len)


def parse_xml_root_with_limit(content: str, max_len: int) -> ET.Element | None:
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


def parse_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def summarize_record_items(items: list[dict[str, str]], max_items: int = 4) -> str:
    snippets = []
    for item in items[:max_items]:
        name = collapse_text(item.get("name", ""), max_len=24)
        text = collapse_text(item.get("text", ""), max_len=60)
        if not text:
            continue
        snippets.append(f"{name}: {text}" if name else text)
    return "；".join(snippets)


def build_rich_card_preview(metadata: dict[str, Any], fallback_text: str) -> str:
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
    match = re.search(r'(?:"([^"]+)"|(我))\s*拍了拍\s*"([^"]+)"', title or "")
    if not match:
        return "", ""
    return (match.group(1) or match.group(2) or "").strip(), match.group(3).strip()


def parse_query_param(url: str, name: str) -> str:
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


def has_meaningful_rich_content(message: StructuredMessage) -> bool:
    metadata = message.metadata or {}
    kind = metadata.get("rich_kind", "")
    if kind == "link_card":
        return bool(metadata.get("title") or metadata.get("summary"))
    if kind == "merged_chat":
        return bool(metadata.get("title") or metadata.get("summary") or metadata.get("items"))
    return False


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def make_user_placeholder(sender_username: str) -> str:
    sender_username = (sender_username or "").strip()
    return f"[[user:{sender_username}]]" if sender_username else ""


def safe_json_loads(payload: str) -> Any:
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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class LLMClientProtocol:
    provider: str
    model: str

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        raise NotImplementedError


def is_rate_limit_error(exc: Exception) -> bool:
    text = str(exc or "").lower()
    return any(token in text for token in ["429", "1302", "rate limit", "速率限制"])


def build_stage_fingerprint(
    stage: str,
    input_payload: Any,
    *,
    dry_run: bool,
    model: str = "",
    system_prompt: str = "",
    user_prompt: str = "",
) -> str:
    envelope = {
        "stage": stage,
        "cache_version": STAGE_CACHE_VERSION,
        "dry_run": dry_run,
        "model": model,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "input": input_payload,
    }
    raw = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def get_stage_meta_path(output_path: Path) -> Path:
    return output_path.with_suffix(output_path.suffix + ".meta.json")


def load_cached_stage_output(output_path: Path, fingerprint: str) -> Any | None:
    meta_path = get_stage_meta_path(output_path)
    if not output_path.exists() or not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if meta.get("fingerprint") != fingerprint:
        return None
    return json.loads(output_path.read_text(encoding="utf-8"))


def write_stage_output(output_path: Path, payload: Any, fingerprint: str) -> None:
    write_json(output_path, payload)
    write_json(
        get_stage_meta_path(output_path),
        {
            "fingerprint": fingerprint,
            "cache_version": STAGE_CACHE_VERSION,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )


def find_local_browser_executable() -> str:
    candidates = [
        shutil.which("chrome.exe"),
        shutil.which("chrome"),
        shutil.which("msedge.exe"),
        shutil.which("msedge"),
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(candidate)
    return ""


def export_report_image_with_playwright(
    html_path: Path,
    image_path: Path,
    viewport_width: int = DEFAULT_REPORT_IMAGE_WIDTH,
    timeout_ms: int = DEFAULT_REPORT_IMAGE_TIMEOUT_MS,
) -> str:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return f"Playwright 不可用: {exc}"

    browser_executable = find_local_browser_executable()
    if not browser_executable:
        return "未找到可用的 Chrome/Edge 浏览器。"

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                executable_path=browser_executable,
                args=[
                    "--allow-file-access-from-files",
                    "--disable-web-security",
                ],
            )
            try:
                page = browser.new_page(
                    viewport={"width": viewport_width, "height": 1600},
                    device_scale_factor=2,
                )
                page.goto(html_path.resolve().as_uri(), wait_until="load", timeout=timeout_ms)
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
                page.evaluate(
                    """() => {
                        if (document.fonts && document.fonts.ready) {
                            return document.fonts.ready;
                        }
                        return Promise.resolve();
                    }"""
                )
                page.wait_for_timeout(300)
                page.screenshot(path=str(image_path), full_page=True, type="png")
            finally:
                browser.close()
        return ""
    except PlaywrightError as exc:
        return f"Playwright 渲染失败: {exc}"
    except Exception as exc:
        return f"浏览器截图失败: {exc}"


def export_report_image_with_chrome_cli(
    html_path: Path,
    image_path: Path,
    viewport_width: int = DEFAULT_REPORT_IMAGE_WIDTH,
    timeout_ms: int = DEFAULT_REPORT_IMAGE_TIMEOUT_MS,
) -> str:
    browser_executable = find_local_browser_executable()
    if not browser_executable:
        return "未找到可用的 Chrome/Edge 浏览器。"

    try:
        run(
            [
                browser_executable,
                "--headless=new",
                "--disable-gpu",
                "--hide-scrollbars",
                f"--window-size={viewport_width},1600",
                f"--screenshot={str(image_path)}",
                html_path.resolve().as_uri(),
            ],
            check=True,
            stdout=DEVNULL,
            stderr=DEVNULL,
            timeout=max(10, math.ceil(timeout_ms / 1000)),
        )
        return ""
    except CalledProcessError as exc:
        return f"Chrome CLI 截图失败: {exc}"
    except Exception as exc:
        return f"Chrome CLI 渲染失败: {exc}"


def export_report_image(
    html_path: Path,
    image_path: Path,
    viewport_width: int = DEFAULT_REPORT_IMAGE_WIDTH,
    timeout_ms: int = DEFAULT_REPORT_IMAGE_TIMEOUT_MS,
) -> str:
    error = export_report_image_with_playwright(
        html_path,
        image_path,
        viewport_width=viewport_width,
        timeout_ms=timeout_ms,
    )
    if not error and image_path.exists():
        return ""
    fallback_error = export_report_image_with_chrome_cli(
        html_path,
        image_path,
        viewport_width=viewport_width,
        timeout_ms=timeout_ms,
    )
    if not fallback_error and image_path.exists():
        return ""
    return fallback_error or error or "未知截图错误"


def send_report_png_to_chat(
    image_path: Path,
    message_lines: list[str] | None = None,
    friend_name: str = DEFAULT_FILEHELPER_NAME,
) -> None:
    pywechat_path = ROOT_DIR / "pywechat"
    if str(pywechat_path) not in sys.path:
        sys.path.insert(0, str(pywechat_path))

    from pyweixin.WeChatAuto import Files
    from pyweixin.WeChatTools import Tools

    if not image_path.exists():
        raise FileNotFoundError(f"图片不存在: {image_path}")
    if not Tools.is_weixin_running():
        raise RuntimeError("微信未运行，无法发送到文件传输助手。")

    normalized_messages = [normalize_text(line) for line in (message_lines or [])]
    normalized_messages = [line for line in normalized_messages if line]
    Files.send_files_to_friend(
        friend=friend_name,
        files=[str(image_path.resolve())],
        with_messages=bool(normalized_messages),
        messages=normalized_messages,
        messages_first=True,
        is_maximize=False,
        close_weixin=False,
    )


def has_cli_option(*names: str) -> bool:
    argv = sys.argv[1:]
    return any(arg == name or arg.startswith(f"{name}=") for arg in argv for name in names)


def compute_auto_time_range(cutoff: str = DEFAULT_AUTO_TIME_CUTOFF) -> tuple[str, str]:
    cutoff_time = datetime.strptime(cutoff, "%H:%M").time()
    end_dt = datetime.now().replace(
        hour=cutoff_time.hour,
        minute=cutoff_time.minute,
        second=0,
        microsecond=0,
    )
    start_dt = end_dt - timedelta(days=1)
    return (
        start_dt.strftime("%Y-%m-%d %H:%M"),
        end_dt.strftime("%Y-%m-%d %H:%M"),
    )


def split_send_targets(values: Any) -> list[str]:
    if values is None:
        raw_values: list[str] = []
    elif isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = [str(value) for value in values]

    targets: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        for part in re.split(r"[,，;；\n]+", raw_value or ""):
            target = normalize_text(part)
            if target and target not in seen:
                targets.append(target)
                seen.add(target)
    return targets


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, pos
        shift += 7
    raise ValueError("incomplete varint")


def _parse_proto_fields(data: bytes) -> list[tuple[int, int, Any]]:
    pos = 0
    fields: list[tuple[int, int, Any]] = []
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_no = tag >> 3
        wire_type = tag & 0x07
        if wire_type == 0:
            value, pos = _read_varint(data, pos)
        elif wire_type == 1:
            value = data[pos : pos + 8]
            pos += 8
        elif wire_type == 2:
            length, pos = _read_varint(data, pos)
            value = data[pos : pos + length]
            pos += length
        elif wire_type == 5:
            value = data[pos : pos + 4]
            pos += 4
        else:
            break
        fields.append((field_no, wire_type, value))
    return fields


def _looks_like_wechat_username(value: str) -> bool:
    return bool(
        value
        and (
            value.startswith("wxid_")
            or value.startswith("gh_")
            or value.endswith("@chatroom")
            or re.fullmatch(r"[A-Za-z0-9_]{4,}", value) is not None
        )
    )


def infer_sender_display_from_text(text: str) -> str:
    patterns = [
        r'^\[链接/文件\] "([^"]+)" 拍了拍',
        r'^\[系统\] "([^"]+)" 撤回了一条消息',
        r'^\[系统\] (.+?)发起了语音通话',
        r'^\[系统\] (.+?)结束了语音通话',
    ]
    for pattern in patterns:
        match = re.match(pattern, text or "")
        if match:
            return match.group(1).strip()
    return ""


def _collect_group_nicknames(blob: bytes, mapping: dict[str, str], depth: int = 0) -> None:
    if not blob or depth > 6:
        return
    try:
        fields = _parse_proto_fields(blob)
    except Exception:
        return

    string_fields: dict[int, str] = {}
    nested_payloads: list[bytes] = []
    for field_no, wire_type, value in fields:
        if wire_type != 2:
            continue
        if isinstance(value, bytes):
            try:
                decoded = value.decode("utf-8")
            except UnicodeDecodeError:
                decoded = ""
            if decoded:
                string_fields[field_no] = decoded
            nested_payloads.append(value)

    username = string_fields.get(1, "").strip()
    nickname = string_fields.get(2, "").strip()
    if _looks_like_wechat_username(username) and nickname:
        mapping[username] = nickname

    for payload in nested_payloads:
        _collect_group_nicknames(payload, mapping, depth + 1)


def get_group_nickname_map(chat_id: str) -> dict[str, str]:
    if chat_id in _GROUP_NICKNAME_CACHE:
        return _GROUP_NICKNAME_CACHE[chat_id]

    contact_db = SCRIPT_DIR / "decrypted" / "contact" / "contact.db"
    mapping: dict[str, str] = {}
    if not contact_db.exists():
        _GROUP_NICKNAME_CACHE[chat_id] = mapping
        return mapping

    conn = wechat_mcp.sqlite3.connect(str(contact_db))
    try:
        row = conn.execute(
            "SELECT ext_buffer FROM chat_room WHERE username = ?",
            (chat_id,),
        ).fetchone()
    finally:
        conn.close()

    if row and row[0]:
        _collect_group_nicknames(row[0], mapping)

    _GROUP_NICKNAME_CACHE[chat_id] = mapping
    return mapping


def resolve_sender_identity(
    ctx: dict[str, Any],
    names: dict[str, str],
    group_nicknames: dict[str, str],
    id_to_username: dict[int, str],
    real_sender_id: int,
    sender_from_content: str,
    text: str,
) -> tuple[str, str]:
    sender_username = id_to_username.get(real_sender_id, "") or sender_from_content or ""

    if sender_username:
        if ctx["is_group"]:
            if sender_username in group_nicknames:
                return sender_username, group_nicknames[sender_username]
        return sender_username, names.get(sender_username, sender_username)

    if ctx["is_group"]:
        inferred_display = infer_sender_display_from_text(text)
        if inferred_display:
            return "", inferred_display
        return "", "unknown"
    return "", ctx["display_name"] if not ctx["is_group"] else "unknown"


def resolve_member_display_name(
    username: str,
    names: dict[str, str],
    group_nicknames: dict[str, str],
    fallback: str = "",
) -> str:
    username = (username or "").strip()
    fallback = (fallback or "").strip()
    if not username:
        return fallback
    return group_nicknames.get(username) or names.get(username) or fallback or username


def is_resolved_member_display(username: str, display_name: str) -> bool:
    username = (username or "").strip()
    display_name = (display_name or "").strip()
    if not username or not display_name or display_name == username:
        return False
    if display_name.startswith(("wxid_", "gh_")) or display_name.endswith("@chatroom"):
        return False
    return True


def collect_member_aliases_from_messages(messages: list[StructuredMessage]) -> dict[str, str]:
    aliases: dict[str, str] = {}

    def add_alias(username: str, display_name: str) -> None:
        username = (username or "").strip()
        display_name = (display_name or "").strip()
        if is_resolved_member_display(username, display_name):
            aliases[username] = display_name

    for message in messages:
        metadata = message.metadata or {}
        add_alias(message.sender_username, message.sender)
        for username_key, name_key in (
            ("reply_to_username", "reply_to_name"),
            ("pat_from_username", "pat_from_name"),
            ("pat_to_username", "pat_to_name"),
            ("redpacket_sender_username", "redpacket_sender_name"),
            ("redpacket_receiver_username", "redpacket_receiver_name"),
        ):
            add_alias(metadata.get(username_key, ""), metadata.get(name_key, ""))
    return aliases


def enrich_interaction_metadata(
    metadata: dict[str, Any],
    names: dict[str, str],
    group_nicknames: dict[str, str],
) -> dict[str, Any]:
    if not metadata:
        return metadata

    kind = metadata.get("interaction_kind", "")
    if kind == "reply":
        metadata["reply_to_name"] = resolve_member_display_name(
            metadata.get("reply_to_username", ""),
            names,
            group_nicknames,
            metadata.get("reply_to_name", ""),
        )
    elif kind == "pat":
        metadata["pat_from_name"] = resolve_member_display_name(
            metadata.get("pat_from_username", ""),
            names,
            group_nicknames,
            metadata.get("pat_from_name", ""),
        )
        metadata["pat_to_name"] = resolve_member_display_name(
            metadata.get("pat_to_username", ""),
            names,
            group_nicknames,
            metadata.get("pat_to_name", ""),
        )
    elif kind in {"direct_redpacket", "redpacket"}:
        metadata["redpacket_sender_name"] = resolve_member_display_name(
            metadata.get("redpacket_sender_username", ""),
            names,
            group_nicknames,
            metadata.get("redpacket_sender_name", ""),
        )
        metadata["redpacket_receiver_name"] = resolve_member_display_name(
            metadata.get("redpacket_receiver_username", ""),
            names,
            group_nicknames,
            metadata.get("redpacket_receiver_name", ""),
        )
    return metadata


def extract_topic_tokens(text: str) -> set[str]:
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


def estimate_message_tokens(message: StructuredMessage) -> int:
    base = estimate_text_tokens(message.text)
    sender_cost = max(1, math.ceil(len(message.sender) / 3))
    return base + sender_cost + 8


def estimate_deepseek_cost_usd(
    input_tokens: int,
    output_tokens: int,
    *,
    cache_hit: bool = False,
) -> float:
    input_rate = (
        DEEPSEEK_INPUT_CACHE_HIT_USD_PER_M_TOKEN
        if cache_hit
        else DEEPSEEK_INPUT_CACHE_MISS_USD_PER_M_TOKEN
    )
    return (
        (input_tokens * input_rate)
        + (output_tokens * DEEPSEEK_OUTPUT_USD_PER_M_TOKEN)
    ) / 1_000_000


def estimate_deepseek_usage_cost_usd(usage: dict[str, Any]) -> float:
    hit_tokens = int(usage.get("prompt_cache_hit_tokens") or 0)
    miss_tokens = int(usage.get("prompt_cache_miss_tokens") or 0)
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    if not hit_tokens and not miss_tokens:
        miss_tokens = prompt_tokens
    return (
        (hit_tokens * DEEPSEEK_INPUT_CACHE_HIT_USD_PER_M_TOKEN)
        + (miss_tokens * DEEPSEEK_INPUT_CACHE_MISS_USD_PER_M_TOKEN)
        + (completion_tokens * DEEPSEEK_OUTPUT_USD_PER_M_TOKEN)
    ) / 1_000_000


def format_usd(value: float) -> str:
    if value < 0.0001:
        return f"${value:.6f}"
    if value < 0.01:
        return f"${value:.5f}"
    return f"${value:.4f}"


def estimate_prompt_tokens(system_prompt: str, user_prompt: str) -> int:
    return estimate_text_tokens(system_prompt) + estimate_text_tokens(user_prompt)


def direct_final_max_tokens_for_client(
    client: LLMClientProtocol | None,
    system_prompt: str,
    user_prompt: str,
    requested_max_tokens: int,
) -> int:
    if client is None or client.provider != "deepseek":
        return requested_max_tokens
    prompt_tokens = estimate_prompt_tokens(system_prompt, user_prompt)
    available = DEEPSEEK_CONTEXT_WINDOW_TOKENS - prompt_tokens - 1024
    if available < DEFAULT_DIRECT_FINAL_MIN_TOKENS:
        return requested_max_tokens
    return max(DEFAULT_DIRECT_FINAL_MIN_TOKENS, min(requested_max_tokens, available))


def parse_context_length_error(value: str) -> tuple[int, int, int] | None:
    match = re.search(
        r"maximum context length is (\d+) tokens\. However, you requested (\d+) tokens \((\d+) in the messages, (\d+) in the completion\)",
        value,
    )
    if not match:
        return None
    return tuple(int(match.group(index)) for index in range(1, 5))  # type: ignore[return-value]


def log_llm_request_estimate(
    stage: str,
    client: LLMClientProtocol | None,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
) -> None:
    input_tokens = estimate_prompt_tokens(system_prompt, user_prompt)
    provider = client.provider if client else "none"
    model = client.model if client else ""
    message = (
        f"[LLMEstimate] {stage} provider={provider}"
        f"{('/' + model) if model else ''} "
        f"input~{input_tokens} tokens, output<= {max_tokens} tokens"
    )
    if provider == "deepseek":
        miss_cost = estimate_deepseek_cost_usd(input_tokens, max_tokens, cache_hit=False)
        hit_cost = estimate_deepseek_cost_usd(input_tokens, max_tokens, cache_hit=True)
        message += (
            f", DeepSeek cost~{format_usd(miss_cost)}"
            f" (cache hit~{format_usd(hit_cost)})"
        )
    else:
        message += ", cost estimate unavailable for this provider"
    print(message, flush=True)


def estimate_chunk_payload_bytes(chunk: MessageChunk) -> int:
    return len(json.dumps(chunk_payload(chunk), ensure_ascii=False).encode("utf-8"))


def extract_word_cloud_terms(messages: list[StructuredMessage], top_n: int = 40) -> list[dict[str, Any]]:
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


def build_time_segment_breakdown(messages: list[StructuredMessage]) -> list[dict[str, Any]]:
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


def build_chunk(index: int, messages: list[StructuredMessage]) -> MessageChunk:
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


def topic_similarity(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0


def is_substantive_message(message: StructuredMessage) -> bool:
    if message.sender == "unknown":
        return False
    if message.msg_type in {"系统"}:
        return False
    return True


def classify_message_category(message: StructuredMessage) -> str:
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
    if message.sender == "unknown":
        return False
    category = classify_message_category(message)
    if category in {"link_card", "merged_chat"}:
        return has_meaningful_rich_content(message)
    return category in {"text", "reply", "emoji", "image", "voice", "video"}


def is_analysis_message(message: StructuredMessage) -> bool:
    return is_effective_conversation_message(message)


def fetch_structured_messages(
    chat_ref: str,
    start_time: str,
    end_time: str,
    batch_size: int = 500,
) -> tuple[dict[str, Any], list[StructuredMessage]]:
    ctx = wechat_mcp._resolve_chat_context(chat_ref)
    if not ctx:
        raise ValueError(f"找不到聊天对象: {chat_ref}")
    if not ctx["message_tables"]:
        raise ValueError(f"{ctx['display_name']} 没有可查询的消息表")

    start_ts, end_ts = wechat_mcp._parse_time_range(start_time, end_time)
    names = wechat_mcp.get_contact_names()
    group_nicknames = get_group_nickname_map(ctx["username"]) if ctx["is_group"] else {}
    collected: list[StructuredMessage] = []
    seen_ids: set[str] = set()
    dedupe_fingerprints: set[tuple[Any, ...]] = set()

    for table_ctx in wechat_mcp._iter_table_contexts(ctx):
        conn = wechat_mcp.sqlite3.connect(table_ctx["db_path"])
        try:
            id_to_username = wechat_mcp._load_name2id_maps(conn)
            fetch_offset = 0
            while True:
                rows = wechat_mcp._query_messages(
                    conn,
                    table_ctx["table_name"],
                    start_ts=start_ts,
                    end_ts=end_ts,
                    limit=batch_size,
                    offset=fetch_offset,
                )
                if not rows:
                    break
                fetch_offset += len(rows)

                for row in rows:
                    local_id, local_type, create_time, real_sender_id, content, ct = row
                    content = wechat_mcp._decompress_content(content, ct)
                    if content is None:
                        content = "(无法解压)"
                    metadata = extract_rich_message_metadata(
                        content,
                        local_type,
                        table_ctx["is_group"],
                    )
                    sender, text = wechat_mcp._format_message_text(
                        local_id,
                        local_type,
                        content,
                        table_ctx["is_group"],
                        table_ctx["username"],
                        table_ctx["display_name"],
                        names,
                    )
                    if metadata.get("analysis_text"):
                        text = metadata["analysis_text"]
                    sender_label = wechat_mcp._resolve_sender_label(
                        real_sender_id,
                        sender,
                        table_ctx["is_group"],
                        table_ctx["username"],
                        table_ctx["display_name"],
                        names,
                        id_to_username,
                    )
                    normalized_text = normalize_text(text or "(无内容)", max_len=MAX_LINE_TEXT_LEN)
                    sender_username, sender_display = resolve_sender_identity(
                        table_ctx,
                        names,
                        group_nicknames,
                        id_to_username,
                        real_sender_id,
                        sender,
                        normalized_text,
                    )
                    metadata = enrich_interaction_metadata(metadata, names, group_nicknames)
                    fingerprint = (
                        ctx["username"],
                        create_time,
                        sender_username or sender_label or sender_display,
                        wechat_mcp.format_msg_type(local_type),
                        normalized_text,
                    )
                    if fingerprint in dedupe_fingerprints:
                        continue
                    dedupe_fingerprints.add(fingerprint)

                    message_id_seed = (
                        f"{ctx['username']}|{create_time}|{sender_username}|{local_type}|{normalized_text}"
                    )
                    message_id = "m_" + hashlib.sha1(message_id_seed.encode("utf-8")).hexdigest()[:16]
                    if message_id in seen_ids:
                        continue
                    seen_ids.add(message_id)
                    collected.append(
                        StructuredMessage(
                            id=message_id,
                            local_id=local_id,
                            timestamp=create_time,
                            time=format_ts(create_time),
                            sender_username=sender_username or "",
                            sender=sender_display or sender_label or "unknown",
                            text=normalized_text,
                            msg_type=wechat_mcp.format_msg_type(local_type),
                            chat_id=ctx["username"],
                            chat_name=ctx["display_name"],
                            table_name=table_ctx["table_name"],
                            metadata=metadata,
                        )
                    )

                if len(rows) < batch_size:
                    break
        finally:
            conn.close()

    member_aliases = {**group_nicknames, **collect_member_aliases_from_messages(collected)}
    for message in collected:
        alias = member_aliases.get(message.sender_username or "")
        if alias and not is_resolved_member_display(message.sender_username, message.sender):
            message.sender = alias

    collected.sort(key=lambda item: (item.timestamp, item.local_id, item.id))
    return ctx, collected


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
    direct_token_threshold: int,
    direct_max_bytes: int,
) -> tuple[list[MessageChunk], dict[str, Any]]:
    analysis_messages = [message for message in messages if is_analysis_message(message)]
    if not analysis_messages:
        return [], {
            "strategy": "range-first",
            "direct_token_threshold": direct_token_threshold,
            "direct_max_bytes": direct_max_bytes,
            "analysis_message_count": 0,
            "estimated_tokens": 0,
            "estimated_direct_payload_bytes": 0,
            "mode": "empty",
            "range_direct": False,
            "shard_count": 0,
        }

    estimated_tokens = sum(estimate_message_tokens(message) for message in analysis_messages)
    direct_chunk = build_chunk(1, analysis_messages)
    direct_payload_bytes = estimate_chunk_payload_bytes(direct_chunk)
    byte_limit_allows_direct = direct_max_bytes <= 0 or direct_payload_bytes <= direct_max_bytes
    if estimated_tokens <= direct_token_threshold and byte_limit_allows_direct:
        chunk = direct_chunk
        return [chunk], {
            "strategy": "range-first",
            "direct_token_threshold": direct_token_threshold,
            "direct_max_bytes": direct_max_bytes,
            "analysis_message_count": len(analysis_messages),
            "estimated_tokens": estimated_tokens,
            "estimated_direct_payload_bytes": direct_payload_bytes,
            "mode": "direct_range",
            "range_direct": True,
            "shard_count": 1,
            "range": {
                "start": analysis_messages[0].time,
                "end": analysis_messages[-1].time,
            },
        }

    return build_sharded_range_chunks(
        messages,
        max_messages=max_messages,
        max_chars=max_chars,
        max_minutes=max_minutes,
        hard_gap_minutes=hard_gap_minutes,
        soft_gap_minutes=soft_gap_minutes,
        low_similarity_threshold=low_similarity_threshold,
        min_chunk_messages=min_chunk_messages,
        direct_token_threshold=direct_token_threshold,
        direct_max_bytes=direct_max_bytes,
        estimated_direct_payload_bytes=direct_payload_bytes,
    )


def build_sharded_range_chunks(
    messages: list[StructuredMessage],
    max_messages: int,
    max_chars: int,
    max_minutes: int,
    hard_gap_minutes: int,
    soft_gap_minutes: int,
    low_similarity_threshold: float,
    min_chunk_messages: int,
    direct_token_threshold: int,
    direct_max_bytes: int,
    fallback_reason: str = "",
    estimated_direct_payload_bytes: int = 0,
) -> tuple[list[MessageChunk], dict[str, Any]]:
    analysis_messages = [message for message in messages if is_analysis_message(message)]
    if not analysis_messages:
        return [], {
            "strategy": "range-first",
            "direct_token_threshold": direct_token_threshold,
            "direct_max_bytes": direct_max_bytes,
            "analysis_message_count": 0,
            "estimated_tokens": 0,
            "estimated_direct_payload_bytes": 0,
            "mode": "empty",
            "range_direct": False,
            "shard_count": 0,
        }

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

    plan = {
        "strategy": "range-first",
        "direct_token_threshold": direct_token_threshold,
        "direct_max_bytes": direct_max_bytes,
        "analysis_message_count": len(analysis_messages),
        "estimated_tokens": estimated_tokens,
        "estimated_direct_payload_bytes": estimated_direct_payload_bytes,
        "mode": "sharded_range",
        "range_direct": False,
        "shard_count": len(chunks),
        "range": {
            "start": analysis_messages[0].time,
            "end": analysis_messages[-1].time,
        },
    }
    if fallback_reason:
        plan["fallback_reason"] = fallback_reason
        plan["fallback_from_direct_range"] = True
    return chunks, plan


def estimate_reduce_call_count(item_count: int, fan_in: int) -> int:
    fan_in = max(2, fan_in)
    total = 0
    current = item_count
    while current > fan_in:
        groups = math.ceil(current / fan_in)
        total += groups
        current = groups
    return total


def build_topic_retry_chunks(chunk: MessageChunk) -> list[MessageChunk]:
    return build_chunks(
        chunk.messages,
        max_messages=min(220, max(80, chunk.message_count // 2)),
        max_chars=min(12000, max(5000, chunk.char_count // 2)),
        max_minutes=120,
        hard_gap_minutes=45,
        soft_gap_minutes=10,
        low_similarity_threshold=0.14,
        min_chunk_messages=10,
    )


def analyze_failed_chunk_by_topics(
    chunk: MessageChunk,
    chat_name: str,
    client: LLMClientProtocol | None,
) -> dict[str, Any]:
    if client is None:
        return fallback_map_analysis(chunk)

    subchunks = build_topic_retry_chunks(chunk)
    if len(subchunks) <= 1:
        return fallback_map_analysis(chunk)

    sub_results: list[dict[str, Any]] = []
    for index, subchunk in enumerate(subchunks, start=1):
        subchunk.id = f"{chunk.id}-topic-{index:02d}"
        subchunk.index = index
        system_prompt, user_prompt = build_map_prompts(chat_name, subchunk)
        try:
            log_llm_request_estimate(f"topic-map:{subchunk.id}", client, system_prompt, user_prompt, 4096)
            result = client.chat_json(system_prompt, user_prompt, max_tokens=4096, temperature=0.2)
        except Exception as sub_exc:
            print(f"[TopicMapFallback] {subchunk.id} -> {sub_exc}")
            result = fallback_map_analysis(subchunk)
        result.setdefault("shard_id", subchunk.id)
        result.setdefault("time_range", {"start": subchunk.start_time, "end": subchunk.end_time})
        sub_results.append(result)

    try:
        system_prompt, user_prompt = build_reduce_prompts(f"{chunk.id}-topic-merge", sub_results)
        log_llm_request_estimate(f"topic-reduce:{chunk.id}", client, system_prompt, user_prompt, 4096)
        merged = client.chat_json(system_prompt, user_prompt, max_tokens=4096, temperature=0.2)
    except Exception as merge_exc:
        print(f"[TopicReduceFallback] {chunk.id} -> {merge_exc}")
        merged = fallback_reduce_bundle(f"{chunk.id}-topic-merge", sub_results)

    merged["shard_id"] = chunk.id
    merged["time_range"] = {"start": chunk.start_time, "end": chunk.end_time}
    merged.setdefault("summary", f"{len(sub_results)} 个主题簇的合并摘要。")
    return merged


def normalize_rank_name(name: str) -> str:
    name = collapse_text(name or "", max_len=80)
    return "" if name in {"", "unknown"} else name


def ranked_counter(counter: Counter[str], limit: int = 10) -> list[dict[str, Any]]:
    items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [
        {"rank": index, "name": name, "count": count}
        for index, (name, count) in enumerate(items, start=1)
    ]


def build_interaction_rankings(messages: list[StructuredMessage]) -> dict[str, list[dict[str, Any]]]:
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


def build_local_stats(messages: list[StructuredMessage]) -> dict[str, Any]:
    category_labels = get_message_category_labels()
    category_counts = Counter(classify_message_category(message) for message in messages)
    substantive_messages = [message for message in messages if is_substantive_message(message)]
    effective_messages = [message for message in messages if is_effective_conversation_message(message)]
    member_aliases = collect_member_aliases_from_messages(messages)

    def message_sender_display(message: StructuredMessage) -> str:
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


def serialize_messages(messages: list[StructuredMessage]) -> list[dict[str, Any]]:
    return [asdict(message) for message in messages]


def get_chunk_topic_keywords(messages: list[StructuredMessage], top_n: int = 12) -> list[str]:
    counter: Counter[str] = Counter()
    for message in messages:
        if not is_analysis_message(message):
            continue
        counter.update(extract_topic_tokens(message.text))
    return [token for token, _ in counter.most_common(top_n)]


def chunk_payload(chunk: MessageChunk) -> dict[str, Any]:
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


def compact_direct_chunk_payload(chunk: MessageChunk) -> dict[str, Any]:
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


def compact_prompt_stats(stats: dict[str, Any]) -> dict[str, Any]:
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


def indexed_analysis_messages(chunk: MessageChunk) -> list[tuple[int, StructuredMessage]]:
    return [
        (index, message)
        for index, message in enumerate(
            [item for item in chunk.messages if is_analysis_message(item)],
            start=1,
        )
    ]


def compact_topic_index_payload(chunk: MessageChunk) -> dict[str, Any]:
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


class DeepSeekClient(LLMClientProtocol):
    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_DEEPSEEK_MODEL,
        api_url: str = DEFAULT_API_URL,
        timeout: int = 180,
        max_retries: int = 3,
    ) -> None:
        self.provider = "deepseek"
        self.api_key = api_key
        self.model = model
        self.api_url = api_url
        self.timeout = timeout
        self.max_retries = max_retries

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                content = self._request_content(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                if not content.strip():
                    raise ValueError("DeepSeek 返回空内容")
                return safe_json_loads(content)
            except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, ValueError, RuntimeError) as exc:
                last_error = exc
                if isinstance(exc, json.JSONDecodeError):
                    try:
                        repaired = self._repair_json(
                            broken_json=content if "content" in locals() else "",
                            max_tokens=max_tokens,
                        )
                        if repaired:
                            return safe_json_loads(repaired)
                    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, ValueError, RuntimeError) as repair_exc:
                        last_error = repair_exc
                if attempt >= self.max_retries:
                    break
                time.sleep(attempt * 2)
        raise RuntimeError(f"DeepSeek 调用失败: {last_error}") from last_error

    def _request_content(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.api_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            if detail:
                raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {detail[:1000]}") from exc
            raise
        parsed = json.loads(raw)
        usage = parsed.get("usage", {})
        if usage:
            cost = estimate_deepseek_usage_cost_usd(usage)
            print(
                "[LLMUsage] deepseek "
                f"prompt={usage.get('prompt_tokens', 0)} "
                f"cache_hit={usage.get('prompt_cache_hit_tokens', 0)} "
                f"cache_miss={usage.get('prompt_cache_miss_tokens', 0)} "
                f"completion={usage.get('completion_tokens', 0)} "
                f"cost~{format_usd(cost)}",
                flush=True,
            )
        return (
            parsed.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )

    def _repair_json(self, broken_json: str, max_tokens: int) -> str:
        candidate = extract_json_object(broken_json) or broken_json
        candidate = candidate.strip()
        if not candidate:
            raise ValueError("empty broken json")

        repair_system_prompt = """
你是一个 JSON 修复器。你会收到一段损坏或截断的 JSON。

要求：
1. 只输出一个合法 JSON 对象。
2. 不要添加 markdown、解释、注释。
3. 尽量保留原字段和原值。
4. 若局部截断无法恢复，删除损坏字段或把该字段改为空数组/空字符串，但必须保持整体 JSON 合法。
5. 所有括号、引号、逗号都必须正确闭合。
""".strip()
        repair_user_prompt = f"""
请把下面这段损坏 JSON 修复成合法 JSON 对象，只输出修复后的 JSON：

{candidate}
""".strip()
        return self._request_content(
            system_prompt=repair_system_prompt,
            user_prompt=repair_user_prompt,
            max_tokens=min(max_tokens, 4096),
            temperature=0.0,
        )


class ZhipuClient(LLMClientProtocol):
    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_ZHIPU_MODEL,
        max_retries: int = 8,
        min_interval_seconds: float = 1.2,
        rate_limit_base_delay: float = 4.0,
        rate_limit_max_delay: float = 90.0,
    ) -> None:
        try:
            from zhipuai_tool import create_zhipu_client
        except Exception as exc:
            raise RuntimeError(f"无法导入 zhipuai_tool: {exc}") from exc

        self.provider = "zhipu"
        self.model = model
        self.max_retries = max_retries
        self.min_interval_seconds = min_interval_seconds
        self.rate_limit_base_delay = rate_limit_base_delay
        self.rate_limit_max_delay = rate_limit_max_delay
        self.client = create_zhipu_client(api_key, max_retries=0)
        self.client.MODEL_TEXT = model

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        global _ZHIPU_LAST_CALL_AT
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with _ZHIPU_RATE_LOCK:
                    now = time.time()
                    wait_seconds = self.min_interval_seconds - (now - _ZHIPU_LAST_CALL_AT)
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)
                    _ZHIPU_LAST_CALL_AT = time.time()
                result = self.client.text_chat(
                    prompt=user_prompt,
                    messages=[{"role": "system", "content": system_prompt}],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                return safe_json_loads(result.get("content", ""))
            except (json.JSONDecodeError, ValueError, RuntimeError, TypeError) as exc:
                last_error = exc
                if is_rate_limit_error(exc) and attempt < self.max_retries:
                    delay = min(self.rate_limit_max_delay, self.rate_limit_base_delay * (2 ** (attempt - 1)))
                    print(f"[ZhipuBackoff] rate limited, waiting {delay:.1f}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(delay)
                    continue
            except Exception as exc:
                last_error = exc
                if is_rate_limit_error(exc) and attempt < self.max_retries:
                    delay = min(self.rate_limit_max_delay, self.rate_limit_base_delay * (2 ** (attempt - 1)))
                    print(f"[ZhipuBackoff] rate limited, waiting {delay:.1f}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(delay)
                    continue
        raise RuntimeError(f"Zhipu 调用失败: {last_error}")


MAP_SCHEMA_EXAMPLE = {
    "shard_id": "shard-001",
    "time_range": {"start": "2026-04-08 09:00", "end": "2026-04-08 10:30"},
    "summary": "该时间片主要围绕一个或多个连续话题展开。",
    "theme_cards": [
        {
            "title": "主题标题",
            "summary": "该主题在这个时间片里的简明总结。",
            "evidence_ids": ["m_xxx", "m_yyy"],
        }
    ],
    "highlight_sections": [
        {
            "title": "一个可单独成段的话题簇标题",
            "start_time": "2026-04-08 09:12",
            "end_time": "2026-04-08 09:35",
            "summary": "这个话题簇发生了什么。",
            "bullets": ["关键点1", "关键点2"],
            "evidence_ids": ["m_xxx", "m_yyy"],
        }
    ],
    "participant_notes": [
        {
            "name": "[[user:wxid_xxx]]",
            "observation": "[[user:wxid_xxx]] 在本片段中的作用或表现。",
            "evidence_ids": ["m_xxx"],
        }
    ],
    "quotes": [
        {
            "speaker": "[[user:wxid_xxx]]",
            "time": "2026-04-08 09:23",
            "quote": "一句值得引用的话",
            "message_id": "m_xxx",
            "why_it_matters": "为什么值得引用",
        }
    ],
    "decisions": [{"content": "已达成的结论", "evidence_ids": ["m_xxx"]}],
    "action_items": [
        {
            "owner": "[[user:wxid_xxx]] 或留空",
            "task": "[[user:wxid_xxx]] 相关待办事项",
            "deadline": "时间或留空",
            "status_hint": "紧急/一般/观察",
            "evidence_ids": ["m_xxx"],
        }
    ],
    "open_questions": [{"question": "未解决的问题", "evidence_ids": ["m_xxx"]}],
    "mood": {
        "label": "活跃/理性/轻松/焦虑/冲突等",
        "reason": "判断依据",
        "evidence_ids": ["m_xxx"],
    },
}


REDUCE_SCHEMA_EXAMPLE = {
    "bundle_id": "reduce-001",
    "summary": "多片段合并后的摘要",
    "theme_cards": [
        {"title": "核心主题", "summary": "主题归纳", "source_refs": ["shard-001"]}
    ],
    "highlight_sections": [
        {
            "title": "重要话题簇",
            "start_time": "2026-04-08 09:12",
            "end_time": "2026-04-08 10:30",
            "summary": "合并后的话题总结",
            "bullets": ["关键点1", "关键点2"],
            "source_refs": ["shard-001", "shard-002"],
        }
    ],
    "participant_notes": [
        {"name": "[[user:wxid_xxx]]", "observation": "[[user:wxid_xxx]] 的角色观察", "source_refs": ["shard-001"]}
    ],
    "quotes": [
        {
            "speaker": "[[user:wxid_xxx]]",
            "time": "2026-04-08 09:23",
            "quote": "一句代表性话语",
            "source_refs": ["shard-001"],
        }
    ],
    "decisions": [{"content": "结论", "source_refs": ["shard-001"]}],
    "action_items": [
        {
            "owner": "[[user:wxid_xxx]] 或留空",
            "task": "[[user:wxid_xxx]] 相关待办事项",
            "deadline": "时间或留空",
            "status_hint": "紧急/一般/观察",
            "source_refs": ["shard-001"],
        }
    ],
    "open_questions": [{"question": "未决问题", "source_refs": ["shard-001"]}],
    "risk_flags": ["潜在风险或争议点"],
    "mood": {"label": "整体氛围", "reason": "原因", "source_refs": ["shard-001"]},
}


FINAL_REPORT_SCHEMA_EXAMPLE = {
    "headline": "一句报告总标题",
    "tagline": "一句短副标题",
    "lead_summary": "1-2 段的默认总结",
    "theme_cards": [
        {"title": "主题一", "summary": "适合展示在摘要卡片中的简短文本"}
    ],
    "sections": [
        {
            "title": "话题簇标题",
            "start_time": "2026-04-08 09:12",
            "end_time": "2026-04-08 10:30",
            "summary": "这个话题簇的核心结论",
            "bullets": ["要点1", "要点2"],
            "takeaway": "一句点评或收束",
        }
    ],
    "participant_insights": [
        {"name": "[[user:wxid_xxx]]", "insight": "[[user:wxid_xxx]] 的关键作用或状态"}
    ],
    "quotes": [
        {
            "speaker": "[[user:wxid_xxx]]",
            "time": "2026-04-08 09:23",
            "quote": "一句可放进报告的原话",
            "why_it_matters": "为什么重要",
        }
    ],
    "decisions": ["已明确的结论"],
    "action_items": [
        {"owner": "[[user:wxid_xxx]] 或留空", "task": "[[user:wxid_xxx]] 相关行动项", "deadline": "时间或留空"}
    ],
    "open_questions": ["未解决的问题"],
    "risk_flags": ["需要继续观察的风险或争议"],
    "mood": {"label": "整体氛围", "reason": "判断依据"},
}


def build_map_prompts(chat_name: str, chunk: MessageChunk) -> tuple[str, str]:
    system_prompt = f"""
你是一个严谨的群聊分析师。请基于用户提供的群聊时间片消息做结构化分析，并只输出 json。

要求：
1. 只基于提供的消息内容，不要补充外部事实。
2. 所有数组字段都必须存在，没内容时返回空数组。
3. evidence_ids 必须引用输入消息中的 id。
4. 主题和亮点要偏“可直接上报表”的表达，不要写成学术论文。
5. 允许保留轻度口语化，但不能夸张、不能编造。
6. 严格控制输出长度，宁可少写，不要写长段落。
7. theme_cards 最多 3 条，highlight_sections 最多 4 条，participant_notes 最多 4 条。
8. quotes 最多 2 条，decisions/action_items/open_questions 各最多 3 条。
9. action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
10. 每个 highlight_sections.bullets 最多 2 条。
11. highlight_sections 表示“话题簇”而不是机械时间切段；如果同一时间窗口里存在多个不同话题，可以拆成多个 sections，时间范围允许重叠。
12. 不要只写最显眼的主线，持续时间较短但消息量可观、内容明确的次级话题也要覆盖，避免遗漏例如运动分享、生活分享、工具讨论这类支线。
13. 输入里会提供 member_directory；提到具体成员时，请统一使用对应的 `[[user:sender_id]]` 占位符，不要直接输出昵称。

输出 json schema 示例：
{json.dumps(MAP_SCHEMA_EXAMPLE, ensure_ascii=False, indent=2)}
""".strip()
    user_prompt = f"""
请分析群聊“{chat_name}”的一个时间片，并输出严格 json。

时间片数据：
{json.dumps(chunk_payload(chunk), ensure_ascii=False, indent=2)}
""".strip()
    return system_prompt, user_prompt


def build_reduce_prompts(bundle_id: str, items: list[dict[str, Any]]) -> tuple[str, str]:
    system_prompt = f"""
你是一个群聊分析 reducer。你会收到多个 shard 分析结果或中间 reduce 结果，请将它们合并成一个更高层摘要，并只输出 json。

要求：
1. 只整合输入中的已有信息，不要引入外部信息。
2. 去重同类主题、同类结论和重复行动项。
3. highlight_sections 应按“话题簇”整理，不要机械按时间线硬切；如果多个话题的主要活跃区间重叠，允许时间范围重叠。
4. source_refs 必须引用输入里的 shard_id 或 bundle_id。
5. risk_flags 至少覆盖明显争议、风险、未落地事项；没有则返回空数组。
6. 严格控制输出长度，宁可少写，不要写长段落。
7. theme_cards 最多 4 条，highlight_sections 最多 6 条，participant_notes 最多 6 条。
8. quotes 最多 3 条，decisions/action_items/open_questions 各最多 4 条。
9. 合并时检查是否遗漏持续但相对次级的话题，不要只保留最热主线。
10. 如果输入覆盖多个明显不同的活跃区间，highlight_sections 至少为每个输入 shard/bundle 保留一个非重复的话题 section，除非其内容与其他输入完全重复。
11. 不要让结果出现长时间空洞；如果 source_refs 对应的输入在某一时间段内明显活跃，合并结果应覆盖该时段。
12. 如果输入里出现 `[[user:sender_id]]` 占位符，输出时保留该占位符，不要改写成昵称。

输出 json schema 示例：
{json.dumps(REDUCE_SCHEMA_EXAMPLE, ensure_ascii=False, indent=2)}
""".strip()
    user_prompt = f"""
请把以下多个群聊分析结果合并为一个中间 bundle，并输出严格 json。

目标 bundle_id: {bundle_id}

输入：
{json.dumps(items, ensure_ascii=False, indent=2)}
""".strip()
    return system_prompt, user_prompt


def build_final_prompts(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    bundles: list[dict[str, Any]],
) -> tuple[str, str]:
    system_prompt = f"""
你是一个中文群聊洞察报表编辑。你会收到本地统计数据和一组最终 reduce bundles，请产出适合日报/周报页面渲染的最终结构化结果，并只输出 json。

要求：
1. 只基于输入，不得补充不存在的数字。
2. 主题卡片应短、清晰、适合视觉卡片展示。
3. sections 是报告主体，表示“话题簇”而不是机械时间段；数量控制在 6-12 段。
4. 报表语言要像运营洞察报告，不要写成泛泛总结。
5. action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
6. 严格控制输出长度，宁可少写，不要把每条细节都展开。
7. theme_cards 最多 4 条，participant_insights 最多 6 条，quotes 最多 4 条。
8. sections 每段 bullets 最多 2 条。
9. 如果多个话题的主要活跃时间交叠，允许不同 sections 的 start_time / end_time 重叠，不要为了避免重叠而把不同主题强行糅合成一段。
10. 请覆盖当日所有明显成型的话题，不要遗漏持续时间较短但消息量可观的讨论，例如下午的运动分享、生活分享、工具讨论等。
11. 如果输入 bundles 覆盖多个明显不同的活跃区间，sections 至少为每个 bundle/shard 保留一个非重复 section，除非两个输入本质上是同一话题。
12. 不要生成大段时间空洞；若输入在某个中段时窗存在明显活跃讨论，最终 sections 应覆盖该时段。
13. 如果输入里的 bundles 使用 `[[user:sender_id]]` 占位符，最终输出请保留这些占位符，不要改写成昵称。

最终 json schema 示例：
{json.dumps(FINAL_REPORT_SCHEMA_EXAMPLE, ensure_ascii=False, indent=2)}
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”生成最终结构化报表 json。

统计区间：{start_time} ~ {end_time}

本地精确统计：
{json.dumps(stats, ensure_ascii=False, indent=2)}

最终 reduce 输入：
{json.dumps(bundles, ensure_ascii=False, indent=2)}
""".strip()
    return system_prompt, user_prompt


def build_direct_final_prompts(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
) -> tuple[str, str]:
    compact_stats = compact_prompt_stats(stats)
    system_prompt = """
你是一个中文群聊洞察报表编辑。你会收到本地统计数据和完整群聊消息，请直接产出适合日报/周报页面渲染的最终结构化结果，并只输出 json。

要求：
1. 只基于输入，不得补充不存在的数字。
2. 这是 direct_range 模式，请直接从原始消息提炼主题，不要先按连续时间片机械概括。
3. sections 是报告主体，表示“话题簇”而不是机械时间段；数量控制在 8-15 段，允许时间范围重叠。
4. 不要只保留抽象结论；保留关键人、具体事件、分歧点、工具/食物/运动/祝福等可复述细节。
5. 重复寒暄和刷屏内容可以合并，但持续时间较短且内容明确的话题也要覆盖。
6. sections 每段 bullets 最多 3 条；每条 bullet 应包含具体信息，不要写空泛评价。
7. theme_cards 最多 4 条，participant_insights 最多 8 条，quotes 最多 6 条。
8. action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
9. 消息中的 sender_ref 已是 `[[user:sender_id]]` 占位符；提到具体成员时保留该占位符，不要改写成昵称。
10. 输出必须是合法 JSON 对象，不要添加 markdown 或解释。
11. JSON 字段：headline, tagline, lead_summary, theme_cards, sections, participant_insights, quotes, decisions, action_items, open_questions, risk_flags, mood。
12. sections 字段：title, start_time, end_time, summary, bullets, takeaway。
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”生成最终结构化报表 json。

统计区间：{start_time} ~ {end_time}

紧凑统计：
{json.dumps(compact_stats, ensure_ascii=False, separators=(",", ":"))}

完整 direct_range 消息采用紧凑文本格式，字段顺序为：
time|sender_ref|message_type|text

{json.dumps(compact_direct_chunk_payload(chunk), ensure_ascii=False, separators=(",", ":"))}
""".strip()
    return system_prompt, user_prompt


def build_topic_plan_prompts(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
    max_topics: int,
) -> tuple[str, str]:
    compact_stats = compact_prompt_stats(stats)
    payload = compact_topic_index_payload(chunk)
    system_prompt = f"""
你是一个群聊主题聚类器。你会收到完整群聊消息的紧凑索引，请只输出 JSON。

任务：
1. 按“话题簇”聚类，而不是按连续时间段切片。
2. 同一时间段内可以有多个 topic；同一条消息也可以被多个 topic 引用。
3. 覆盖主要话题，也保留短但内容明确的支线话题。
4. 每个 topic 的 message_indexes 必须引用输入里的 idx，按相关度和时间顺序列出。
5. topic 数量控制在 8-{max_topics} 个；如果内容不足可以少于 8 个。
6. 输出 JSON 字段：topics。
7. 每个 topic 字段：topic_id, title, summary, message_indexes, start_time, end_time, priority。
8. priority 只能是 major 或 minor。
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”生成主题聚类计划。

统计区间：{start_time} ~ {end_time}

紧凑统计：
{json.dumps(compact_stats, ensure_ascii=False, separators=(",", ":"))}

消息格式：
idx|time|sender_ref|message_type|text

输入消息：
{json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}
""".strip()
    return system_prompt, user_prompt


def build_topic_section_prompts(
    chat_name: str,
    topic: dict[str, Any],
    messages: list[StructuredMessage],
) -> tuple[str, str]:
    payload = compact_topic_section_payload(topic, messages)
    system_prompt = """
你是一个群聊话题 section 分析器。你会收到一个 topic 的相关原始消息，请只输出 JSON。

要求：
1. 只分析当前 topic，不要扩展到无关话题。
2. 保留具体事件、分歧点、成员动作和可复述细节。
3. section 字段：title, start_time, end_time, summary, bullets, takeaway。
4. bullets 最多 3 条，必须具体。
5. quotes 最多 2 条，字段：speaker, time, quote, message_id, why_it_matters。
6. participant_insights 最多 3 条，字段：name, insight。
7. decisions/action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
8. 提到成员时保留 `[[user:sender_id]]` 占位符。
9. 输出 JSON 字段：topic_id, section, participant_insights, quotes, decisions, action_items, open_questions, risk_flags。
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”的这个 topic 生成一个详细 section。

输入格式：
message_id|time|sender_ref|message_type|text

topic 与相关消息：
{json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}
""".strip()
    return system_prompt, user_prompt


def parse_report_time(value: str) -> datetime | None:
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
    start_dt = parse_report_time(section.get("start_time", ""))
    end_dt = parse_report_time(section.get("end_time", ""))
    start_ts = start_dt.timestamp() if start_dt else float("inf")
    end_ts = end_dt.timestamp() if end_dt else float("inf")
    return start_ts, end_ts, section.get("title", "")


def dedupe_theme_cards(cards: list[dict[str, Any]], limit: int = 4) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for card in cards:
        title = normalize_text(card.get("title", ""), max_len=80)
        summary = normalize_text(card.get("summary", ""))
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
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for section in sorted(sections, key=section_sort_key):
        title = normalize_text(section.get("title", ""), max_len=100)
        start_time = (section.get("start_time", "") or "").strip()
        end_time = (section.get("end_time", "") or "").strip()
        summary = normalize_text(section.get("summary", ""), max_len=240)
        bullets = [
            normalize_text(item, max_len=120)
            for item in section.get("bullets", [])
            if normalize_text(item, max_len=120)
        ][:2]
        takeaway = normalize_text(section.get("takeaway", ""), max_len=160)
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


MAX_REPORT_SECTIONS = 15


def select_timeline_sections(sections: list[dict[str, Any]], limit: int = MAX_REPORT_SECTIONS) -> list[dict[str, Any]]:
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


SECTION_TOPIC_COVERAGE_THRESHOLD = 0.18


def section_topic_tokens(section: dict[str, Any]) -> set[str]:
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
    merged = dedupe_sections(report_sections)
    for section in bundle_sections:
        if not bundle_section_is_covered(merged, section):
            merged.append(section)
    return select_timeline_sections(dedupe_sections(merged), limit=limit)


def build_theme_cards_from_bundles(bundles: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    repaired = {
        "headline": normalize_text(report.get("headline", ""), max_len=80) or f"{chat_name} 群洞察报表",
        "tagline": normalize_text(report.get("tagline", ""), max_len=120) or f"{start_time} - {end_time}",
        "lead_summary": normalize_text(report.get("lead_summary", ""), max_len=800),
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


def run_map_stage(
    chunks: list[MessageChunk],
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
    max_workers: int,
    allow_fallback: bool = True,
) -> list[dict[str, Any]]:
    map_dir = ensure_dir(output_dir / "map")

    def analyze_chunk(chunk: MessageChunk) -> dict[str, Any]:
        chunk_input = chunk_payload(chunk)
        input_path = map_dir / f"{chunk.id}.input.json"
        output_path = map_dir / f"{chunk.id}.output.json"
        chat_name = chunk.messages[0].chat_name
        system_prompt = ""
        user_prompt = ""
        if not dry_run:
            system_prompt, user_prompt = build_map_prompts(chat_name, chunk)
        fingerprint = build_stage_fingerprint(
            "map",
            chunk_input,
            dry_run=dry_run,
            model=f"{client.provider}:{client.model}" if client else "",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        write_json(input_path, chunk_input)

        cached = load_cached_stage_output(output_path, fingerprint)
        if cached is not None:
            return cached

        if dry_run:
            result = fallback_map_analysis(chunk)
        else:
            if client is None:
                raise RuntimeError("LLM client 未初始化")
            try:
                log_llm_request_estimate(f"map:{chunk.id}", client, system_prompt, user_prompt, 4096)
                result = client.chat_json(system_prompt, user_prompt, max_tokens=4096, temperature=0.2)
            except Exception as exc:
                if not allow_fallback:
                    raise
                print(f"[MapFallback] {chunk.id} -> {exc}")
                if client is not None and client.provider == "zhipu" and is_rate_limit_error(exc):
                    result = fallback_map_analysis(chunk)
                else:
                    result = analyze_failed_chunk_by_topics(chunk, chat_name, client)

        write_stage_output(output_path, result, fingerprint)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(analyze_chunk, chunk) for chunk in chunks]
        results = [future.result() for future in futures]

    results.sort(key=lambda item: item.get("time_range", {}).get("start", ""))
    return results


def reduce_once(
    items: list[dict[str, Any]],
    output_dir: Path,
    round_index: int,
    fan_in: int,
    dry_run: bool,
    client: LLMClientProtocol | None,
) -> list[dict[str, Any]]:
    reduce_dir = ensure_dir(output_dir / "reduce" / f"round-{round_index:02d}")
    groups = [items[i : i + fan_in] for i in range(0, len(items), fan_in)]
    results: list[dict[str, Any]] = []

    for group_index, group_items in enumerate(groups, start=1):
        bundle_id = f"reduce-{round_index:02d}-{group_index:02d}"
        input_path = reduce_dir / f"{bundle_id}.input.json"
        output_path = reduce_dir / f"{bundle_id}.output.json"
        write_json(input_path, group_items)
        system_prompt = ""
        user_prompt = ""
        if not dry_run:
            system_prompt, user_prompt = build_reduce_prompts(bundle_id, group_items)
        fingerprint = build_stage_fingerprint(
            "reduce",
            {"bundle_id": bundle_id, "items": group_items},
            dry_run=dry_run,
            model=f"{client.provider}:{client.model}" if client else "",
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        cached = load_cached_stage_output(output_path, fingerprint)
        if cached is not None:
            results.append(cached)
            continue

        if dry_run:
            bundle = fallback_reduce_bundle(bundle_id, group_items)
        else:
            if client is None:
                raise RuntimeError("LLM client 未初始化")
            try:
                log_llm_request_estimate(f"reduce:{bundle_id}", client, system_prompt, user_prompt, 4096)
                bundle = client.chat_json(system_prompt, user_prompt, max_tokens=4096, temperature=0.2)
                bundle.setdefault("bundle_id", bundle_id)
            except Exception as exc:
                print(f"[ReduceFallback] {bundle_id} -> {exc}")
                bundle = fallback_reduce_bundle(bundle_id, group_items)

        write_stage_output(output_path, bundle, fingerprint)
        results.append(bundle)

    return results


def run_reduce_stage(
    map_results: list[dict[str, Any]],
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
    fan_in: int,
) -> list[dict[str, Any]]:
    current = map_results[:]
    round_index = 1
    while len(current) > fan_in:
        current = reduce_once(current, output_dir, round_index, fan_in, dry_run, client)
        round_index += 1
    return current


def run_final_stage(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    bundles: list[dict[str, Any]],
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
) -> dict[str, Any]:
    final_dir = ensure_dir(output_dir / "final")
    input_path = final_dir / "report.input.json"
    output_path = final_dir / "report.output.json"
    payload = {
        "chat_name": chat_name,
        "start_time": start_time,
        "end_time": end_time,
        "stats": stats,
        "bundles": bundles,
    }
    write_json(input_path, payload)
    system_prompt = ""
    user_prompt = ""
    if not dry_run:
        system_prompt, user_prompt = build_final_prompts(chat_name, start_time, end_time, stats, bundles)
    fingerprint = build_stage_fingerprint(
        "final",
        payload,
        dry_run=dry_run,
        model=f"{client.provider}:{client.model}" if client else "",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )

    cached = load_cached_stage_output(output_path, fingerprint)
    if cached is not None:
        return repair_final_report(cached, chat_name, start_time, end_time, stats, bundles)

    if dry_run:
        report = fallback_final_report(chat_name, start_time, end_time, stats, bundles)
    else:
        if client is None:
            raise RuntimeError("LLM client 未初始化")
        try:
            log_llm_request_estimate("final", client, system_prompt, user_prompt, 4096)
            report = client.chat_json(system_prompt, user_prompt, max_tokens=4096, temperature=0.2)
        except Exception as exc:
            print(f"[FinalFallback] {chat_name} -> {exc}")
            report = fallback_final_report(chat_name, start_time, end_time, stats, bundles)

    report = repair_final_report(report, chat_name, start_time, end_time, stats, bundles)
    write_stage_output(output_path, report, fingerprint)
    return report


def normalize_topic_plan(plan: dict[str, Any], chunk: MessageChunk, max_topics: int) -> list[dict[str, Any]]:
    indexed_messages = dict(indexed_analysis_messages(chunk))
    topics: list[dict[str, Any]] = []
    seen_topic_ids: set[str] = set()
    for raw_topic in plan.get("topics", []):
        raw_indexes = raw_topic.get("message_indexes", [])
        indexes: list[int] = []
        for value in raw_indexes:
            try:
                index = int(value)
            except (TypeError, ValueError):
                continue
            if index in indexed_messages and index not in indexes:
                indexes.append(index)
        if not indexes:
            continue
        messages = [indexed_messages[index] for index in indexes]
        title = normalize_text(raw_topic.get("title", ""), max_len=100)
        summary = normalize_text(raw_topic.get("summary", ""), max_len=240)
        if not title and not summary:
            continue
        topic_id = normalize_text(raw_topic.get("topic_id", ""), max_len=32) or f"t{len(topics) + 1:02d}"
        topic_id = re.sub(r"[^A-Za-z0-9_-]+", "-", topic_id).strip("-") or f"t{len(topics) + 1:02d}"
        if topic_id in seen_topic_ids:
            topic_id = f"{topic_id}-{len(topics) + 1:02d}"
        seen_topic_ids.add(topic_id)
        priority = normalize_text(raw_topic.get("priority", ""), max_len=12).lower()
        if priority not in {"major", "minor"}:
            priority = "major" if len(indexes) >= 8 else "minor"
        topics.append(
            {
                "topic_id": topic_id,
                "title": title or "话题",
                "summary": summary,
                "message_indexes": indexes,
                "start_time": raw_topic.get("start_time") or messages[0].time,
                "end_time": raw_topic.get("end_time") or messages[-1].time,
                "priority": priority,
            }
        )
        if len(topics) >= max_topics:
            break
    return topics


def select_topic_messages_for_prompt(messages: list[StructuredMessage], max_messages: int = 260) -> list[StructuredMessage]:
    if len(messages) <= max_messages:
        return messages
    if max_messages <= 2:
        return messages[:max_messages]
    selected_indexes = {0, len(messages) - 1}
    for slot in range(1, max_messages - 1):
        selected_indexes.add(round(slot * (len(messages) - 1) / (max_messages - 1)))
    return [messages[index] for index in sorted(selected_indexes)]


def merge_topic_outputs(topics: list[dict[str, Any]], topic_outputs: list[dict[str, Any]]) -> dict[str, Any]:
    topic_by_id = {topic["topic_id"]: topic for topic in topics}
    sections: list[dict[str, Any]] = []
    participant_insights: list[dict[str, Any]] = []
    quotes: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    action_items: list[dict[str, Any]] = []
    open_questions: list[dict[str, Any]] = []
    risk_flags: list[str] = []

    for output in topic_outputs:
        topic = topic_by_id.get(output.get("topic_id", ""), {})
        section = output.get("section", {}) if isinstance(output.get("section"), dict) else {}
        if section:
            sections.append(
                {
                    "title": normalize_text(section.get("title", "") or topic.get("title", ""), max_len=100),
                    "start_time": section.get("start_time") or topic.get("start_time", ""),
                    "end_time": section.get("end_time") or topic.get("end_time", ""),
                    "summary": normalize_text(section.get("summary", "") or topic.get("summary", ""), max_len=240),
                    "bullets": section.get("bullets", [])[:3],
                    "takeaway": normalize_text(section.get("takeaway", ""), max_len=160),
                }
            )
        participant_insights.extend(output.get("participant_insights", [])[:3])
        quotes.extend(output.get("quotes", [])[:2])
        decisions.extend(output.get("decisions", [])[:3])
        action_items.extend(output.get("action_items", [])[:3])
        open_questions.extend(output.get("open_questions", [])[:3])
        risk_flags.extend(str(item) for item in output.get("risk_flags", [])[:3] if item)

    theme_cards = [
        {"title": topic["title"], "summary": topic.get("summary", "")}
        for topic in topics[:4]
    ]
    lead_topics = "、".join(topic["title"] for topic in topics[:5])
    return {
        "theme_cards": theme_cards,
        "lead_summary": f"本次群聊按主题聚类后，主要覆盖 {lead_topics} 等话题。",
        "sections": sections,
        "participant_insights": participant_insights[:8],
        "quotes": quotes[:6],
        "decisions": decisions[:8],
        "action_items": action_items[:8],
        "open_questions": open_questions[:8],
        "risk_flags": risk_flags[:8],
        "mood": {"label": "多主题活跃", "reason": "基于 topic-first 聚类与分主题分析结果。"},
    }


def run_topic_first_report(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
    max_workers: int,
    max_topics: int,
    section_max_tokens: int,
) -> dict[str, Any]:
    if client is None and not dry_run:
        raise RuntimeError("LLM client 未初始化")
    topic_dir = ensure_dir(output_dir / "topic_first")
    plan_input_path = topic_dir / "topic_plan.input.json"
    plan_output_path = topic_dir / "topic_plan.output.json"
    plan_payload = {
        "chat_name": chat_name,
        "start_time": start_time,
        "end_time": end_time,
        "stats": compact_prompt_stats(stats),
        "chunk": compact_topic_index_payload(chunk),
        "max_topics": max_topics,
    }
    write_json(plan_input_path, plan_payload)
    system_prompt = ""
    user_prompt = ""
    if not dry_run:
        system_prompt, user_prompt = build_topic_plan_prompts(chat_name, start_time, end_time, stats, chunk, max_topics)
    fingerprint = build_stage_fingerprint(
        "topic_plan",
        plan_payload,
        dry_run=dry_run,
        model=f"{client.provider}:{client.model}" if client else "",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    cached_plan = load_cached_stage_output(plan_output_path, fingerprint)
    if cached_plan is None:
        if dry_run:
            raise RuntimeError("topic-first dry-run 需要 LLM 输出 topic plan")
        log_llm_request_estimate("topic-plan", client, system_prompt, user_prompt, 4096)
        raw_plan = client.chat_json(system_prompt, user_prompt, max_tokens=4096, temperature=0.2)
        topics = normalize_topic_plan(raw_plan, chunk, max_topics)
        if not topics:
            raise RuntimeError("topic-first 未生成有效主题")
        cached_plan = {"topics": topics}
        write_stage_output(plan_output_path, cached_plan, fingerprint)
    else:
        topics = normalize_topic_plan(cached_plan, chunk, max_topics)
        cached_plan = {"topics": topics}
    topics = cached_plan["topics"]
    indexed_messages = dict(indexed_analysis_messages(chunk))
    sections_dir = ensure_dir(topic_dir / "sections")

    def analyze_topic(topic: dict[str, Any]) -> dict[str, Any]:
        topic_messages = [
            indexed_messages[index]
            for index in topic.get("message_indexes", [])
            if index in indexed_messages
        ]
        topic_messages = select_topic_messages_for_prompt(topic_messages)
        if not topic_messages:
            raise RuntimeError(f"{topic.get('topic_id')} 没有可分析消息")
        topic_id = topic["topic_id"]
        input_path = sections_dir / f"{topic_id}.input.json"
        output_path = sections_dir / f"{topic_id}.output.json"
        payload = compact_topic_section_payload(topic, topic_messages)
        write_json(input_path, payload)
        section_system, section_user = build_topic_section_prompts(chat_name, topic, topic_messages)
        section_fingerprint = build_stage_fingerprint(
            "topic_section",
            payload,
            dry_run=dry_run,
            model=f"{client.provider}:{client.model}" if client else "",
            system_prompt=section_system,
            user_prompt=section_user,
        )
        cached_output = load_cached_stage_output(output_path, section_fingerprint)
        if cached_output is not None:
            return cached_output
        log_llm_request_estimate(f"topic-section:{topic_id}", client, section_system, section_user, section_max_tokens)
        output = client.chat_json(section_system, section_user, max_tokens=section_max_tokens, temperature=0.2)
        output.setdefault("topic_id", topic_id)
        write_stage_output(output_path, output, section_fingerprint)
        return output

    print(f"[TopicFirst] topic_count={len(topics)}; section_calls={len(topics)}", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        topic_outputs = list(executor.map(analyze_topic, topics))
    report = merge_topic_outputs(topics, topic_outputs)
    report["headline"] = f"{chat_name} 群洞察报表"
    report["tagline"] = f"{start_time} - {end_time}"
    return repair_final_report(report, chat_name, start_time, end_time, stats, [])


def run_direct_final_stage(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
    max_tokens: int,
) -> dict[str, Any]:
    final_dir = ensure_dir(output_dir / "final")
    input_path = final_dir / "report.input.json"
    output_path = final_dir / "report.output.json"
    payload = {
        "mode": "direct_final",
        "chat_name": chat_name,
        "start_time": start_time,
        "end_time": end_time,
        "stats": stats,
        "chunk": compact_direct_chunk_payload(chunk),
    }
    write_json(input_path, payload)
    system_prompt = ""
    user_prompt = ""
    if not dry_run:
        system_prompt, user_prompt = build_direct_final_prompts(chat_name, start_time, end_time, stats, chunk)
    fingerprint = build_stage_fingerprint(
        "direct_final",
        payload,
        dry_run=dry_run,
        model=f"{client.provider}:{client.model}" if client else "",
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )

    cached = load_cached_stage_output(output_path, fingerprint)
    if cached is not None:
        return repair_final_report(cached, chat_name, start_time, end_time, stats, [])

    if dry_run:
        report = fallback_final_report(chat_name, start_time, end_time, stats, [fallback_map_analysis(chunk)])
    else:
        if client is None:
            raise RuntimeError("LLM client 未初始化")
        adjusted_max_tokens = direct_final_max_tokens_for_client(client, system_prompt, user_prompt, max_tokens)
        if adjusted_max_tokens != max_tokens:
            print(
                "[LLMPlan] direct-final output budget adjusted "
                f"{max_tokens} -> {adjusted_max_tokens} "
                "to stay under DeepSeek context window.",
                flush=True,
            )
        max_tokens = adjusted_max_tokens
        log_llm_request_estimate("direct-final", client, system_prompt, user_prompt, max_tokens)
        try:
            report = client.chat_json(system_prompt, user_prompt, max_tokens=max_tokens, temperature=0.2)
        except RuntimeError as exc:
            context_error = parse_context_length_error(str(exc))
            if not context_error or client.provider != "deepseek":
                raise
            limit, _requested, message_tokens, _completion_tokens = context_error
            retry_max_tokens = limit - message_tokens - 256
            if retry_max_tokens < DEFAULT_DIRECT_FINAL_MIN_TOKENS or retry_max_tokens >= max_tokens:
                raise
            print(
                "[LLMPlan] direct-final retry with smaller output budget "
                f"{max_tokens} -> {retry_max_tokens} after DeepSeek context error.",
                flush=True,
            )
            log_llm_request_estimate("direct-final-retry", client, system_prompt, user_prompt, retry_max_tokens)
            report = client.chat_json(system_prompt, user_prompt, max_tokens=retry_max_tokens, temperature=0.2)

    report = repair_final_report(report, chat_name, start_time, end_time, stats, [])
    write_stage_output(output_path, report, fingerprint)
    return report


def render_html_report(
        chat_name: str,
        chat_id: str,
        start_time: str,
        end_time: str,
        stats: dict[str, Any],
        report: dict[str, Any],
    ) -> str:
        def format_handle(name: str) -> str:
            name = (name or '').strip()
            if not name:
                return ''
            if name in {'待定', '暂无', 'unknown'}:
                return name
            if name.startswith('@'):
                return name
            return f'@{name}'

        group_nicknames = get_group_nickname_map(chat_id) if chat_id else {}
        speaker_directory_map: dict[str, str] = {}
        member_display_map: dict[str, str] = {}

        def add_member_mapping(member_id: str, display_name: str) -> None:
            member_id = (member_id or '').strip()
            display_name = (display_name or '').strip()
            if not is_resolved_member_display(member_id, display_name):
                return
            member_display_map[member_id] = display_name

        for item in stats.get('speaker_directory', []):
            member_id = item.get('sender_id', '')
            display_name = item.get('sender_name', '')
            if is_resolved_member_display(member_id, display_name):
                speaker_directory_map[member_id] = display_name
            add_member_mapping(member_id, display_name)
        for item in stats.get('member_aliases', []):
            add_member_mapping(item.get('sender_id', ''), item.get('sender_name', ''))
        for member_id, display_name in group_nicknames.items():
            add_member_mapping(member_id, display_name)

        def resolve_member_name(value: str) -> str:
            value = (value or '').strip()
            if not value:
                return ''
            match = re.fullmatch(r'\[\[user:([^\]]+)\]\]', value)
            if match:
                member_id = match.group(1).strip()
                return member_display_map.get(member_id, speaker_directory_map.get(member_id, member_id))
            return member_display_map.get(value, value)

        def render_member_field(value: str) -> str:
            resolved = resolve_member_name(value)
            if not resolved:
                return ''
            return f'<span class="mention">{html.escape(format_handle(resolved))}</span>'

        def render_rich_text(text: Any) -> str:
            escaped = html.escape(str(text or ''))

            def replace_placeholder(match: re.Match[str]) -> str:
                sender_id = match.group(1).strip()
                sender_name = resolve_member_name(f'[[user:{sender_id}]]')
                return f'<span class="mention">{html.escape(format_handle(sender_name))}</span>'

            return re.sub(r'\[\[user:([^\]]+)\]\]', replace_placeholder, escaped)

        def render_participant_item(item: dict[str, Any]) -> str:
            role = (item.get('role', '') or '').strip()
            role_html = f'（{html.escape(role)}）' if role else ''
            return (
                f"<li><strong>{render_member_field(item.get('name', ''))}</strong>"
                f"{role_html}：{render_rich_text(item.get('observation', ''))}</li>"
            )

        def render_quote_item(item: dict[str, Any]) -> str:
            time_value = (item.get('time', '') or '').strip()
            time_html = f' <span class="quote-time">{html.escape(time_value)}</span>' if time_value else ''
            return f"""
            <blockquote>
              <div class="quote-text">"{render_rich_text(item.get('quote', ''))}"</div>
              <div class="quote-meta">{render_member_field(item.get('speaker', ''))}{time_html}</div>
              <div class="quote-why">{render_rich_text(item.get('why', ''))}</div>
            </blockquote>
            """

        theme_cards_html = ''.join(
            f"""
            <div class="theme-card">
              <div class="theme-title">{render_rich_text(card.get('title', '主题'))}</div>
              <div class="theme-summary">{render_rich_text(card.get('summary', ''))}</div>
            </div>
            """
            for card in report.get('theme_cards', [])[:4]
        )

        participant_sources: list[dict[str, Any]] = []
        for key in ('participant_insights', 'participant_notes'):
            value = report.get(key, [])
            if isinstance(value, list):
                participant_sources.extend(item for item in value if isinstance(item, dict))

        participant_seen: set[tuple[str, str, str]] = set()
        participant_items: list[dict[str, Any]] = []
        for item in participant_sources:
            name = item.get('name', '') or item.get('participant_ref', '') or ''
            observation = item.get('insight', '') or item.get('observation', '') or item.get('contribution', '') or ''
            role = item.get('role', '') or item.get('participant_role', '') or ''
            if not name and not observation:
                continue
            key = (name.strip(), observation.strip(), role.strip())
            if key in participant_seen:
                continue
            participant_seen.add(key)
            participant_items.append({'name': name, 'observation': observation, 'role': role})

        participant_notes_html = ''.join(render_participant_item(item) for item in participant_items[:6])

        quote_items: list[dict[str, Any]] = []
        quote_seen: set[tuple[str, str, str, str]] = set()
        for item in report.get('quotes', []):
            speaker = item.get('speaker', '') or item.get('speaker_ref', '') or ''
            quote = item.get('quote', '') or item.get('text', '') or ''
            why = item.get('why_it_matters', '') or item.get('context', '') or ''
            time_value = item.get('time', '') or ''
            key = (speaker.strip(), quote.strip(), why.strip(), time_value.strip())
            if key in quote_seen:
                continue
            quote_seen.add(key)
            quote_items.append({'speaker': speaker, 'time': time_value, 'quote': quote, 'why': why})

        quotes_html = ''.join(render_quote_item(item) for item in quote_items[:4])

        sections_html = ''.join(
            f"""
            <section class="section-card">
              <div class="section-index">{index}</div>
              <div class="section-body">
                <div class="section-header">
                  <h3>{render_rich_text(section.get('title', f'讨论片段 {index}'))}</h3>
                  <div class="section-time">{html.escape(section.get('start_time', ''))} - {html.escape(section.get('end_time', ''))}</div>
                </div>
                <p class="section-summary">{render_rich_text(section.get('summary', ''))}</p>
                <ul class="section-bullets">
                  {''.join(f"<li>{render_rich_text(item)}</li>" for item in section.get('bullets', [])[:4])}
                </ul>
                <div class="section-takeaway">{render_rich_text(section.get('takeaway', ''))}</div>
              </div>
            </section>
            """
            for index, section in enumerate(report.get('sections', [])[:MAX_REPORT_SECTIONS], start=1)
        )

        leaderboard_html = ''.join(
            f"""
            <li>
              <span class="rank-badge">{speaker['rank']}</span>
              <span class="speaker-name">{render_member_field(speaker['name'])}</span>
              <span class="speaker-count">{speaker['message_count']} 条</span>
            </li>
            """
            for speaker in stats.get('top_speakers', [])[:10]
        )

        interaction_labels = [
            ('pat_sender', '拍一拍最多'),
            ('pat_target', '被拍最多'),
            ('direct_redpacket_receiver', '定向红包最多'),
            ('reply_sender', '回复最多'),
        ]
        interaction_rankings = stats.get('interaction_rankings', {})

        def render_interaction_items(items: list[dict[str, Any]]) -> str:
            return ''.join(
                f"<li>{item.get('rank', index)}. <span class='mention'>{html.escape(format_handle(item.get('name', '')))}</span>：{item.get('count', 0)} 次</li>"
                for index, item in enumerate(items[:5], start=1)
            ) or '<li>暂无</li>'

        interaction_rankings_html = ''.join(
            f"""
            <div class="interaction-group">
              <div class="interaction-title">{label}</div>
              <ul class="simple-list">{render_interaction_items(interaction_rankings.get(key, []))}</ul>
            </div>
            """
            for key, label in interaction_labels
        )

        def first_dict_value(item: dict[str, Any], keys: tuple[str, ...]) -> str:
            for key in keys:
                value = item.get(key, "")
                if value:
                    return str(value)
            return ""

        def render_action_item(item: Any) -> str:
            if not isinstance(item, dict):
                text = str(item or "").strip()
                return f"<li>{render_rich_text(text)}</li>" if text else ""
            owner = first_dict_value(item, ("owner", "name", "participant_ref"))
            task = first_dict_value(item, ("task", "content", "action", "summary", "text"))
            deadline = first_dict_value(item, ("deadline", "time", "due"))
            status_hint = first_dict_value(item, ("status_hint", "status"))
            meta = " / ".join(part for part in (deadline, status_hint) if part)
            meta_html = f' <span class="meta">{html.escape(meta)}</span>' if meta else ""
            if owner and task:
                return f"<li><strong>{render_member_field(owner)}</strong>：{render_rich_text(task)}{meta_html}</li>"
            if task:
                return f"<li>{render_rich_text(task)}{meta_html}</li>"
            if owner:
                return f"<li>{render_member_field(owner)}{meta_html}</li>"
            return ""

        def render_text_item(item: Any, keys: tuple[str, ...]) -> str:
            if isinstance(item, dict):
                text = first_dict_value(item, keys)
            else:
                text = str(item or "")
            text = text.strip()
            return f"<li>{render_rich_text(text)}</li>" if text else ""

        action_items_html = ''.join(
            render_action_item(item)
            for item in report.get('action_items', [])[:8]
        )

        open_questions_html = ''.join(
            render_text_item(item, ("question", "content", "text", "summary"))
            for item in report.get('open_questions', [])[:8]
        )
        risk_flags_html = ''.join(
            render_text_item(item, ("risk", "flag", "content", "text", "summary", "reason"))
            for item in report.get('risk_flags', [])[:8]
        )

        action_items_section_html = (
            f'<section class="aside-card"><h2 class="aside-title">行动项</h2><ul class="simple-list">{action_items_html}</ul></section>'
            if action_items_html
            else ''
        )
        open_questions_section_html = (
            f'<section class="aside-card"><h2 class="aside-title">未决问题</h2><ul class="simple-list">{open_questions_html}</ul></section>'
            if open_questions_html
            else ''
        )
        risk_flags_section_html = (
            f'<section class="aside-card"><h2 class="aside-title">风险提示</h2><ul class="simple-list">{risk_flags_html}</ul></section>'
            if risk_flags_html
            else ''
        )

        word_cloud_items = stats.get('word_cloud', [])[:28]
        max_word_count = max((item['count'] for item in word_cloud_items), default=1)
        min_word_count = min((item['count'] for item in word_cloud_items), default=1)
        word_cloud_html = ''.join(
            f'<span class="cloud-item" style="font-size:{15 + ((item["count"] - min_word_count) / max(1, max_word_count - min_word_count)) * 17:.1f}px">{html.escape(item["word"])}</span>'
            for item in word_cloud_items
        )

        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(report.get('headline', '群洞察报表'))}</title>
  <style>
    :root {{
      --bg: #f8fff4;
      --bg-soft: #fefce8;
      --panel: #ffffff;
      --panel-soft: #fffdf5;
      --ink: #23452d;
      --ink-light: #6b7f71;
      --line: rgba(35, 69, 45, 0.10);
      --shadow: 0 10px 28px rgba(92, 135, 83, 0.10);
      --spring-green: #7bc96f;
      --spring-green-dark: #4d9f5f;
      --spring-yellow: #ffd86b;
      --spring-pink: #ffc4d6;
      --spring-pink-dark: #e16a97;
      --mention-blue: #2563eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; color: var(--ink);
      background: linear-gradient(180deg, #fffef7, var(--bg)); }}
    .page {{ width: min(100%, 520px); margin: 0 auto; padding: 8px; }}
    .hero {{ border-radius: 8px; background:
      linear-gradient(160deg, var(--spring-green), #9bd58a 45%, var(--spring-yellow));
      padding: 16px 14px; box-shadow: var(--shadow); color: #193321; }}
    .eyebrow {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: rgba(255,255,255,0.55); font-size: 12px; font-weight: 700; }}
    .title-cn {{ margin: 10px 0 0; font-size: 32px; font-weight: 900; line-height: 1.12; }}
    .hero-meta {{ margin-top: 10px; display: grid; gap: 4px; font-size: 14px; line-height: 1.55; }}
    .hero-meta .chat-name {{ font-size: 22px; font-weight: 800; }}
    .hero-stats {{ margin-top: 12px; display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }}
    .stat-card {{ border-radius: 8px; padding: 10px 10px; background: rgba(255,255,255,0.55); border: 1px solid rgba(255,255,255,0.65); }}
    .stat-label {{ font-size: 12px; opacity: 0.86; }}
    .stat-value {{ margin-top: 4px; font-size: 22px; font-weight: 900; }}
    .section {{ margin-top: 10px; border-radius: 8px; background: var(--panel); box-shadow: var(--shadow); padding: 14px 12px; }}
    .lead-box {{ border-radius: 8px; background: linear-gradient(180deg, #f0fdf4, #fef9c3); color: var(--ink); padding: 12px; font-size: 16px; line-height: 1.7; font-weight: 600; }}
    .theme-grid {{ display: grid; grid-template-columns: 1fr; gap: 8px; margin-top: 10px; }}
    .theme-card {{ padding: 12px; border-radius: 8px; background: linear-gradient(180deg, #fffdf5, #fff7fb); border: 1px solid rgba(225, 106, 151, 0.15); }}
    .theme-title {{ font-size: 17px; font-weight: 900; color: var(--spring-pink-dark); }}
    .theme-summary {{ margin-top: 6px; line-height: 1.65; color: var(--ink-light); font-size: 15px; }}
    .content-grid {{ display: grid; grid-template-columns: 1fr; gap: 10px; margin-top: 10px; }}
    .section-card {{ display: grid; grid-template-columns: 32px 1fr; gap: 8px; padding: 10px 0; border-bottom: 1px solid var(--line); }}
    .section-card:last-child {{ border-bottom: none; padding-bottom: 0; }}
    .section-index {{ width: 28px; height: 28px; border-radius: 50%; background: var(--spring-green); color: #fff; display: flex; align-items: center; justify-content: center; font-size: 14px; font-weight: 900; margin-top: 1px; }}
    .section-header {{ display: block; }}
    .section-header h3 {{ margin: 0; font-size: 18px; line-height: 1.4; color: var(--ink); }}
    .section-time {{ margin-top: 4px; font-size: 12px; color: var(--ink-light); }}
    .section-summary {{ margin: 8px 0 8px; line-height: 1.72; font-size: 16px; }}
    .section-bullets {{ margin: 0; padding-left: 18px; line-height: 1.72; color: var(--ink-light); font-size: 15px; }}
    .section-takeaway {{ margin-top: 8px; padding-top: 8px; border-top: 1px dashed var(--line); color: var(--spring-green-dark); font-size: 15px; line-height: 1.65; font-weight: 700; }}
    .aside-card {{ border-radius: 8px; padding: 12px; background: var(--panel); border: 1px solid var(--line); box-shadow: var(--shadow); }}
    .aside-title {{ font-size: 17px; font-weight: 900; color: var(--ink); margin: 0 0 8px; }}
    .leaderboard {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 10px; }}
    .leaderboard li {{ display: grid; grid-template-columns: 28px 1fr auto; align-items: center; gap: 8px; }}
    .rank-badge {{ width: 26px; height: 26px; border-radius: 50%; background: #fef3c7; color: #92400e; display: flex; align-items: center; justify-content: center; font-weight: 900; font-size: 12px; }}
    .speaker-name {{ font-weight: 700; }} .speaker-count, .meta {{ color: var(--ink-light); font-size: 14px; }}
    .simple-list {{ margin: 0; padding-left: 18px; line-height: 1.7; font-size: 15px; }}
    .interaction-list {{ display: grid; gap: 10px; }}
    .interaction-group {{ border-top: 1px solid var(--line); padding-top: 8px; }}
    .interaction-group:first-child {{ border-top: 0; padding-top: 0; }}
    .interaction-title {{ margin-bottom: 6px; font-weight: 900; color: var(--spring-green-dark); }}
    blockquote {{ margin: 0 0 8px; padding: 12px; border-radius: 8px; background: linear-gradient(180deg, #fffdf5, #fdf2f8); border: 1px solid rgba(225, 106, 151, 0.15); }}
    .quote-text {{ font-size: 16px; line-height: 1.7; font-weight: 700; }}
    .quote-meta, .quote-why {{ margin-top: 6px; color: var(--ink-light); font-size: 14px; line-height: 1.55; }}
    .quote-time {{ color: var(--ink-light); font-size: 12px; }}
    .stack {{ display: grid; gap: 10px; }}
    .mention {{ color: var(--mention-blue); font-weight: 700; }}
    .word-cloud {{ display: flex; flex-wrap: wrap; gap: 8px 10px; align-items: center; }}
    .cloud-item {{ display: inline-flex; align-items: center; padding: 4px 8px; border-radius: 999px; background: #fdf2f8; color: #9d174d; line-height: 1.2; font-size: 14px; }}
    .footer {{ margin-top: 12px; color: var(--ink-light); font-size: 12px; line-height: 1.6; text-align: center; }}
    @media (min-width: 680px) {{
      .page {{ width: min(100%, 640px); }}
      .hero-stats {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <div class="eyebrow">群聊分析</div>
      <h1 class="title-cn">群聊总结</h1>
      <div class="hero-meta">
        <div class="chat-name">{html.escape(chat_name)}</div>
        <div>统计区间：{html.escape(start_time)} - {html.escape(end_time)}</div>
        <div>{render_rich_text(report.get('tagline', ''))}</div>
      </div>
      <div class="hero-stats">
        <div class="stat-card"><div class="stat-label">今日信息数</div><div class="stat-value">{stats.get('effective_message_count', stats['message_count'])}</div></div>
        <div class="stat-card"><div class="stat-label">今日字数</div><div class="stat-value">{stats.get('effective_char_count', stats.get('raw_char_count', 0))}</div></div>
        <div class="stat-card"><div class="stat-label">参与人数</div><div class="stat-value">{stats['participant_count']}</div></div>
      </div>
    </header>
    <section class="section">
      <div class="lead-box">{render_rich_text(report.get('lead_summary', ''))}</div>
      <div class="theme-grid">{theme_cards_html}</div>
    </section>
    <div class="content-grid">
      <section class="section">
        <h2 class="aside-title">讨论脉络</h2>
        {sections_html}
      </section>
      <div class="stack">
        <section class="aside-card"><h2 class="aside-title">高频词云</h2><div class="word-cloud">{word_cloud_html or '<span class="cloud-item">暂无</span>'}</div></section>
        <section class="aside-card"><h2 class="aside-title">发言排行</h2><ol class="leaderboard">{leaderboard_html}</ol></section>
        <section class="aside-card"><h2 class="aside-title">互动榜单</h2><div class="interaction-list">{interaction_rankings_html}</div></section>
        <section class="aside-card"><h2 class="aside-title">成员观察</h2><ul class="simple-list">{participant_notes_html or '<li>暂无</li>'}</ul></section>
        <section class="aside-card"><h2 class="aside-title">引用原话</h2>{quotes_html or '<div class="quote-why">当前没有可展示的原话。</div>'}</section>
        {action_items_section_html}
        {open_questions_section_html}
        {risk_flags_section_html}
      </div>
    </div>
    <div class="footer">
      <div>{html.escape(report.get('headline', '群洞察报表'))}</div>
      <div>生成时间：{html.escape(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</div>
      <div>本地统计来自已解密数据库，语义摘要由模型生成。</div>
    </div>
  </div>
</body>
</html>
"""


def build_report_payload(
    ctx: dict[str, Any],
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    report: dict[str, Any],
    chunk_count: int,
    chunk_plan: dict[str, Any],
    dry_run: bool,
    provider: str,
    model: str,
) -> dict[str, Any]:
    return {
        "metadata": {
            "chat_name": ctx["display_name"],
            "chat_id": ctx["username"],
            "start_time": start_time,
            "end_time": end_time,
            "chunk_count": chunk_count,
            "chunk_strategy": chunk_plan.get("strategy", ""),
            "chunk_mode": chunk_plan.get("mode", ""),
            "estimated_tokens": chunk_plan.get("estimated_tokens", 0),
            "direct_token_threshold": chunk_plan.get("direct_token_threshold", 0),
            "range_direct": chunk_plan.get("range_direct", False),
            "dry_run": dry_run,
            "provider": provider,
            "model": model,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "stats": stats,
        "report": report,
    }


def invalidate_cached_outputs_if_needed(
    output_dir: Path,
    signature: dict[str, Any],
) -> None:
    signature_path = output_dir / "snapshot" / "run_signature.json"
    if not signature_path.exists():
        previous_signature = None
    else:
        try:
            previous_signature = json.loads(signature_path.read_text(encoding="utf-8"))
        except Exception:
            previous_signature = {}

    if previous_signature == signature:
        return

    for dirname in ["map", "reduce", "final", "topic_first"]:
        target_dir = output_dir / dirname
        if target_dir.exists():
            shutil.rmtree(target_dir)

    for filename in ["group_insight_report.json", "group_insight_report.html", "group_insight_report.png"]:
        target_file = output_dir / filename
        if target_file.exists():
            target_file.unlink()


def resolve_llm_runtime_config(args: argparse.Namespace) -> tuple[str, str, str]:
    provider = (args.provider or DEFAULT_PROVIDER).strip().lower()
    if provider == "zhipu":
        api_key = (args.api_key or os.environ.get("ZHIPUAI_API_KEY", "")).strip()
        model = (args.model or os.environ.get("ZHIPUAI_MODEL", "") or DEFAULT_ZHIPU_MODEL).strip()
    else:
        api_key = (args.api_key or os.environ.get("DEEPSEEK_API_KEY", "")).strip()
        model = (args.model or os.environ.get("DEEPSEEK_MODEL", "") or DEFAULT_DEEPSEEK_MODEL).strip()
    return provider, api_key, model


def create_llm_client(
    provider: str,
    api_key: str,
    model: str,
    api_url: str,
    *,
    zhipu_rate_limit_retries: int,
    zhipu_min_interval_seconds: float,
    zhipu_rate_limit_base_delay: float,
    zhipu_rate_limit_max_delay: float,
) -> LLMClientProtocol:
    if provider == "zhipu":
        return ZhipuClient(
            api_key=api_key,
            model=model,
            max_retries=max(1, zhipu_rate_limit_retries),
            min_interval_seconds=max(0.0, zhipu_min_interval_seconds),
            rate_limit_base_delay=max(0.5, zhipu_rate_limit_base_delay),
            rate_limit_max_delay=max(1.0, zhipu_rate_limit_max_delay),
        )
    if provider == "deepseek":
        return DeepSeekClient(api_key=api_key, model=model, api_url=api_url)
    raise ValueError(f"不支持的 provider: {provider}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a structured WeChat group insight report.")
    parser.add_argument("--chat", default=DEFAULT_ANALYZE_CHAT, help="群聊名称、wxid 或 @chatroom ID。未传时读取脚本顶部 DEFAULT_ANALYZE_CHAT。")
    parser.add_argument("--auto-time", action=argparse.BooleanOptionalAction, default=DEFAULT_AUTO_TIME, help="自动使用昨日 03:59 到今日 03:59 的分析时间窗；具体日切点读取 DEFAULT_AUTO_TIME_CUTOFF。")
    parser.add_argument("--start", default=DEFAULT_ANALYZE_START, help="开始时间，支持 YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]。DEFAULT_AUTO_TIME=False 时读取脚本顶部 DEFAULT_ANALYZE_START。")
    parser.add_argument("--end", default=DEFAULT_ANALYZE_END, help="结束时间，支持 YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]。DEFAULT_AUTO_TIME=False 时读取脚本顶部 DEFAULT_ANALYZE_END。")
    parser.add_argument("--provider", default=os.environ.get("GROUP_INSIGHT_PROVIDER", DEFAULT_PROVIDER), choices=["zhipu", "deepseek"], help="LLM 提供方，默认 zhipu。")
    parser.add_argument("--api-key", default=os.environ.get("GROUP_INSIGHT_API_KEY", ""), help="LLM API Key；若不传则按 provider 读取 ZHIPUAI_API_KEY 或 DEEPSEEK_API_KEY。")
    parser.add_argument("--api-url", default=os.environ.get("DEEPSEEK_API_URL", DEFAULT_API_URL), help=f"DeepSeek chat completions URL，默认 {DEFAULT_API_URL}")
    parser.add_argument("--model", default=os.environ.get("GROUP_INSIGHT_MODEL", ""), help=f"模型名；默认 zhipu 用 {DEFAULT_ZHIPU_MODEL}，deepseek 用 {DEFAULT_DEEPSEEK_MODEL}。")
    parser.add_argument("--max-workers", type=int, default=4, help="map 阶段并行请求数。")
    parser.add_argument("--reduce-fan-in", type=int, default=4, help="每轮 reduce 合并的 shard/bundle 数。")
    parser.add_argument("--chunk-max-messages", type=int, default=500, help="每个 shard 的最大消息数。")
    parser.add_argument("--chunk-max-chars", type=int, default=24000, help="每个 shard 的最大字符预算。")
    parser.add_argument("--chunk-max-minutes", type=int, default=240, help="每个 shard 最大跨度分钟数。")
    parser.add_argument("--hard-gap-minutes", type=int, default=90, help="相邻消息超过该分钟数时强制切 shard。")
    parser.add_argument("--soft-gap-minutes", type=int, default=18, help="用于主题切片的软时间间隔。")
    parser.add_argument("--topic-sim-threshold", type=float, default=0.08, help="主题相似度阈值，越低越不容易拆分。")
    parser.add_argument("--topic-min-chunk-messages", type=int, default=24, help="至少达到该消息数后才允许按主题拆分。")
    parser.add_argument(
        "--direct-token-threshold",
        "--direct-day-token-threshold",
        dest="direct_token_threshold",
        type=int,
        default=100000,
        help="整个查询区间的有效对话估算 token 不超过该阈值时，整段直接交给模型分析；超过后才切片。",
    )
    parser.add_argument("--direct-max-bytes", type=int, default=DEFAULT_DIRECT_MAX_BYTES, help="整段 direct_range 的 shard 输入 JSON 超过该字节数时，直接改走分片；0 表示不按字节数限制，默认只按 token 阈值决定是否 direct。")
    parser.add_argument("--no-direct-retry", action="store_true", help="direct_range 单次请求失败时立即停止，不自动回退到分片多次调用。")
    parser.add_argument("--direct-final-max-tokens", type=int, default=DEFAULT_DIRECT_FINAL_MAX_TOKENS, help="direct_range 单次最终报表调用的最大输出 token。")
    parser.add_argument("--topic-first", action=argparse.BooleanOptionalAction, default=DEFAULT_TOPIC_FIRST, help="direct_range 时先用紧凑全量消息做主题聚类，再按主题分发 section 分析。")
    parser.add_argument("--topic-first-max-topics", type=int, default=DEFAULT_TOPIC_FIRST_MAX_TOPICS, help="topic-first 第一阶段最多生成的主题数。")
    parser.add_argument("--topic-section-max-tokens", type=int, default=DEFAULT_TOPIC_SECTION_MAX_TOKENS, help="topic-first 单个 topic section 调用的最大输出 token。")
    parser.add_argument("--output-dir", default="", help="输出目录；不传则自动生成。")
    parser.add_argument("--dry-run", action="store_true", help="不调用 DeepSeek，只验证导出、切片、reduce 和 HTML 渲染。")
    parser.add_argument("--zhipu-rate-limit-retries", type=int, default=8, help="zhipu 遇到 429/1302 时的总重试次数。")
    parser.add_argument("--zhipu-min-interval-seconds", type=float, default=1.2, help="zhipu 相邻请求的最小间隔秒数。")
    parser.add_argument("--zhipu-rate-limit-base-delay", type=float, default=4.0, help="zhipu 429 指数退避的初始秒数。")
    parser.add_argument("--zhipu-rate-limit-max-delay", type=float, default=90.0, help="zhipu 429 指数退避的最大秒数。")
    parser.add_argument("--no-image", action="store_true", help="跳过浏览器渲染 PNG 导出。")
    parser.add_argument("--image-width", type=int, default=DEFAULT_REPORT_IMAGE_WIDTH, help="导出 PNG 时的浏览器视口宽度。")
    parser.add_argument("--image-timeout-ms", type=int, default=DEFAULT_REPORT_IMAGE_TIMEOUT_MS, help="导出 PNG 时的浏览器等待超时。")
    parser.add_argument("--send-after-run", action=argparse.BooleanOptionalAction, default=DEFAULT_SEND_AFTER_RUN, help="执行完成后发送 PNG 到指定会话。默认读取脚本顶部 DEFAULT_SEND_AFTER_RUN。")
    parser.add_argument("--send-target", action="append", default=None, help="发送目标会话名称；可重复传入，也可用逗号/分号分隔。未传时读取脚本顶部 DEFAULT_SEND_TARGET_CHATS。")
    parser.add_argument("--send-message", default=DEFAULT_SEND_MESSAGE, help="发送 PNG 时附带的文本说明；不传则使用默认摘要。未传时读取脚本顶部 DEFAULT_SEND_MESSAGE。")
    parser.add_argument("--send-to-filehelper", action="store_true", help="生成 PNG 后，通过 pyweixin 发送到文件传输助手。")
    parser.add_argument("--filehelper-name", default="", help=f"兼容旧参数；配合 --send-to-filehelper 使用，默认 {DEFAULT_FILEHELPER_NAME}。")
    parser.add_argument("--filehelper-message", default="", help="兼容旧参数；等价于 --send-message。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.auto_time:
        auto_start, auto_end = compute_auto_time_range()
        if not has_cli_option("--start"):
            args.start = auto_start
        if not has_cli_option("--end"):
            args.end = auto_end
    if not normalize_text(args.chat):
        raise SystemExit("未提供群聊名称。请传 --chat 或编辑脚本顶部 DEFAULT_ANALYZE_CHAT。")
    if not normalize_text(args.start):
        raise SystemExit("未提供开始时间。请传 --start 或编辑脚本顶部 DEFAULT_ANALYZE_START。")
    if not normalize_text(args.end):
        raise SystemExit("未提供结束时间。请传 --end 或编辑脚本顶部 DEFAULT_ANALYZE_END。")
    provider, api_key, model = resolve_llm_runtime_config(args)
    if not args.dry_run and not api_key:
        env_name = "ZHIPUAI_API_KEY" if provider == "zhipu" else "DEEPSEEK_API_KEY"
        raise SystemExit(f"未提供 {provider} API Key。请传 --api-key 或设置环境变量 {env_name}。")

    ctx, messages = fetch_structured_messages(args.chat, args.start, args.end)
    if not messages:
        raise SystemExit("指定时间范围内没有消息。")

    chunks, chunk_plan = build_analysis_chunks(
        messages,
        max_messages=args.chunk_max_messages,
        max_chars=args.chunk_max_chars,
        max_minutes=args.chunk_max_minutes,
        hard_gap_minutes=args.hard_gap_minutes,
        soft_gap_minutes=args.soft_gap_minutes,
        low_similarity_threshold=args.topic_sim_threshold,
        min_chunk_messages=args.topic_min_chunk_messages,
        direct_token_threshold=args.direct_token_threshold,
        direct_max_bytes=args.direct_max_bytes,
    )
    stats = build_local_stats(messages)

    timestamp_label = datetime.now().strftime("%Y%m%d-%H%M%S")
    default_dir = DEFAULT_OUTPUT_ROOT / f"{timestamp_label}-{slugify(ctx['display_name'])}"
    output_dir = ensure_dir(Path(args.output_dir) if args.output_dir else default_dir)

    snapshot_dir = ensure_dir(output_dir / "snapshot")
    def build_run_signature() -> dict[str, Any]:
        return {
            "start_time": args.start,
            "end_time": args.end,
            "message_count": len(messages),
            "first_message_time": messages[0].time if messages else "",
            "last_message_time": messages[-1].time if messages else "",
            "chunk_plan": chunk_plan,
            "topic_first": bool(args.topic_first),
            "topic_first_max_topics": args.topic_first_max_topics,
            "topic_section_max_tokens": args.topic_section_max_tokens,
            "direct_final_max_tokens": args.direct_final_max_tokens,
            "chunk_ids": [chunk.id for chunk in chunks],
            "chunk_ranges": [
                {
                    "id": chunk.id,
                    "start": chunk.start_time,
                    "end": chunk.end_time,
                    "message_count": chunk.message_count,
                }
                for chunk in chunks
            ],
        }

    run_signature = build_run_signature()
    invalidate_cached_outputs_if_needed(output_dir, run_signature)

    def write_snapshot_files() -> None:
        write_json(snapshot_dir / "messages.json", serialize_messages(messages))
        write_json(snapshot_dir / "chunks.json", [chunk_payload(chunk) for chunk in chunks])
        write_json(snapshot_dir / "chunk_plan.json", chunk_plan)
        write_json(snapshot_dir / "stats.json", stats)
        write_json(snapshot_dir / "run_signature.json", run_signature)

    write_snapshot_files()

    client = None if args.dry_run else create_llm_client(
        provider=provider,
        api_key=api_key,
        model=model,
        api_url=args.api_url,
        zhipu_rate_limit_retries=args.zhipu_rate_limit_retries,
        zhipu_min_interval_seconds=args.zhipu_min_interval_seconds,
        zhipu_rate_limit_base_delay=args.zhipu_rate_limit_base_delay,
        zhipu_rate_limit_max_delay=args.zhipu_rate_limit_max_delay,
    )

    effective_max_workers = max(1, args.max_workers)
    if client is not None and client.provider == "zhipu":
        effective_max_workers = 1
    use_direct_final = bool(chunk_plan.get("range_direct") and len(chunks) == 1)
    use_topic_first = bool(use_direct_final and args.topic_first and not args.dry_run)
    if client is not None:
        reduce_call_count = 0 if use_direct_final else estimate_reduce_call_count(len(chunks), max(2, args.reduce_fan_in))
        map_call_count = 0 if use_direct_final else len(chunks)
        if use_topic_first:
            final_label = "topic_plan_calls=1 topic_section_calls<=%d final_calls=0" % max(1, args.topic_first_max_topics)
        else:
            final_label = "direct_final_calls=1" if use_direct_final else "final_calls=1"
        print(
            "[LLMPlan] "
            f"provider={client.provider}/{client.model} "
            f"mode={chunk_plan.get('mode')} "
            f"map_calls={map_call_count} reduce_calls={reduce_call_count} {final_label} "
            f"estimated_tokens={chunk_plan.get('estimated_tokens', 0)} "
            f"direct_threshold={chunk_plan.get('direct_token_threshold', 0)}",
            flush=True,
        )

    try:
        if use_topic_first:
            try:
                final_report = run_topic_first_report(
                    chat_name=ctx["display_name"],
                    start_time=args.start,
                    end_time=args.end,
                    stats=stats,
                    chunk=chunks[0],
                    output_dir=output_dir,
                    dry_run=args.dry_run,
                    client=client,
                    max_workers=effective_max_workers,
                    max_topics=max(4, args.topic_first_max_topics),
                    section_max_tokens=max(1024, args.topic_section_max_tokens),
                )
            except Exception as topic_exc:
                print(f"[TopicFirstFallback] topic-first 失败，改用 direct-final：{topic_exc}", flush=True)
                final_report = run_direct_final_stage(
                    chat_name=ctx["display_name"],
                    start_time=args.start,
                    end_time=args.end,
                    stats=stats,
                    chunk=chunks[0],
                    output_dir=output_dir,
                    dry_run=args.dry_run,
                    client=client,
                    max_tokens=max(4096, args.direct_final_max_tokens),
                )
            reduced_bundles = []
        elif use_direct_final:
            final_report = run_direct_final_stage(
                chat_name=ctx["display_name"],
                start_time=args.start,
                end_time=args.end,
                stats=stats,
                chunk=chunks[0],
                output_dir=output_dir,
                dry_run=args.dry_run,
                client=client,
                max_tokens=max(4096, args.direct_final_max_tokens),
            )
            reduced_bundles = []
        else:
            map_results = run_map_stage(
                chunks,
                output_dir=output_dir,
                dry_run=args.dry_run,
                client=client,
                max_workers=effective_max_workers,
                allow_fallback=True,
            )
            reduced_bundles = run_reduce_stage(
                map_results,
                output_dir=output_dir,
                dry_run=args.dry_run,
                client=client,
                fan_in=max(2, args.reduce_fan_in),
            )
            final_report = run_final_stage(
                chat_name=ctx["display_name"],
                start_time=args.start,
                end_time=args.end,
                stats=stats,
                bundles=reduced_bundles,
                output_dir=output_dir,
                dry_run=args.dry_run,
                client=client,
            )
    except Exception as exc:
        if not (chunk_plan.get("range_direct") and not args.dry_run and len(chunks) == 1):
            raise
        if args.no_direct_retry:
            raise SystemExit(f"direct_range 单次请求失败，已按 --no-direct-retry 停止：{exc}") from exc
        print(f"[DirectRangeRetry] direct_range 单次请求失败，准备改用分片重试：{exc}", flush=True)
        chunks, chunk_plan = build_sharded_range_chunks(
            messages,
            max_messages=args.chunk_max_messages,
            max_chars=args.chunk_max_chars,
            max_minutes=args.chunk_max_minutes,
            hard_gap_minutes=args.hard_gap_minutes,
            soft_gap_minutes=args.soft_gap_minutes,
            low_similarity_threshold=args.topic_sim_threshold,
            min_chunk_messages=args.topic_min_chunk_messages,
            direct_token_threshold=args.direct_token_threshold,
            direct_max_bytes=args.direct_max_bytes,
            fallback_reason=str(exc),
        )
        reduce_call_count = estimate_reduce_call_count(len(chunks), max(2, args.reduce_fan_in))
        print(
            "[DirectRangeRetry] "
            f"sharded_range 将执行 map_calls={len(chunks)} "
            f"reduce_calls={reduce_call_count} final_calls=1；"
            "后续出现多次 LLM 请求属于回退分片流程。",
            flush=True,
        )
        run_signature = build_run_signature()
        write_snapshot_files()
        map_results = run_map_stage(
            chunks,
            output_dir=output_dir,
            dry_run=args.dry_run,
            client=client,
            max_workers=effective_max_workers,
            allow_fallback=True,
        )
        reduced_bundles = run_reduce_stage(
            map_results,
            output_dir=output_dir,
            dry_run=args.dry_run,
            client=client,
            fan_in=max(2, args.reduce_fan_in),
        )
        final_report = run_final_stage(
            chat_name=ctx["display_name"],
            start_time=args.start,
            end_time=args.end,
            stats=stats,
            bundles=reduced_bundles,
            output_dir=output_dir,
            dry_run=args.dry_run,
            client=client,
        )

    payload = build_report_payload(
        ctx=ctx,
        start_time=args.start,
        end_time=args.end,
        stats=stats,
        report=final_report,
        chunk_count=len(chunks),
        chunk_plan=chunk_plan,
        dry_run=args.dry_run,
        provider=provider,
        model=model,
    )
    write_json(output_dir / "group_insight_report.json", payload)

    html_text = render_html_report(
        chat_name=ctx["display_name"],
        chat_id=ctx["username"],
        start_time=args.start,
        end_time=args.end,
        stats=stats,
        report=final_report,
    )
    html_output_path = output_dir / "group_insight_report.html"
    html_output_path.write_text(html_text, encoding="utf-8")
    image_output_path = output_dir / "group_insight_report.png"
    image_error = ""
    send_requested = bool(args.send_after_run or args.send_to_filehelper)
    send_targets = split_send_targets(args.send_target) or split_send_targets(DEFAULT_SEND_TARGET_CHATS)
    if args.send_to_filehelper:
        filehelper_target = normalize_text(args.filehelper_name) or DEFAULT_FILEHELPER_NAME
        for target in split_send_targets(filehelper_target):
            if target not in send_targets:
                send_targets.append(target)
    send_text = normalize_text(args.send_message) or normalize_text(args.filehelper_message)
    send_results: list[tuple[str, str, str]] = []
    if not args.no_image:
        image_error = export_report_image(
            html_output_path,
            image_output_path,
            viewport_width=max(480, args.image_width),
            timeout_ms=max(5000, args.image_timeout_ms),
        )
    if send_requested:
        if args.no_image:
            send_results = [(target, "failed", "已指定 --no-image，无法发送 PNG。") for target in send_targets]
        elif not image_output_path.exists():
            send_results = [(target, "failed", f"PNG 未生成成功: {image_error or 'unknown error'}") for target in send_targets]
        elif not send_targets:
            send_results = [("", "failed", "未指定发送目标会话。")]
        else:
            default_send_message = "\n".join(
                [
                    "群聊洞察报表",
                    f"群聊：{ctx['display_name']}",
                    f"区间：{args.start} -> {args.end}",
                    f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                ]
            )
            for send_target in send_targets:
                try:
                    send_report_png_to_chat(
                        image_output_path,
                        message_lines=[send_text] if send_text else [default_send_message],
                        friend_name=send_target,
                    )
                    send_results.append((send_target, "sent", ""))
                except Exception as exc:
                    send_results.append((send_target, "failed", str(exc)))

    print("=" * 72)
    print("群洞察报表生成完成")
    print("=" * 72)
    print(f"群聊: {ctx['display_name']} ({ctx['username']})")
    print(f"区间: {args.start} -> {args.end}")
    print(f"消息数: {len(messages)} | 分片数: {len(chunks)} | dry_run: {args.dry_run}")
    print(
        "分析策略: "
        f"{chunk_plan.get('strategy', 'unknown')} | "
        f"模式 {chunk_plan.get('mode', 'unknown')} | "
        f"估算 tokens {chunk_plan.get('estimated_tokens', 0)} | "
        f"阈值 {chunk_plan.get('direct_token_threshold', 0)}"
    )
    print(f"模型: {provider} / {model}")
    print(f"map 并发: {effective_max_workers}")
    if provider == "zhipu":
        print(
            "zhipu 限频策略: "
            f"retries={args.zhipu_rate_limit_retries}, "
            f"min_interval={args.zhipu_min_interval_seconds}s, "
            f"backoff={args.zhipu_rate_limit_base_delay}s..{args.zhipu_rate_limit_max_delay}s"
        )
    print(f"输出目录: {output_dir}")
    print(f"JSON: {output_dir / 'group_insight_report.json'}")
    print(f"HTML: {html_output_path}")
    if args.no_image:
        print("PNG: skipped (--no-image)")
    elif image_output_path.exists():
        print(f"PNG: {image_output_path}")
    else:
        print(f"PNG: failed ({image_error or 'unknown error'})")
    if send_requested:
        for target, status, detail in send_results:
            if status == "sent":
                print(f"发送: sent -> {target}")
            else:
                print(f"发送: failed -> {target or '(none)'} ({detail})")


if __name__ == "__main__":
    main()
