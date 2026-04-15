"""日报脚本的默认配置、常量与懒加载入口。

这里统一管理运行时默认值、环境变量读取、正则模式和微信 MCP 的延迟导入，
避免上层流程直接依赖零散的全局状态。
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
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPT_DIR = ROOT_DIR
WECHAT_DECRYPT_DIR = ROOT_DIR / "wechat-decrypt"
for candidate in (SCRIPT_DIR, WECHAT_DECRYPT_DIR):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

_wechat_mcp_module = None


def get_wechat_mcp():
    """延迟导入微信 MCP 模块。

    日报脚本并非所有运行路径都需要访问微信数据库，因此这里把导入延迟到首次
    真正使用时，避免启动阶段就触发不必要的依赖加载。
    """

    global _wechat_mcp_module
    if _wechat_mcp_module is None:
        import mcp_server as module
        _wechat_mcp_module = module
    return _wechat_mcp_module


class _LazyWechatMcp:
    """对外暴露的微信 MCP 懒加载代理。"""

    def __getattr__(self, name: str) -> Any:
        """首次访问任意属性时再加载真实 mcp_server 模块。"""

        return getattr(get_wechat_mcp(), name)


wechat_mcp = _LazyWechatMcp()
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
# 默认发送目标会话列表；可以包含“文件传输助手”、好友或群聊名称；@TODO 这会批量发送有点问题
DEFAULT_SEND_TARGET_CHATS = [
    # "有氧运动聊天",
    "文件传输助手",
    ]
# 默认附带文本；留空时使用脚本自动生成的摘要。
DEFAULT_SEND_MESSAGE = datetime.now().strftime("%m-%d") + "日报已发送"
DEFAULT_OUTPUT_ROOT = SCRIPT_DIR / "reports" / "group_insight"
STAGE_CACHE_VERSION = 3
MAX_LINE_TEXT_LEN = 1200
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
    """从本地 `.env` 文件加载环境变量。

    优先读取仓库根目录，再兼容当前工作目录和父目录。已存在于 `os.environ`
    的键不会被覆盖，便于任务计划或外部 shell 显式传入变量。
    """

    raw_candidates = [SCRIPT_DIR / ".env", Path.cwd() / ".env", SCRIPT_DIR.parent / ".env"]
    candidates: list[Path] = []
    seen_paths: set[Path] = set()
    for candidate in raw_candidates:
        resolved = candidate.resolve()
        if resolved in seen_paths:
            continue
        seen_paths.add(resolved)
        candidates.append(resolved)

    for env_path in candidates:
        if not env_path.exists():
            continue
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.lstrip("\ufeff").strip()
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


load_local_env()
