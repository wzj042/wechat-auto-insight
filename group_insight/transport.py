"""日报 HTML、PNG 导出与发送相关的传输层工具。"""

from __future__ import annotations

import math
import re
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from subprocess import DEVNULL, CalledProcessError, run
from typing import Any

from .common import normalize_text
from .settings import (
    DEFAULT_AUTO_TIME_CUTOFF,
    DEFAULT_REPORT_IMAGE_TIMEOUT_MS,
    DEFAULT_REPORT_IMAGE_WIDTH,
    ROOT_DIR,
)


def find_local_browser_executable() -> str:
    """查找本机可用的 Chrome/Edge 可执行文件。"""

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
    """使用 Playwright 将 HTML 报表导出为 PNG。

    返回空字符串表示成功，否则返回可读错误信息。
    """

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
    """使用浏览器命令行兜底导出 PNG。"""

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
    """优先用 Playwright 导出，失败后回退到浏览器 CLI。"""

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
    friend_name: str = "文件传输助手",
    max_retries: int = 3,
    retry_delay: float = 3.0,
    send_delay: float | None = None,
) -> None:
    """通过 `pyweixin` 将 PNG 发送到指定会话，失败时自动重试。"""

    import time

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

    kwargs: dict[str, Any] = {
        "friend": friend_name,
        "files": [str(image_path.resolve())],
        "with_messages": bool(normalized_messages),
        "messages": normalized_messages,
        "messages_first": True,
        "is_maximize": False,
        "close_weixin": False,
    }
    if send_delay is not None:
        kwargs["send_delay"] = send_delay

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            Files.send_files_to_friend(**kwargs)
            return
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            backoff = retry_delay * (2 ** (attempt - 1))
            print(
                f"[SendRetry] 第 {attempt} 次发送到 '{friend_name}' 失败: {exc}，"
                f"{backoff:.1f}s 后重试（指数退避）...",
                flush=True,
            )
            time.sleep(backoff)

    raise RuntimeError(f"发送到 '{friend_name}' 失败（已重试 {max_retries} 次）: {last_error}") from last_error


def send_report_png_to_chats(
    image_path: Path,
    message_lines: list[str] | None = None,
    friend_names: list[str] | None = None,
    max_retries: int = 3,
    retry_delay: float = 3.0,
    send_delay: float | None = None,
    send_interval: float = 1.5,
) -> list[tuple[str, str, str]]:
    """将 PNG 报表依次发送到多个微信会话。

    对每个目标循环调用 `send_report_png_to_chat`，目标之间固定间隔
    `send_interval` 秒，避免操作过快导致微信风控或 UI 状态错乱。

    返回每个目标的结果列表，格式为 ``(friend_name, status, detail)``，
    其中 ``status`` 为 ``"sent"`` 或 ``"failed"``。
    """

    import time

    targets = friend_names or []
    if not targets:
        return []

    results: list[tuple[str, str, str]] = []
    for idx, friend_name in enumerate(targets):
        try:
            send_report_png_to_chat(
                image_path=image_path,
                message_lines=message_lines,
                friend_name=friend_name,
                max_retries=max_retries,
                retry_delay=retry_delay,
                send_delay=send_delay,
            )
            results.append((friend_name, "sent", ""))
        except Exception as exc:
            detail = str(exc)
            results.append((friend_name, "failed", detail))
            print(
                f"[MultiSendFailed] 发送到 '{friend_name}' 失败: {detail}",
                flush=True,
            )
        # 最后一个目标之后不需要等待
        if idx < len(targets) - 1:
            time.sleep(send_interval)

    return results


def has_cli_option(*names: str) -> bool:
    """检查当前命令行是否显式传入了某个参数名。"""

    argv = sys.argv[1:]
    return any(arg == name or arg.startswith(f"{name}=") for arg in argv for name in names)


def compute_auto_time_range(cutoff: str = DEFAULT_AUTO_TIME_CUTOFF) -> tuple[str, str]:
    """计算自动时间窗的起止时间。"""

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
    """将发送目标参数拆分、去空和去重。"""

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
