"""日报 HTML、PNG 导出与发送相关的传输层工具。"""

from __future__ import annotations

from subprocess import DEVNULL, CalledProcessError, run

from .conversation import *


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
    friend_name: str = DEFAULT_FILEHELPER_NAME,
) -> None:
    """通过 `pyweixin` 将 PNG 发送到指定会话。"""

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
