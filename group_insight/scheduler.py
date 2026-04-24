"""注册 Windows 任务计划，每日定时运行群洞察报表。"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_TASK_NAME = "GroupInsightReportDaily"
DEFAULT_TIME = "22:30"
DEFAULT_MODULE = "group_insight"
DEFAULT_VENV_PYTHON = ROOT_DIR / ".venv" / "Scripts" / "python.exe"
DEFAULT_PYTHON = str(DEFAULT_VENV_PYTHON if DEFAULT_VENV_PYTHON.exists() else Path(sys.executable).resolve())

TASK_TRIGGER_DAILY = 2
TASK_ACTION_EXEC = 0
TASK_CREATE_OR_UPDATE = 6
TASK_LOGON_INTERACTIVE_TOKEN = 3
TASK_RUNLEVEL_LUA = 0
TASK_RUNLEVEL_HIGHEST = 1


@dataclass(frozen=True)
class TaskTarget:
    """任务计划中要执行的 Python 入口。"""

    kind: str
    value: str | Path

    def display(self) -> str:
        """返回便于打印的入口描述。"""

        if self.kind == "module":
            return f"module {self.value}"
        return str(self.value)


def load_task_scheduler():
    """延迟导入 Windows 任务计划 COM 相关模块。"""

    try:
        import win32com.client
    except ImportError as exc:  # pragma: no cover - 环境缺失时直接给出可读错误
        raise SystemExit(
            "缺少 pywin32/win32com.client，无法注册任务计划。请先安装 pywin32 后再运行这个脚本。"
        ) from exc
    return win32com.client.Dispatch("Schedule.Service")


def parse_time(value: str) -> tuple[int, int]:
    """解析 HH:MM 格式的任务触发时间。"""

    try:
        parsed = datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--time 需要是 HH:MM 格式，例如 21:00") from exc
    return parsed.hour, parsed.minute


def resolve_executable(value: str) -> str:
    """解析 Python 可执行文件路径，支持 PATH 中的命令名。"""

    candidate = Path(value)
    if candidate.exists():
        return str(candidate.resolve())

    has_path_separator = os.path.sep in value or (os.path.altsep is not None and os.path.altsep in value)
    resolved = shutil.which(value) if not has_path_separator else None
    if resolved:
        return resolved

    raise FileNotFoundError(f"找不到 Python 可执行文件: {value}")


def resolve_script(value: str) -> Path:
    """解析并校验任务计划要执行的脚本路径。"""

    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = ROOT_DIR / candidate
    candidate = candidate.resolve()
    if not candidate.is_file():
        raise FileNotFoundError(f"找不到脚本文件: {candidate}")
    return candidate


def resolve_task_target(module_name: str, script_path: str) -> TaskTarget:
    """解析最终要注册的任务入口。"""

    legacy_script = script_path.strip()
    if legacy_script:
        return TaskTarget(kind="script", value=resolve_script(legacy_script))

    module = module_name.strip()
    if not module:
        raise SystemExit("未提供任务入口。请传 --module 或 --script。")
    return TaskTarget(kind="module", value=module)


def build_start_boundary(hour: int, minute: int) -> str:
    """生成任务计划每日触发器需要的本地 ISO 时间。"""

    now = datetime.now()
    start = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if start <= now:
        start += timedelta(days=1)
    return start.isoformat(timespec="seconds")


def build_arguments(target: TaskTarget, extra_args: str) -> str:
    """拼接任务计划中 python.exe 的命令行参数。"""

    if target.kind == "module":
        args = ["-m", str(target.value)]
    else:
        args = [str(target.value)]
    if extra_args.strip():
        args.extend(shlex.split(extra_args, posix=False))
    return subprocess.list2cmdline(args)


def register_task(
    task_name: str,
    python_path: str,
    task_target: TaskTarget,
    task_args: str,
    start_boundary: str,
    *,
    run_highest: bool,
    wake_to_run: bool,
) -> None:
    """创建或更新 Windows 每日任务计划。"""

    service = load_task_scheduler()
    service.Connect()

    task = service.NewTask(0)
    task.RegistrationInfo.Description = f"Daily run of {task_target.display()}"
    settings = task.Settings
    settings.Enabled = True
    settings.StartWhenAvailable = True
    settings.AllowDemandStart = True
    settings.DisallowStartIfOnBatteries = False
    settings.StopIfGoingOnBatteries = False
    settings.MultipleInstances = 0
    settings.WakeToRun = bool(wake_to_run)

    principal = task.Principal
    principal.LogonType = TASK_LOGON_INTERACTIVE_TOKEN
    principal.RunLevel = TASK_RUNLEVEL_HIGHEST if run_highest else TASK_RUNLEVEL_LUA

    trigger = task.Triggers.Create(TASK_TRIGGER_DAILY)
    trigger.StartBoundary = start_boundary
    trigger.DaysInterval = 1
    trigger.Enabled = True

    action = task.Actions.Create(TASK_ACTION_EXEC)
    action.Path = python_path
    action.Arguments = build_arguments(task_target, task_args)
    action.WorkingDirectory = str(ROOT_DIR)

    root_folder = service.GetFolder("\\")
    root_folder.RegisterTaskDefinition(
        task_name,
        task,
        TASK_CREATE_OR_UPDATE,
        "",
        "",
        TASK_LOGON_INTERACTIVE_TOKEN,
    )


def parse_args() -> argparse.Namespace:
    """解析任务计划注册脚本的命令行参数。"""

    parser = argparse.ArgumentParser(description="注册 Windows 任务计划，每日定时运行 group_insight 报表。")
    parser.add_argument("--time", default=DEFAULT_TIME, type=parse_time, help="每天运行时间，格式 HH:MM，默认 23:50")
    parser.add_argument("--task-name", default=DEFAULT_TASK_NAME, help="任务计划名称")
    parser.add_argument("--python", default=DEFAULT_PYTHON, help="用于执行脚本的 Python 可执行文件，默认优先使用仓库 .venv")
    parser.add_argument("--module", default=DEFAULT_MODULE, help="要执行的 Python 模块，默认 group_insight")
    parser.add_argument("--script", default="", help="兼容旧参数；传入时改为执行脚本路径，而不是 --module")
    parser.add_argument("--args", default="", help="传给报表入口的额外参数，例如 \"--chat xxx --send-after-run\"")
    parser.add_argument("--highest", action="store_true", help="用最高权限运行任务；需要管理员权限注册")
    parser.add_argument("--no-wake", action="store_true", help="不要允许任务唤醒计算机")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要注册的任务信息，不实际写入任务计划")
    return parser.parse_args()


def main() -> None:
    """注册任务计划或在 dry-run 模式下打印将要执行的配置。"""

    args = parse_args()
    hour, minute = args.time
    python_path = resolve_executable(args.python)
    task_target = resolve_task_target(args.module, args.script)
    start_boundary = build_start_boundary(hour, minute)
    command_line = build_arguments(task_target, args.args)

    print(f"任务名称: {args.task_name}")
    print(f"启动时间: 每日 {hour:02d}:{minute:02d}")
    print(f"Python: {python_path}")
    print(f"入口: {task_target.display()}")
    print(f"工作目录: {ROOT_DIR}")
    print(f"命令行: {python_path} {command_line}")
    print(f"首次触发: {start_boundary}")
    print(f"最高权限: {bool(args.highest)}")
    print(f"唤醒计算机: {not args.no_wake}")

    if args.dry_run:
        print("dry-run: 未注册任务")
        return

    register_task(
        args.task_name,
        python_path,
        task_target,
        args.args,
        start_boundary,
        run_highest=bool(args.highest),
        wake_to_run=not args.no_wake,
    )
    print(f"已注册任务: {args.task_name}")


if __name__ == "__main__":
    from .runtime import rerun_current_module_with_workspace_venv

    rerun_current_module_with_workspace_venv("group_insight.scheduler")
    main()
