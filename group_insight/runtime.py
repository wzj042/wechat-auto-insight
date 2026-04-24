"""包入口运行时工具。"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_VENV_PYTHON = ROOT_DIR / ".venv" / "Scripts" / "python.exe"
NO_REDIRECT_ENV = "GROUP_INSIGHT_NO_VENV_REDIRECT"


def rerun_with_workspace_venv(invocation: list[str]) -> None:
    """优先使用仓库 `.venv`，避免落到全局 Python 的过期依赖。"""

    if os.environ.get(NO_REDIRECT_ENV):
        return
    if not WORKSPACE_VENV_PYTHON.exists():
        return
    if Path(sys.executable).resolve() == WORKSPACE_VENV_PYTHON.resolve():
        return
    os.execv(
        str(WORKSPACE_VENV_PYTHON),
        [str(WORKSPACE_VENV_PYTHON), *invocation, *sys.argv[1:]],
    )


def rerun_current_script_with_workspace_venv(script_path: Path) -> None:
    """按当前脚本路径重启到工作区虚拟环境。"""

    rerun_with_workspace_venv([str(script_path.resolve())])


def rerun_current_module_with_workspace_venv(module_name: str) -> None:
    """按当前模块名重启到工作区虚拟环境。"""

    rerun_with_workspace_venv(["-m", module_name])
