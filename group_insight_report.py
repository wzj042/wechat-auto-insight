#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""兼容入口：生成微信群聊洞察报表。"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def rerun_with_workspace_venv() -> None:
    """优先使用仓库 `.venv`，避免落到全局 Python 的过期依赖。"""

    if os.environ.get("GROUP_INSIGHT_NO_VENV_REDIRECT"):
        return
    root_dir = Path(__file__).resolve().parent
    venv_python = root_dir / ".venv" / "Scripts" / "python.exe"
    if not venv_python.exists():
        return
    if Path(sys.executable).resolve() == venv_python.resolve():
        return
    os.execv(str(venv_python), [str(venv_python), str(Path(__file__).resolve()), *sys.argv[1:]])


rerun_with_workspace_venv()

from group_insight.cli import main


if __name__ == "__main__":
    main()
