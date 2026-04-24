"""`python -m group_insight` 入口。"""

from __future__ import annotations

from .runtime import rerun_current_module_with_workspace_venv

rerun_current_module_with_workspace_venv("group_insight")

from .cli import main


if __name__ == "__main__":
    main()
