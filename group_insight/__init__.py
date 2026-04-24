"""Group insight report package."""

from __future__ import annotations


def main() -> None:
    """惰性导入 CLI 入口，避免普通包导入时拉起整条运行链。"""

    from .cli import main as cli_main

    cli_main()


__all__ = ["main"]
