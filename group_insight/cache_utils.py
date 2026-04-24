"""阶段缓存工具。

为 map/reduce/final 各阶段提供基于 SHA1 指纹的输入输出缓存，
避免重复调用模型消耗 API 额度。
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .common import write_json
from .settings import STAGE_CACHE_VERSION


def build_stage_fingerprint(
    stage: str,
    input_payload: Any,
    *,
    dry_run: bool,
    model: str = "",
    system_prompt: str = "",
    user_prompt: str = "",
) -> str:
    """根据阶段输入、模型和 prompt 生成缓存指纹。"""
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
    """根据阶段输出路径推导旁路元数据文件路径。"""
    return output_path.with_suffix(output_path.suffix + ".meta.json")


def load_cached_stage_output(output_path: Path, fingerprint: str) -> Any | None:
    """在指纹一致时读取阶段缓存，避免重复调用模型。"""
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
    """写入阶段输出及其缓存元数据。"""
    write_json(output_path, payload)
    write_json(
        get_stage_meta_path(output_path),
        {
            "fingerprint": fingerprint,
            "cache_version": STAGE_CACHE_VERSION,
            "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
