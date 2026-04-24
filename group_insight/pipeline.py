"""群聊日报的 LLM 分析流水线。

固定按 map/reduce/final 路径执行，并为每个阶段落盘输入输出与指纹缓存，
保证重跑时可复用。
"""
from __future__ import annotations

from .report_model import *
def run_map_stage(
    chunks: list[MessageChunk],
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
    max_workers: int,
) -> list[dict[str, Any]]:
    """并发执行 map 阶段，生成每个消息片段的结构化分析。"""
    map_dir = ensure_dir(output_dir / "map")

    def analyze_chunk(chunk: MessageChunk) -> dict[str, Any]:
        """分析单个分片并处理缓存与 dry-run 路径。"""
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
            model=llm_cache_identity(client),
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        write_json(input_path, chunk_input)

        # 每个阶段先写 input，再按指纹读缓存；这样重跑时不会重复消耗模型额度。
        cached = load_cached_stage_output(output_path, fingerprint)
        if cached is not None:
            return cached

        if dry_run:
            result = fallback_map_analysis(chunk)
        else:
            if client is None:
                raise RuntimeError("LLM client 未初始化")
            stage_max_tokens = structured_stage_max_tokens_for_client(client)
            log_llm_request_estimate(f"map:{chunk.id}", client, system_prompt, user_prompt, stage_max_tokens)
            result = client.chat_json(system_prompt, user_prompt, max_tokens=stage_max_tokens, temperature=0.2)

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
    """执行一轮 reduce，把若干分析结果按 fan-in 合并成 bundle。"""
    reduce_dir = ensure_dir(output_dir / "reduce" / f"round-{round_index:02d}")
    # fan-in 控制每次 reduce 的输入规模，过大的窗口会增加上下文超限风险。
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
            model=llm_cache_identity(client),
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
            stage_max_tokens = structured_stage_max_tokens_for_client(client)
            log_llm_request_estimate(f"reduce:{bundle_id}", client, system_prompt, user_prompt, stage_max_tokens)
            bundle = client.chat_json(system_prompt, user_prompt, max_tokens=stage_max_tokens, temperature=0.2)
            bundle.setdefault("bundle_id", bundle_id)

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
    """循环执行 reduce，直到剩余结果可直接进入 final。"""
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
    """基于 reduce bundles 生成并修复最终日报结构。"""
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
        model=llm_cache_identity(client),
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
        stage_max_tokens = structured_stage_max_tokens_for_client(client)
        log_llm_request_estimate("final", client, system_prompt, user_prompt, stage_max_tokens)
        report = client.chat_json(system_prompt, user_prompt, max_tokens=stage_max_tokens, temperature=0.2)

    report = repair_final_report(report, chat_name, start_time, end_time, stats, bundles)
    write_stage_output(output_path, report, fingerprint)
    return report


