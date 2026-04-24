"""群聊日报的 LLM 分析流水线。

根据输入规模选择 map/reduce/final、direct-final 或 topic-first 路径，并为每个阶段
落盘输入输出与指纹缓存，保证重跑时可复用。
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


def normalize_topic_plan(plan: dict[str, Any], chunk: MessageChunk, max_topics: int) -> list[dict[str, Any]]:
    """校验和清洗模型返回的 topic-first 主题计划。"""
    indexed_messages = dict(indexed_analysis_messages(chunk))
    topics: list[dict[str, Any]] = []
    seen_topic_ids: set[str] = set()
    for raw_topic in plan.get("topics", []):
        raw_indexes = raw_topic.get("message_indexes", [])
        indexes: list[int] = []
        for value in raw_indexes:
            try:
                index = int(value)
            except (TypeError, ValueError):
                continue
            if index in indexed_messages and index not in indexes:
                indexes.append(index)
        if not indexes:
            continue
        messages = [indexed_messages[index] for index in indexes]
        title = normalize_text(raw_topic.get("title", ""), max_len=100)
        summary = normalize_text(raw_topic.get("summary", ""), max_len=240)
        if not title and not summary:
            continue
        topic_id = normalize_text(raw_topic.get("topic_id", ""), max_len=32) or f"t{len(topics) + 1:02d}"
        topic_id = re.sub(r"[^A-Za-z0-9_-]+", "-", topic_id).strip("-") or f"t{len(topics) + 1:02d}"
        if topic_id in seen_topic_ids:
            topic_id = f"{topic_id}-{len(topics) + 1:02d}"
        seen_topic_ids.add(topic_id)
        priority = normalize_text(raw_topic.get("priority", ""), max_len=12).lower()
        if priority not in {"major", "minor"}:
            priority = "major" if len(indexes) >= 8 else "minor"
        topics.append(
            {
                "topic_id": topic_id,
                "title": title or "话题",
                "summary": summary,
                "message_indexes": indexes,
                "start_time": raw_topic.get("start_time") or messages[0].time,
                "end_time": raw_topic.get("end_time") or messages[-1].time,
                "priority": priority,
            }
        )
        if len(topics) >= max_topics:
            break
    return topics


def select_topic_messages_for_prompt(messages: list[StructuredMessage], max_messages: int = 260) -> list[StructuredMessage]:
    """为单个 topic 抽样消息，控制 section prompt 长度。"""
    if len(messages) <= max_messages:
        return messages
    if max_messages <= 2:
        return messages[:max_messages]
    selected_indexes = {0, len(messages) - 1}
    for slot in range(1, max_messages - 1):
        selected_indexes.add(round(slot * (len(messages) - 1) / (max_messages - 1)))
    return [messages[index] for index in sorted(selected_indexes)]


def merge_topic_outputs(topics: list[dict[str, Any]], topic_outputs: list[dict[str, Any]]) -> dict[str, Any]:
    """把多个 topic section 输出合并为最终报表草稿。"""
    topic_by_id = {topic["topic_id"]: topic for topic in topics}
    sections: list[dict[str, Any]] = []
    participant_insights: list[dict[str, Any]] = []
    quotes: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    action_items: list[dict[str, Any]] = []
    open_questions: list[dict[str, Any]] = []
    risk_flags: list[str] = []

    for output in topic_outputs:
        topic = topic_by_id.get(output.get("topic_id", ""), {})
        section = output.get("section", {}) if isinstance(output.get("section"), dict) else {}
        if section:
            sections.append(
                {
                    "title": normalize_text(section.get("title", "") or topic.get("title", ""), max_len=100),
                    "start_time": section.get("start_time") or topic.get("start_time", ""),
                    "end_time": section.get("end_time") or topic.get("end_time", ""),
                    "summary": normalize_text(section.get("summary", "") or topic.get("summary", ""), max_len=240),
                    "bullets": section.get("bullets", [])[:3],
                    "takeaway": normalize_text(section.get("takeaway", ""), max_len=160),
                }
            )
        participant_insights.extend(output.get("participant_insights", [])[:3])
        quotes.extend(output.get("quotes", [])[:2])
        decisions.extend(output.get("decisions", [])[:3])
        action_items.extend(output.get("action_items", [])[:3])
        open_questions.extend(output.get("open_questions", [])[:3])
        risk_flags.extend(str(item) for item in output.get("risk_flags", [])[:3] if item)

    theme_cards = [
        {"title": topic["title"], "summary": topic.get("summary", "")}
        for topic in topics[:4]
    ]
    lead_topics = "、".join(topic["title"] for topic in topics[:5])
    return {
        "theme_cards": theme_cards,
        "lead_summary": f"本次群聊按主题聚类后，主要覆盖 {lead_topics} 等话题。",
        "sections": sections,
        "participant_insights": participant_insights[:8],
        "quotes": quotes[:6],
        "decisions": decisions[:8],
        "action_items": action_items[:8],
        "open_questions": open_questions[:8],
        "risk_flags": risk_flags[:8],
        "mood": {"label": "多主题活跃", "reason": "基于 topic-first 聚类与分主题分析结果。"},
    }


def run_topic_first_report(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
    max_workers: int,
    max_topics: int,
    section_max_tokens: int,
) -> dict[str, Any]:
    """执行 topic-first：先聚类主题，再并发分析各主题 section。"""
    if client is None and not dry_run:
        raise RuntimeError("LLM client 未初始化")
    topic_dir = ensure_dir(output_dir / "topic_first")
    plan_input_path = topic_dir / "topic_plan.input.json"
    plan_output_path = topic_dir / "topic_plan.output.json"
    plan_payload = {
        "chat_name": chat_name,
        "start_time": start_time,
        "end_time": end_time,
        "stats": compact_prompt_stats(stats),
        "chunk": compact_topic_index_payload(chunk),
        "max_topics": max_topics,
    }
    write_json(plan_input_path, plan_payload)
    system_prompt = ""
    user_prompt = ""
    if not dry_run:
        system_prompt, user_prompt = build_topic_plan_prompts(chat_name, start_time, end_time, stats, chunk, max_topics)
    fingerprint = build_stage_fingerprint(
        "topic_plan",
        plan_payload,
        dry_run=dry_run,
        model=llm_cache_identity(client),
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    cached_plan = load_cached_stage_output(plan_output_path, fingerprint)
    if cached_plan is None:
        if dry_run:
            raise RuntimeError("topic-first dry-run 需要 LLM 输出 topic plan")
        if client is None:
            raise RuntimeError("LLM client 未初始化")
        stage_max_tokens = structured_stage_max_tokens_for_client(client)
        log_llm_request_estimate("topic-plan", client, system_prompt, user_prompt, stage_max_tokens)
        raw_plan = client.chat_json(system_prompt, user_prompt, max_tokens=stage_max_tokens, temperature=0.2)
        topics = normalize_topic_plan(raw_plan, chunk, max_topics)
        if not topics:
            raise RuntimeError("topic-first 未生成有效主题")
        cached_plan = {"topics": topics}
        write_stage_output(plan_output_path, cached_plan, fingerprint)
    else:
        topics = normalize_topic_plan(cached_plan, chunk, max_topics)
        cached_plan = {"topics": topics}
    topics = cached_plan["topics"]
    indexed_messages = dict(indexed_analysis_messages(chunk))
    sections_dir = ensure_dir(topic_dir / "sections")

    def analyze_topic(topic: dict[str, Any]) -> dict[str, Any]:
        """分析 topic-first 中的单个主题并缓存输出。"""
        topic_messages = [
            indexed_messages[index]
            for index in topic.get("message_indexes", [])
            if index in indexed_messages
        ]
        topic_messages = select_topic_messages_for_prompt(topic_messages)
        if not topic_messages:
            raise RuntimeError(f"{topic.get('topic_id')} 没有可分析消息")
        topic_id = topic["topic_id"]
        input_path = sections_dir / f"{topic_id}.input.json"
        output_path = sections_dir / f"{topic_id}.output.json"
        payload = compact_topic_section_payload(topic, topic_messages)
        write_json(input_path, payload)
        section_system, section_user = build_topic_section_prompts(chat_name, topic, topic_messages)
        section_fingerprint = build_stage_fingerprint(
            "topic_section",
            payload,
            dry_run=dry_run,
            model=llm_cache_identity(client),
            system_prompt=section_system,
            user_prompt=section_user,
        )
        cached_output = load_cached_stage_output(output_path, section_fingerprint)
        if cached_output is not None:
            return cached_output
        if client is None:
            raise RuntimeError("LLM client 未初始化")
        log_llm_request_estimate(f"topic-section:{topic_id}", client, section_system, section_user, section_max_tokens)
        output = client.chat_json(section_system, section_user, max_tokens=section_max_tokens, temperature=0.2)
        output.setdefault("topic_id", topic_id)
        write_stage_output(output_path, output, section_fingerprint)
        return output

    # topic-first 先用完整上下文做主题规划，再把每个主题拆成可并行的 section 分析。
    print(f"[TopicFirst] topic_count={len(topics)}; section_calls={len(topics)}", flush=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        topic_outputs = list(executor.map(analyze_topic, topics))
    report = merge_topic_outputs(topics, topic_outputs)
    report["headline"] = f"{chat_name} 群洞察报表"
    report["tagline"] = f"{start_time} - {end_time}"
    return repair_final_report(report, chat_name, start_time, end_time, stats, [])


def run_direct_final_stage(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
    output_dir: Path,
    dry_run: bool,
    client: LLMClientProtocol | None,
    max_tokens: int,
) -> dict[str, Any]:
    """执行 direct-final，直接用完整时间窗生成最终日报。"""
    final_dir = ensure_dir(output_dir / "final")
    input_path = final_dir / "report.input.json"
    output_path = final_dir / "report.output.json"
    payload = {
        "mode": "direct_final",
        "chat_name": chat_name,
        "start_time": start_time,
        "end_time": end_time,
        "stats": stats,
        "chunk": compact_direct_chunk_payload(chunk),
    }
    write_json(input_path, payload)
    system_prompt = ""
    user_prompt = ""
    if not dry_run:
        system_prompt, user_prompt = build_direct_final_prompts(chat_name, start_time, end_time, stats, chunk)
    fingerprint = build_stage_fingerprint(
        "direct_final",
        payload,
        dry_run=dry_run,
        model=llm_cache_identity(client),
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )

    cached = load_cached_stage_output(output_path, fingerprint)
    if cached is not None:
        return repair_final_report(cached, chat_name, start_time, end_time, stats, [])

    if dry_run:
        report = fallback_final_report(chat_name, start_time, end_time, stats, [fallback_map_analysis(chunk)])
    else:
        if client is None:
            raise RuntimeError("LLM client 未初始化")
        adjusted_max_tokens = direct_final_max_tokens_for_client(client, system_prompt, user_prompt, max_tokens)
        if adjusted_max_tokens != max_tokens:
            print(
                "[LLMPlan] direct-final output budget adjusted "
                f"{max_tokens} -> {adjusted_max_tokens} "
                "to stay under DeepSeek context window.",
                flush=True,
            )
        max_tokens = adjusted_max_tokens
        log_llm_request_estimate("direct-final", client, system_prompt, user_prompt, max_tokens)
        try:
            report = client.chat_json(system_prompt, user_prompt, max_tokens=max_tokens, temperature=0.2)
        except RuntimeError as exc:
            context_error = parse_context_length_error(str(exc))
            if not context_error or client.provider != "deepseek":
                raise
            limit, _requested, message_tokens, _completion_tokens = context_error
            retry_max_tokens = limit - message_tokens - 256
            if retry_max_tokens < DEFAULT_DIRECT_FINAL_MIN_TOKENS or retry_max_tokens >= max_tokens:
                raise
            # DeepSeek 上下文报错时只收缩输出预算，不改变输入，避免破坏 direct_range 语义。
            print(
                "[LLMPlan] direct-final retry with smaller output budget "
                f"{max_tokens} -> {retry_max_tokens} after DeepSeek context error.",
                flush=True,
            )
            log_llm_request_estimate("direct-final-retry", client, system_prompt, user_prompt, retry_max_tokens)
            report = client.chat_json(system_prompt, user_prompt, max_tokens=retry_max_tokens, temperature=0.2)

    report = repair_final_report(report, chat_name, start_time, end_time, stats, [])
    write_stage_output(output_path, report, fingerprint)
    return report
