"""日报脚本的命令行入口与主编排流程。"""

from __future__ import annotations

from .pipeline import *
from .rendering import *
from .transport import *


def parse_optional_env_bool(value: str | None, *, default: bool, env_name: str) -> bool:
    """解析环境变量中的布尔开关。"""
    normalized = (value or "").strip().lower()
    if not normalized:
        return default
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    raise SystemExit(f"{env_name} 只能是 true/false、1/0、on/off、enabled/disabled。")


def normalize_reasoning_effort(value: str | None) -> str:
    """校验 DeepSeek reasoning_effort 配置。"""
    normalized = (value or "").strip().lower()
    if not normalized:
        return DEFAULT_DEEPSEEK_REASONING_EFFORT
    if normalized not in {"high", "max"}:
        raise SystemExit("THINKING_LEVEL / --reasoning-effort 只能是 high 或 max。")
    return normalized


def resolve_llm_runtime_config(args: argparse.Namespace) -> tuple[str, str, bool, str]:
    """根据参数与环境变量解析运行时 DeepSeek 配置。"""

    api_key = (args.api_key or os.environ.get("DEEPSEEK_API_KEY", "")).strip()
    model = (args.model or os.environ.get("DEEPSEEK_MODEL", "") or DEFAULT_DEEPSEEK_MODEL).strip()
    thinking_enabled = (
        bool(args.thinking)
        if args.thinking is not None
        else parse_optional_env_bool(
            os.environ.get("THINKING"),
            default=DEFAULT_DEEPSEEK_THINKING,
            env_name="THINKING",
        )
    )
    reasoning_effort = normalize_reasoning_effort(
        args.reasoning_effort if args.reasoning_effort is not None else os.environ.get("THINKING_LEVEL")
    )
    return api_key, model, thinking_enabled, reasoning_effort


def create_llm_client(
    api_key: str,
    model: str,
    api_url: str,
    allow_json_repair: bool,
    thinking_enabled: bool,
    reasoning_effort: str,
) -> LLMClientProtocol:
    """创建 DeepSeek LLM 客户端。"""

    return DeepSeekClient(
        api_key=api_key,
        model=model,
        api_url=api_url,
        allow_json_repair=allow_json_repair,
        thinking_enabled=thinking_enabled,
        reasoning_effort=reasoning_effort,
    )


def capture_balance_snapshot(client: LLMClientProtocol | None, stage: str) -> dict[str, Any] | None:
    """在任务前后抓取一次 DeepSeek 余额快照。"""
    getter = getattr(client, "get_user_balance", None)
    if client is None or not callable(getter):
        return None
    try:
        snapshot = getter()
    except Exception as exc:
        print(f"[BalanceCheckFailed] {stage}: {exc}", flush=True)
        return None
    print(f"[BalanceSnapshot] {stage}: {format_balance_snapshot(snapshot)}", flush=True)
    return snapshot


def parse_args() -> argparse.Namespace:
    """解析日报脚本的命令行参数。"""

    parser = argparse.ArgumentParser(description="Generate a structured WeChat group insight report.")
    parser.add_argument("--chat", default=DEFAULT_ANALYZE_CHAT, help="群聊名称、wxid 或 @chatroom ID。未传时读取脚本顶部 DEFAULT_ANALYZE_CHAT。")
    parser.add_argument("--auto-time", action=argparse.BooleanOptionalAction, default=DEFAULT_AUTO_TIME, help=f"自动使用昨日 {DEFAULT_AUTO_TIME_CUTOFF} 到今日 {DEFAULT_AUTO_TIME_CUTOFF} 的分析时间窗；具体日切点读取 DEFAULT_AUTO_TIME_CUTOFF。")
    parser.add_argument("--start", default=DEFAULT_ANALYZE_START, help="开始时间，支持 YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]。DEFAULT_AUTO_TIME=False 时读取脚本顶部 DEFAULT_ANALYZE_START。")
    parser.add_argument("--end", default=DEFAULT_ANALYZE_END, help="结束时间，支持 YYYY-MM-DD / YYYY-MM-DD HH:MM[:SS]。DEFAULT_AUTO_TIME=False 时读取脚本顶部 DEFAULT_ANALYZE_END。")
    parser.add_argument("--api-key", default="", help="DeepSeek API Key；若不传则读取环境变量 DEEPSEEK_API_KEY。")
    parser.add_argument("--api-url", default=os.environ.get("DEEPSEEK_API_URL", DEFAULT_API_URL), help=f"DeepSeek chat completions URL，默认 {DEFAULT_API_URL}")
    parser.add_argument("--model", default="", help=f"DeepSeek 模型名；默认 {DEFAULT_DEEPSEEK_MODEL}。")
    parser.add_argument("--thinking", action=argparse.BooleanOptionalAction, default=None, help="是否启用 DeepSeek 思考模式；默认读取环境变量 THINKING，未设置时关闭。")
    parser.add_argument("--reasoning-effort", choices=["high", "max"], default=None, help=f"DeepSeek 思考强度；默认读取环境变量 THINKING_LEVEL，未设置时为 {DEFAULT_DEEPSEEK_REASONING_EFFORT}。")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAP_MAX_WORKERS, help="map 阶段并行请求数；适当提高可加快速度，但也会增加瞬时并发。")
    parser.add_argument("--reduce-fan-in", type=int, default=DEFAULT_REDUCE_FAN_IN, help="每轮 reduce 合并的 shard/bundle 数；越大轮数越少，但单次上下文越长。")
    parser.add_argument("--chunk-max-messages", type=int, default=DEFAULT_CHUNK_MAX_MESSAGES, help="每个 shard 允许的最大消息条数。")
    parser.add_argument("--chunk-max-chars", type=int, default=DEFAULT_CHUNK_MAX_CHARS, help="每个 shard 允许的最大字符预算，用来控制 prompt 体积。")
    parser.add_argument("--chunk-max-minutes", type=int, default=DEFAULT_CHUNK_MAX_MINUTES, help="每个 shard 允许覆盖的最大时间跨度，单位分钟。")
    parser.add_argument("--hard-gap-minutes", type=int, default=DEFAULT_HARD_GAP_MINUTES, help="相邻消息超过该分钟数时强制切 shard。")
    parser.add_argument("--soft-gap-minutes", type=int, default=DEFAULT_SOFT_GAP_MINUTES, help="主题连续性判断用的软间隔分钟数。")
    parser.add_argument("--topic-sim-threshold", type=float, default=DEFAULT_TOPIC_SIM_THRESHOLD, help="主题相似度阈值，越低越不容易因话题切换而拆 shard。")
    parser.add_argument("--topic-min-chunk-messages", type=int, default=DEFAULT_TOPIC_MIN_CHUNK_MESSAGES, help="至少达到该消息数后才允许按主题连续性进一步切 shard。")
    parser.add_argument("--allow-json-repair", action="store_true", help="允许 DeepSeek 返回损坏 JSON 时自动发起一次修复请求。默认关闭，避免掩盖模型输出问题。")
    parser.add_argument("--output-dir", default="", help="输出目录；不传则自动生成。")
    parser.add_argument("--dry-run", action="store_true", help="不调用 DeepSeek，只验证导出、切片、reduce 和 HTML 渲染。")
    parser.add_argument("--no-image", action="store_true", help="跳过浏览器渲染 PNG 导出。")
    parser.add_argument("--image-width", type=int, default=DEFAULT_REPORT_IMAGE_WIDTH, help="导出 PNG 时的浏览器视口宽度。")
    parser.add_argument("--image-timeout-ms", type=int, default=DEFAULT_REPORT_IMAGE_TIMEOUT_MS, help="导出 PNG 时的浏览器等待超时。")
    parser.add_argument("--send-after-run", action=argparse.BooleanOptionalAction, default=DEFAULT_SEND_AFTER_RUN, help="执行完成后发送 PNG 到指定会话。默认读取脚本顶部 DEFAULT_SEND_AFTER_RUN。")
    parser.add_argument("--send-target", action="append", default=None, help="发送目标会话名称；可重复传入，也可用逗号/分号分隔。未传时读取脚本顶部 DEFAULT_SEND_TARGET_CHATS。")
    parser.add_argument("--send-message", default=DEFAULT_SEND_MESSAGE, help="发送 PNG 时附带的文本说明；不传则使用默认摘要。未传时读取脚本顶部 DEFAULT_SEND_MESSAGE。")
    return parser.parse_args()


def resolve_send_delivery(args: argparse.Namespace) -> tuple[bool, list[str], str]:
    """归一化发送开关、目标会话和附带文本。"""

    send_requested = bool(args.send_after_run)
    send_targets = split_send_targets(args.send_target)
    if not send_targets and args.send_after_run:
        send_targets = split_send_targets(DEFAULT_SEND_TARGET_CHATS)
    send_text = normalize_text(args.send_message)
    return send_requested, send_targets, send_text


def main() -> None:
    """执行日报报表的完整生成、渲染与可选发送流程。"""

    args = parse_args()
    # 自动时间窗只负责补默认值，不覆盖用户显式传入的 --start / --end。
    if args.auto_time:
        auto_start, auto_end = compute_auto_time_range()
        if not has_cli_option("--start"):
            args.start = auto_start
        if not has_cli_option("--end"):
            args.end = auto_end
    if not normalize_text(args.chat):
        raise SystemExit("未提供群聊名称。请传 --chat 或编辑脚本顶部 DEFAULT_ANALYZE_CHAT。")
    if not normalize_text(args.start):
        raise SystemExit("未提供开始时间。请传 --start 或编辑脚本顶部 DEFAULT_ANALYZE_START。")
    if not normalize_text(args.end):
        raise SystemExit("未提供结束时间。请传 --end 或编辑脚本顶部 DEFAULT_ANALYZE_END。")
    provider = DEFAULT_PROVIDER
    api_key, model, thinking_enabled, reasoning_effort = resolve_llm_runtime_config(args)
    if not args.dry_run and not api_key:
        raise SystemExit("未提供 DeepSeek API Key。请传 --api-key 或设置环境变量 DEEPSEEK_API_KEY。")

    ctx, messages = fetch_structured_messages(args.chat, args.start, args.end)
    if not messages:
        raise SystemExit("指定时间范围内没有消息。")

    # 先做分片与本地统计，再进入 LLM 阶段，便于保存快照和复用中间产物。
    chunks, chunk_plan = build_analysis_chunks(
        messages,
        max_messages=args.chunk_max_messages,
        max_chars=args.chunk_max_chars,
        max_minutes=args.chunk_max_minutes,
        hard_gap_minutes=args.hard_gap_minutes,
        soft_gap_minutes=args.soft_gap_minutes,
        low_similarity_threshold=args.topic_sim_threshold,
        min_chunk_messages=args.topic_min_chunk_messages,
    )
    stats = build_local_stats(messages)

    timestamp_label = datetime.now().strftime("%Y%m%d-%H%M%S")
    default_dir = DEFAULT_OUTPUT_ROOT / f"{timestamp_label}-{slugify(ctx['display_name'])}"
    output_dir = ensure_dir(Path(args.output_dir) if args.output_dir else default_dir)

    snapshot_dir = ensure_dir(output_dir / "snapshot")

    def build_run_signature() -> dict[str, Any]:
        """构造本次运行的缓存签名。"""

        return {
            "llm_model": model,
            "llm_thinking_enabled": thinking_enabled,
            "llm_reasoning_effort": reasoning_effort,
            "max_workers": max(1, args.max_workers),
            "reduce_fan_in": max(2, args.reduce_fan_in),
            "start_time": args.start,
            "end_time": args.end,
            "message_count": len(messages),
            "first_message_time": messages[0].time if messages else "",
            "last_message_time": messages[-1].time if messages else "",
            "chunk_plan": chunk_plan,
            "chunk_ids": [chunk.id for chunk in chunks],
            "chunk_ranges": [
                {
                    "id": chunk.id,
                    "start": chunk.start_time,
                    "end": chunk.end_time,
                    "message_count": chunk.message_count,
                }
                for chunk in chunks
            ],
        }

    run_signature = build_run_signature()
    invalidate_cached_outputs_if_needed(output_dir, run_signature)

    def write_snapshot_files() -> None:
        """写出本次运行的调试与回溯快照。"""

        write_json(snapshot_dir / "messages.json", serialize_messages(messages))
        write_json(snapshot_dir / "chunks.json", [chunk_payload(chunk) for chunk in chunks])
        write_json(snapshot_dir / "chunk_plan.json", chunk_plan)
        write_json(snapshot_dir / "stats.json", stats)
        write_json(snapshot_dir / "run_signature.json", run_signature)

    write_snapshot_files()

    client = None if args.dry_run else create_llm_client(
        api_key=api_key,
        model=model,
        api_url=args.api_url,
        allow_json_repair=bool(args.allow_json_repair),
        thinking_enabled=thinking_enabled,
        reasoning_effort=reasoning_effort,
    )
    balance_before = None if args.dry_run else capture_balance_snapshot(client, "before")

    effective_max_workers = max(1, args.max_workers)
    if client is not None:
        reduce_call_count = estimate_reduce_call_count(len(chunks), max(2, args.reduce_fan_in))
        map_call_count = len(chunks)
        effort_label = f"effort={getattr(client, 'reasoning_effort', '')} " if getattr(client, "thinking_enabled", False) else ""
        print(
            "[LLMPlan] "
            f"provider={client.provider}/{client.model} "
            f"thinking={'enabled' if getattr(client, 'thinking_enabled', False) else 'disabled'} "
            f"{effort_label}"
            f"mode={chunk_plan.get('mode')} "
            f"map_calls={map_call_count} reduce_calls={reduce_call_count} final_calls=1 "
            f"estimated_tokens={chunk_plan.get('estimated_tokens', 0)} "
            f"fan_in={max(2, args.reduce_fan_in)}",
            flush=True,
        )

    # 固定流程按 map -> reduce -> final 逐级汇总。
    map_results = run_map_stage(
        chunks,
        output_dir=output_dir,
        dry_run=args.dry_run,
        client=client,
        max_workers=effective_max_workers,
    )
    reduced_bundles = run_reduce_stage(
        map_results,
        output_dir=output_dir,
        dry_run=args.dry_run,
        client=client,
        fan_in=max(2, args.reduce_fan_in),
    )
    final_report = run_final_stage(
        chat_name=ctx["display_name"],
        start_time=args.start,
        end_time=args.end,
        stats=stats,
        bundles=reduced_bundles,
        output_dir=output_dir,
        dry_run=args.dry_run,
        client=client,
    )
    balance_after = None if args.dry_run else capture_balance_snapshot(client, "after")

    payload = build_report_payload(
        ctx=ctx,
        start_time=args.start,
        end_time=args.end,
        stats=stats,
        report=final_report,
        chunk_count=len(chunks),
        chunk_plan=chunk_plan,
        dry_run=args.dry_run,
        provider=provider,
        model=model,
    )
    write_json(output_dir / "group_insight_report.json", payload)

    # 生成 HTML 和 PNG，最后按需发送到微信会话。
    html_text = render_html_report(
        chat_name=ctx["display_name"],
        chat_id=ctx["username"],
        start_time=args.start,
        end_time=args.end,
        stats=stats,
        report=final_report,
    )
    html_output_path = output_dir / "group_insight_report.html"
    html_output_path.write_text(html_text, encoding="utf-8")
    image_output_path = output_dir / "group_insight_report.png"
    image_error = ""
    send_requested, send_targets, send_text = resolve_send_delivery(args)
    send_results: list[tuple[str, str, str]] = []
    if not args.no_image:
        image_error = export_report_image(
            html_output_path,
            image_output_path,
            viewport_width=max(480, args.image_width),
            timeout_ms=max(5000, args.image_timeout_ms),
        )
    if send_requested:
        if args.no_image:
            send_results = [(target, "failed", "已指定 --no-image，无法发送 PNG。") for target in send_targets]
        elif not image_output_path.exists():
            send_results = [(target, "failed", f"PNG 未生成成功: {image_error or 'unknown error'}") for target in send_targets]
        elif not send_targets:
            send_results = [("", "failed", "未指定发送目标会话。")]
        else:
            default_send_message = "\n".join(
                [
                    "群聊洞察报表",
                    f"群聊：{ctx['display_name']}",
                    f"区间：{args.start} -> {args.end}",
                    f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                ]
            )
            for send_target in send_targets:
                try:
                    send_report_png_to_chat(
                        image_output_path,
                        message_lines=[send_text] if send_text else [default_send_message],
                        friend_name=send_target,
                    )
                    send_results.append((send_target, "sent", ""))
                except Exception as exc:
                    send_results.append((send_target, "failed", str(exc)))

    print("=" * 72)
    print("群洞察报表生成完成")
    print("=" * 72)
    print(f"群聊: {ctx['display_name']} ({ctx['username']})")
    print(f"区间: {args.start} -> {args.end}")
    print(f"消息数: {len(messages)} | 分片数: {len(chunks)} | dry_run: {args.dry_run}")
    print(
        "分析策略: "
        f"{chunk_plan.get('strategy', 'unknown')} | "
        f"模式 {chunk_plan.get('mode', 'unknown')} | "
        f"估算 tokens {chunk_plan.get('estimated_tokens', 0)}"
    )
    print(f"模型: {provider} / {model}")
    print(f"map 并发: {effective_max_workers}")
    print(f"reduce fan-in: {max(2, args.reduce_fan_in)}")
    if balance_before and balance_after:
        print(f"余额变化: {format_balance_delta(balance_before, balance_after)}")
    print(f"输出目录: {output_dir}")
    print(f"JSON: {output_dir / 'group_insight_report.json'}")
    print(f"HTML: {html_output_path}")
    if args.no_image:
        print("PNG: skipped (--no-image)")
    elif image_output_path.exists():
        print(f"PNG: {image_output_path}")
    else:
        print(f"PNG: failed ({image_error or 'unknown error'})")
    if send_requested:
        for target, status, detail in send_results:
            if status == "sent":
                print(f"发送: sent -> {target}")
            else:
                print(f"发送: failed -> {target or '(none)'} ({detail})")
