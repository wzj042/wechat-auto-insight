"""日报 JSON 载荷与 HTML 页面渲染。

本模块把最终 report、统计信息和成员占位符解析成可导出的移动端 HTML，并负责在
输入签名变化时清理过期阶段缓存。
"""
from __future__ import annotations

import html
import re
from datetime import datetime

from .common import write_json
from .fetching import get_group_nickname_map, is_resolved_member_display
from .settings import MAX_REPORT_SECTIONS


def render_html_report(
        chat_name: str,
        chat_id: str,
        start_time: str,
        end_time: str,
        stats: dict[str, Any],
        report: dict[str, Any],
    ) -> str:
        """把最终报表和本地统计渲染为移动端 HTML 页面。"""
        def format_handle(name: str) -> str:
            """把成员显示名格式化为报表中的 @昵称。"""
            name = (name or '').strip()
            if not name:
                return ''
            if name in {'待定', '暂无', 'unknown'}:
                return name
            if name.startswith('@'):
                return name
            return f'@{name}'

        group_nicknames = get_group_nickname_map(chat_id) if chat_id else {}
        speaker_directory_map: dict[str, str] = {}
        member_display_map: dict[str, str] = {}

        def add_member_mapping(member_id: str, display_name: str) -> None:
            """把成员 ID 与有效展示名加入渲染期映射表。"""
            member_id = (member_id or '').strip()
            display_name = (display_name or '').strip()
            if not is_resolved_member_display(member_id, display_name):
                return
            member_display_map[member_id] = display_name

        for item in stats.get('speaker_directory', []):
            member_id = item.get('sender_id', '')
            display_name = item.get('sender_name', '')
            if is_resolved_member_display(member_id, display_name):
                speaker_directory_map[member_id] = display_name
            add_member_mapping(member_id, display_name)
        for item in stats.get('member_aliases', []):
            add_member_mapping(item.get('sender_id', ''), item.get('sender_name', ''))
        for member_id, display_name in group_nicknames.items():
            add_member_mapping(member_id, display_name)

        def resolve_member_id_prefix(prefix: str) -> str:
            """用唯一前缀兜底修复被截断的成员占位符。"""
            prefix = (prefix or '').strip().rstrip('.')
            if not prefix:
                return ''
            if prefix in member_display_map:
                return prefix
            matches = [member_id for member_id in member_display_map if member_id.startswith(prefix)]
            return matches[0] if len(matches) == 1 else ''

        def resolve_member_name(value: str) -> str:
            """把成员占位符或原始 ID 解析为展示名。"""
            value = (value or '').strip()
            if not value:
                return ''
            match = re.fullmatch(r'\[\[user:([^\]]+)\]\]', value)
            if match:
                member_id = match.group(1).strip()
                return member_display_map.get(member_id, speaker_directory_map.get(member_id, '未知成员'))
            if value.startswith(('wxid_', 'gh_')) or value.endswith('@chatroom'):
                member_id = resolve_member_id_prefix(value)
                if member_id:
                    return member_display_map.get(member_id, member_id)
                return '未知成员'
            return member_display_map.get(value, value)

        def render_member_field(value: str) -> str:
            """把成员字段渲染为带 mention 样式的 HTML。"""
            resolved = resolve_member_name(value)
            if not resolved:
                return ''
            return f'<span class="mention">{html.escape(format_handle(resolved))}</span>'

        def render_rich_text(text: Any) -> str:
            """转义普通文本并替换其中的成员占位符。"""
            escaped = html.escape(str(text or ''))

            def replace_placeholder(match: re.Match[str]) -> str:
                """把富文本中的成员占位符替换为 mention HTML。"""
                sender_id = match.group(1).strip().rstrip('.')
                member_id = resolve_member_id_prefix(sender_id) or sender_id
                sender_name = resolve_member_name(f'[[user:{member_id}]]')
                return f'<span class="mention">{html.escape(format_handle(sender_name))}</span>'

            def replace_raw_member_id(match: re.Match[str]) -> str:
                """把模型偶发直出的 wxid/gh id 也替换为成员名。"""
                sender_id = match.group(1).strip().rstrip('.')
                member_id = resolve_member_id_prefix(sender_id) or sender_id
                sender_name = resolve_member_name(f'[[user:{member_id}]]')
                return f'<span class="mention">{html.escape(format_handle(sender_name))}</span>'

            escaped = re.sub(r'\[\[user:([A-Za-z0-9_@.-]+)(?:\]\]|\.\.\.)?', replace_placeholder, escaped)
            return re.sub(r'@?\b((?:wxid|gh)_[A-Za-z0-9_.-]{6,})\b', replace_raw_member_id, escaped)

        def render_participant_item(item: dict[str, Any]) -> str:
            """渲染一条成员观察列表项。"""
            role = (item.get('role', '') or '').strip()
            role_html = f'（{html.escape(role)}）' if role else ''
            return (
                f"<li><strong>{render_member_field(item.get('name', ''))}</strong>"
                f"{role_html}：{render_rich_text(item.get('observation', ''))}</li>"
            )

        def render_quote_item(item: dict[str, Any]) -> str:
            """渲染一条引用原话块。"""
            time_value = (item.get('time', '') or '').strip()
            time_html = f' <span class="quote-time">{html.escape(time_value)}</span>' if time_value else ''
            return f"""
            <blockquote>
              <div class="quote-text">"{render_rich_text(item.get('quote', ''))}"</div>
              <div class="quote-meta">{render_member_field(item.get('speaker', ''))}{time_html}</div>
              <div class="quote-why">{render_rich_text(item.get('why', ''))}</div>
            </blockquote>
            """

        theme_cards_html = ''.join(
            f"""
            <div class="theme-card">
              <div class="theme-title">{render_rich_text(card.get('title', '主题'))}</div>
              <div class="theme-summary">{render_rich_text(card.get('summary', ''))}</div>
            </div>
            """
            for card in report.get('theme_cards', [])[:4]
        )

        participant_sources: list[dict[str, Any]] = []
        for key in ('participant_insights', 'participant_notes'):
            value = report.get(key, [])
            if isinstance(value, list):
                participant_sources.extend(item for item in value if isinstance(item, dict))

        participant_seen: set[tuple[str, str, str]] = set()
        participant_items: list[dict[str, Any]] = []
        for item in participant_sources:
            name = item.get('name', '') or item.get('participant_ref', '') or ''
            observation = item.get('insight', '') or item.get('observation', '') or item.get('contribution', '') or ''
            role = item.get('role', '') or item.get('participant_role', '') or ''
            if not name and not observation:
                continue
            key = (name.strip(), observation.strip(), role.strip())
            if key in participant_seen:
                continue
            participant_seen.add(key)
            participant_items.append({'name': name, 'observation': observation, 'role': role})

        participant_notes_html = ''.join(render_participant_item(item) for item in participant_items[:6])

        quote_items: list[dict[str, Any]] = []
        quote_seen: set[tuple[str, str, str, str]] = set()
        for item in report.get('quotes', []):
            speaker = item.get('speaker', '') or item.get('speaker_ref', '') or ''
            quote = item.get('quote', '') or item.get('text', '') or ''
            why = item.get('why_it_matters', '') or item.get('context', '') or ''
            time_value = item.get('time', '') or ''
            key = (speaker.strip(), quote.strip(), why.strip(), time_value.strip())
            if key in quote_seen:
                continue
            quote_seen.add(key)
            quote_items.append({'speaker': speaker, 'time': time_value, 'quote': quote, 'why': why})

        quotes_html = ''.join(render_quote_item(item) for item in quote_items[:4])

        sections_html = ''.join(
            f"""
            <section class="section-card">
              <div class="section-index">{index}</div>
              <div class="section-body">
                <div class="section-header">
                  <h3>{render_rich_text(section.get('title', f'讨论片段 {index}'))}</h3>
                  <div class="section-time">{html.escape(section.get('start_time', ''))} - {html.escape(section.get('end_time', ''))}</div>
                </div>
                <p class="section-summary">{render_rich_text(section.get('summary', ''))}</p>
                <ul class="section-bullets">
                  {''.join(f"<li>{render_rich_text(item)}</li>" for item in section.get('bullets', [])[:4])}
                </ul>
                <div class="section-takeaway">{render_rich_text(section.get('takeaway', ''))}</div>
              </div>
            </section>
            """
            for index, section in enumerate(report.get('sections', [])[:MAX_REPORT_SECTIONS], start=1)
        )

        leaderboard_html = ''.join(
            f"""
            <li>
              <span class="rank-badge">{speaker['rank']}</span>
              <span class="speaker-name">{render_member_field(speaker['name'])}</span>
              <span class="speaker-count">{speaker['message_count']} 条</span>
            </li>
            """
            for speaker in stats.get('top_speakers', [])[:10]
        )

        interaction_labels = [
            ('pat_sender', '拍一拍最多'),
            ('pat_target', '被拍最多'),
            ('direct_redpacket_receiver', '定向红包最多'),
            ('reply_sender', '回复最多'),
        ]
        interaction_rankings = stats.get('interaction_rankings', {})

        def render_interaction_items(items: list[dict[str, Any]]) -> str:
            """渲染一个互动榜单分组。"""
            return ''.join(
                f"<li>{item.get('rank', index)}. {render_member_field(item.get('name', ''))}：{item.get('count', 0)} 次</li>"
                for index, item in enumerate(items[:5], start=1)
            ) or '<li>暂无</li>'

        interaction_rankings_html = ''.join(
            f"""
            <div class="interaction-group">
              <div class="interaction-title">{label}</div>
              <ul class="simple-list">{render_interaction_items(interaction_rankings.get(key, []))}</ul>
            </div>
            """
            for key, label in interaction_labels
        )

        def first_dict_value(item: dict[str, Any], keys: tuple[str, ...]) -> str:
            """按优先级从字典中取第一个非空字段。"""
            for key in keys:
                value = item.get(key, "")
                if value:
                    return str(value)
            return ""

        def render_action_item(item: Any) -> str:
            """渲染行动项，兼容字符串和字典两种输入。"""
            if not isinstance(item, dict):
                text = str(item or "").strip()
                return f"<li>{render_rich_text(text)}</li>" if text else ""
            owner = first_dict_value(item, ("owner", "name", "participant_ref"))
            task = first_dict_value(item, ("task", "content", "action", "summary", "text"))
            deadline = first_dict_value(item, ("deadline", "time", "due"))
            status_hint = first_dict_value(item, ("status_hint", "status"))
            meta = " / ".join(part for part in (deadline, status_hint) if part)
            meta_html = f' <span class="meta">{html.escape(meta)}</span>' if meta else ""
            if owner and task:
                return f"<li><strong>{render_member_field(owner)}</strong>：{render_rich_text(task)}{meta_html}</li>"
            if task:
                return f"<li>{render_rich_text(task)}{meta_html}</li>"
            if owner:
                return f"<li>{render_member_field(owner)}{meta_html}</li>"
            return ""

        def render_text_item(item: Any, keys: tuple[str, ...]) -> str:
            """渲染风险、问题等简单文本列表项。"""
            if isinstance(item, dict):
                text = first_dict_value(item, keys)
            else:
                text = str(item or "")
            text = text.strip()
            return f"<li>{render_rich_text(text)}</li>" if text else ""

        action_items_html = ''.join(
            render_action_item(item)
            for item in report.get('action_items', [])[:8]
        )

        open_questions_html = ''.join(
            render_text_item(item, ("question", "content", "text", "summary"))
            for item in report.get('open_questions', [])[:8]
        )
        risk_flags_html = ''.join(
            render_text_item(item, ("risk", "flag", "content", "text", "summary", "reason"))
            for item in report.get('risk_flags', [])[:8]
        )

        action_items_section_html = (
            f'<section class="aside-card"><h2 class="aside-title">行动项</h2><ul class="simple-list">{action_items_html}</ul></section>'
            if action_items_html
            else ''
        )
        open_questions_section_html = (
            f'<section class="aside-card"><h2 class="aside-title">未决问题</h2><ul class="simple-list">{open_questions_html}</ul></section>'
            if open_questions_html
            else ''
        )
        risk_flags_section_html = (
            f'<section class="aside-card"><h2 class="aside-title">风险提示</h2><ul class="simple-list">{risk_flags_html}</ul></section>'
            if risk_flags_html
            else ''
        )

        word_cloud_items = stats.get('word_cloud', [])[:28]
        max_word_count = max((item['count'] for item in word_cloud_items), default=1)
        min_word_count = min((item['count'] for item in word_cloud_items), default=1)
        word_cloud_html = ''.join(
            f'<span class="cloud-item" style="font-size:{15 + ((item["count"] - min_word_count) / max(1, max_word_count - min_word_count)) * 17:.1f}px">{html.escape(item["word"])}</span>'
            for item in word_cloud_items
        )

        return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(report.get('headline', '群洞察报表'))}</title>
  <style>
    :root {{
      --bg: #f8fff4;
      --bg-soft: #fefce8;
      --panel: #ffffff;
      --panel-soft: #fffdf5;
      --ink: #23452d;
      --ink-light: #6b7f71;
      --line: rgba(35, 69, 45, 0.10);
      --shadow: 0 10px 28px rgba(92, 135, 83, 0.10);
      --spring-green: #7bc96f;
      --spring-green-dark: #4d9f5f;
      --spring-yellow: #ffd86b;
      --spring-pink: #ffc4d6;
      --spring-pink-dark: #e16a97;
      --mention-blue: #2563eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; color: var(--ink);
      background: linear-gradient(180deg, #fffef7, var(--bg)); }}
    .page {{ width: min(100%, 520px); margin: 0 auto; padding: 8px; }}
    .hero {{ border-radius: 8px; background:
      linear-gradient(160deg, var(--spring-green), #9bd58a 45%, var(--spring-yellow));
      padding: 16px 14px; box-shadow: var(--shadow); color: #193321; }}
    .eyebrow {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: rgba(255,255,255,0.55); font-size: 12px; font-weight: 700; }}
    .title-cn {{ margin: 10px 0 0; font-size: 32px; font-weight: 900; line-height: 1.12; }}
    .hero-meta {{ margin-top: 10px; display: grid; gap: 4px; font-size: 14px; line-height: 1.55; }}
    .hero-meta .chat-name {{ font-size: 22px; font-weight: 800; }}
    .hero-stats {{ margin-top: 12px; display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }}
    .stat-card {{ border-radius: 8px; padding: 10px 10px; background: rgba(255,255,255,0.55); border: 1px solid rgba(255,255,255,0.65); }}
    .stat-label {{ font-size: 12px; opacity: 0.86; }}
    .stat-value {{ margin-top: 4px; font-size: 22px; font-weight: 900; }}
    .section {{ margin-top: 10px; border-radius: 8px; background: var(--panel); box-shadow: var(--shadow); padding: 14px 12px; }}
    .lead-box {{ border-radius: 8px; background: linear-gradient(180deg, #f0fdf4, #fef9c3); color: var(--ink); padding: 12px; font-size: 16px; line-height: 1.7; font-weight: 600; }}
    .theme-grid {{ display: grid; grid-template-columns: 1fr; gap: 8px; margin-top: 10px; }}
    .theme-card {{ padding: 12px; border-radius: 8px; background: linear-gradient(180deg, #fffdf5, #fff7fb); border: 1px solid rgba(225, 106, 151, 0.15); }}
    .theme-title {{ font-size: 17px; font-weight: 900; color: var(--spring-pink-dark); }}
    .theme-summary {{ margin-top: 6px; line-height: 1.65; color: var(--ink-light); font-size: 15px; }}
    .content-grid {{ display: grid; grid-template-columns: 1fr; gap: 10px; margin-top: 10px; }}
    .section-card {{ display: grid; grid-template-columns: 32px 1fr; gap: 8px; padding: 10px 0; border-bottom: 1px solid var(--line); }}
    .section-card:last-child {{ border-bottom: none; padding-bottom: 0; }}
    .section-index {{ width: 28px; height: 28px; border-radius: 50%; background: var(--spring-green); color: #fff; display: flex; align-items: center; justify-content: center; font-size: 14px; font-weight: 900; margin-top: 1px; }}
    .section-header {{ display: block; }}
    .section-header h3 {{ margin: 0; font-size: 18px; line-height: 1.4; color: var(--ink); }}
    .section-time {{ margin-top: 4px; font-size: 12px; color: var(--ink-light); }}
    .section-summary {{ margin: 8px 0 8px; line-height: 1.72; font-size: 16px; }}
    .section-bullets {{ margin: 0; padding-left: 18px; line-height: 1.72; color: var(--ink-light); font-size: 15px; }}
    .section-takeaway {{ margin-top: 8px; padding-top: 8px; border-top: 1px dashed var(--line); color: var(--spring-green-dark); font-size: 15px; line-height: 1.65; font-weight: 700; }}
    .aside-card {{ border-radius: 8px; padding: 12px; background: var(--panel); border: 1px solid var(--line); box-shadow: var(--shadow); }}
    .aside-title {{ font-size: 17px; font-weight: 900; color: var(--ink); margin: 0 0 8px; }}
    .leaderboard {{ list-style: none; margin: 0; padding: 0; display: grid; gap: 10px; }}
    .leaderboard li {{ display: grid; grid-template-columns: 28px 1fr auto; align-items: center; gap: 8px; }}
    .rank-badge {{ width: 26px; height: 26px; border-radius: 50%; background: #fef3c7; color: #92400e; display: flex; align-items: center; justify-content: center; font-weight: 900; font-size: 12px; }}
    .speaker-name {{ font-weight: 700; }} .speaker-count, .meta {{ color: var(--ink-light); font-size: 14px; }}
    .simple-list {{ margin: 0; padding-left: 18px; line-height: 1.7; font-size: 15px; }}
    .interaction-list {{ display: grid; gap: 10px; }}
    .interaction-group {{ border-top: 1px solid var(--line); padding-top: 8px; }}
    .interaction-group:first-child {{ border-top: 0; padding-top: 0; }}
    .interaction-title {{ margin-bottom: 6px; font-weight: 900; color: var(--spring-green-dark); }}
    blockquote {{ margin: 0 0 8px; padding: 12px; border-radius: 8px; background: linear-gradient(180deg, #fffdf5, #fdf2f8); border: 1px solid rgba(225, 106, 151, 0.15); }}
    .quote-text {{ font-size: 16px; line-height: 1.7; font-weight: 700; }}
    .quote-meta, .quote-why {{ margin-top: 6px; color: var(--ink-light); font-size: 14px; line-height: 1.55; }}
    .quote-time {{ color: var(--ink-light); font-size: 12px; }}
    .stack {{ display: grid; gap: 10px; }}
    .mention {{ color: var(--mention-blue); font-weight: 700; }}
    .word-cloud {{ display: flex; flex-wrap: wrap; gap: 8px 10px; align-items: center; }}
    .cloud-item {{ display: inline-flex; align-items: center; padding: 4px 8px; border-radius: 999px; background: #fdf2f8; color: #9d174d; line-height: 1.2; font-size: 14px; }}
    .footer {{ margin-top: 12px; color: var(--ink-light); font-size: 12px; line-height: 1.6; text-align: center; }}
    @media (min-width: 680px) {{
      .page {{ width: min(100%, 640px); }}
      .hero-stats {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <div class="eyebrow">群聊分析</div>
      <h1 class="title-cn">群聊总结</h1>
      <div class="hero-meta">
        <div class="chat-name">{html.escape(chat_name)}</div>
        <div>统计区间：{html.escape(start_time)} - {html.escape(end_time)}</div>
        <div>{render_rich_text(report.get('tagline', ''))}</div>
      </div>
      <div class="hero-stats">
        <div class="stat-card"><div class="stat-label">今日信息数</div><div class="stat-value">{stats.get('effective_message_count', stats['message_count'])}</div></div>
        <div class="stat-card"><div class="stat-label">今日字数</div><div class="stat-value">{stats.get('effective_char_count', stats.get('raw_char_count', 0))}</div></div>
        <div class="stat-card"><div class="stat-label">参与人数</div><div class="stat-value">{stats['participant_count']}</div></div>
      </div>
    </header>
    <section class="section">
      <div class="lead-box">{render_rich_text(report.get('lead_summary', ''))}</div>
      <div class="theme-grid">{theme_cards_html}</div>
    </section>
    <div class="content-grid">
      <section class="section">
        <h2 class="aside-title">讨论脉络</h2>
        {sections_html}
      </section>
      <div class="stack">
        <section class="aside-card"><h2 class="aside-title">高频词云</h2><div class="word-cloud">{word_cloud_html or '<span class="cloud-item">暂无</span>'}</div></section>
        <section class="aside-card"><h2 class="aside-title">发言排行</h2><ol class="leaderboard">{leaderboard_html}</ol></section>
        <section class="aside-card"><h2 class="aside-title">互动榜单</h2><div class="interaction-list">{interaction_rankings_html}</div></section>
        <section class="aside-card"><h2 class="aside-title">成员观察</h2><ul class="simple-list">{participant_notes_html or '<li>暂无</li>'}</ul></section>
        <section class="aside-card"><h2 class="aside-title">引用原话</h2>{quotes_html or '<div class="quote-why">当前没有可展示的原话。</div>'}</section>
        {action_items_section_html}
        {open_questions_section_html}
        {risk_flags_section_html}
      </div>
    </div>
    <div class="footer">
      <div>{html.escape(report.get('headline', '群洞察报表'))}</div>
      <div>生成时间：{html.escape(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</div>
      <div>本地统计来自已解密数据库，语义摘要由模型生成。</div>
    </div>
  </div>
</body>
</html>
"""


def build_report_payload(
    ctx: dict[str, Any],
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    report: dict[str, Any],
    chunk_count: int,
    chunk_plan: dict[str, Any],
    dry_run: bool,
    provider: str,
    model: str,
) -> dict[str, Any]:
    """构造写入 JSON 文件的完整报表载荷。"""
    return {
        "metadata": {
            "chat_name": ctx["display_name"],
            "chat_id": ctx["username"],
            "start_time": start_time,
            "end_time": end_time,
            "chunk_count": chunk_count,
            "chunk_strategy": chunk_plan.get("strategy", ""),
            "chunk_mode": chunk_plan.get("mode", ""),
            "estimated_tokens": chunk_plan.get("estimated_tokens", 0),
            "dry_run": dry_run,
            "provider": provider,
            "model": model,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "stats": stats,
        "report": report,
    }


def invalidate_cached_outputs_if_needed(
    output_dir: Path,
    signature: dict[str, Any],
) -> None:
    """当运行签名变化时删除过期阶段缓存和导出物。"""
    signature_path = output_dir / "snapshot" / "run_signature.json"
    if not signature_path.exists():
        previous_signature = None
    else:
        try:
            previous_signature = json.loads(signature_path.read_text(encoding="utf-8"))
        except Exception:
            previous_signature = {}

    if previous_signature == signature:
        return

    # 输入签名变化意味着阶段产物不再可信，统一清理后让本次运行重新生成。
    for dirname in ["map", "reduce", "final"]:
        target_dir = output_dir / dirname
        if target_dir.exists():
            shutil.rmtree(target_dir)

    for filename in ["group_insight_report.json", "group_insight_report.html", "group_insight_report.png"]:
        target_file = output_dir / filename
        if target_file.exists():
            target_file.unlink()
