"""微信消息拉取与成员身份解析。

连接 wechat-decrypt 数据库，把时间范围内的原始消息行转换为
StructuredMessage，并完成成员 ID → 显示名的归一化。
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

from .common import format_ts, make_user_placeholder, normalize_text
from .models import StructuredMessage
from .settings import MAX_LINE_TEXT_LEN, WECHAT_DECRYPT_DIR, wechat_mcp
from .rich_content import extract_rich_message_metadata

_GROUP_NICKNAME_CACHE: dict[str, dict[str, str]] = {}


def _looks_like_wechat_username(value: str) -> bool:
    """判断字符串是否像微信账号或群成员 ID。"""
    return bool(
        value
        and (
            value.startswith("wxid_")
            or value.startswith("gh_")
            or value.endswith("@chatroom")
            or re.fullmatch(r"[A-Za-z0-9_]{4,}", value) is not None
        )
    )


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    """从 protobuf 字节流当前位置读取 varint。"""
    value = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, pos
        shift += 7
    raise ValueError("incomplete varint")


def _parse_proto_fields(data: bytes) -> list[tuple[int, int, Any]]:
    """以宽松方式解析 protobuf 字段，供群昵称映射提取使用。"""
    pos = 0
    fields: list[tuple[int, int, Any]] = []
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_no = tag >> 3
        wire_type = tag & 0x07
        if wire_type == 0:
            value, pos = _read_varint(data, pos)
        elif wire_type == 1:
            value = data[pos : pos + 8]
            pos += 8
        elif wire_type == 2:
            length, pos = _read_varint(data, pos)
            value = data[pos : pos + length]
            pos += length
        elif wire_type == 5:
            value = data[pos : pos + 4]
            pos += 4
        else:
            break
        fields.append((field_no, wire_type, value))
    return fields


def _collect_group_nicknames(blob: bytes, mapping: dict[str, str], depth: int = 0) -> None:
    """递归扫描 protobuf 字段，收集群成员 ID 到昵称的映射。"""
    if not blob or depth > 6:
        return
    try:
        fields = _parse_proto_fields(blob)
    except Exception:
        return

    string_fields: dict[int, str] = {}
    nested_payloads: list[bytes] = []
    for field_no, wire_type, value in fields:
        if wire_type != 2:
            continue
        if isinstance(value, bytes):
            try:
                decoded = value.decode("utf-8")
            except UnicodeDecodeError:
                decoded = ""
            if decoded:
                string_fields[field_no] = decoded
            nested_payloads.append(value)

    username = string_fields.get(1, "").strip()
    nickname = string_fields.get(2, "").strip()
    if _looks_like_wechat_username(username) and nickname:
        mapping[username] = nickname

    for payload in nested_payloads:
        _collect_group_nicknames(payload, mapping, depth + 1)


def get_group_nickname_map(chat_id: str) -> dict[str, str]:
    """读取并缓存指定群聊的成员昵称映射。"""
    if chat_id in _GROUP_NICKNAME_CACHE:
        return _GROUP_NICKNAME_CACHE[chat_id]

    decrypted_dir = Path(getattr(wechat_mcp, "DECRYPTED_DIR", WECHAT_DECRYPT_DIR / "decrypted"))
    contact_db = decrypted_dir / "contact" / "contact.db"
    mapping: dict[str, str] = {}
    if not contact_db.exists():
        _GROUP_NICKNAME_CACHE[chat_id] = mapping
        return mapping

    conn = wechat_mcp.sqlite3.connect(str(contact_db))
    try:
        row = conn.execute(
            "SELECT ext_buffer FROM chat_room WHERE username = ?",
            (chat_id,),
        ).fetchone()
    finally:
        conn.close()

    if row and row[0]:
        _collect_group_nicknames(row[0], mapping)

    _GROUP_NICKNAME_CACHE[chat_id] = mapping
    return mapping


def infer_sender_display_from_text(text: str) -> str:
    """从格式化文本中反推可能的发送人显示名。"""
    patterns = [
        r'^\[链接/文件\] "([^"]+)" 拍了拍',
        r'^\[系统\] "([^"]+)" 撤回了一条消息',
        r'^\[系统\] (.+?)发起了语音通话',
        r'^\[系统\] (.+?)结束了语音通话',
    ]
    for pattern in patterns:
        match = re.match(pattern, text or "")
        if match:
            return match.group(1).strip()
    return ""


def resolve_sender_identity(
    ctx: dict[str, Any],
    names: dict[str, str],
    group_nicknames: dict[str, str],
    id_to_username: dict[int, str],
    real_sender_id: int,
    sender_from_content: str,
    text: str,
) -> tuple[str, str]:
    """综合数据库 ID、联系人表、群昵称和文本内容确定发送人身份。"""
    sender_username = id_to_username.get(real_sender_id, "") or sender_from_content or ""

    if sender_username:
        if ctx["is_group"]:
            if sender_username in group_nicknames:
                return sender_username, group_nicknames[sender_username]
        return sender_username, names.get(sender_username, sender_username)

    if ctx["is_group"]:
        inferred_display = infer_sender_display_from_text(text)
        if inferred_display:
            return "", inferred_display
        return "", "unknown"
    return "", ctx["display_name"] if not ctx["is_group"] else "unknown"


def resolve_member_display_name(
    username: str,
    names: dict[str, str],
    group_nicknames: dict[str, str],
    fallback: str = "",
) -> str:
    """把成员 ID 解析为更适合展示的昵称。"""
    username = (username or "").strip()
    fallback = (fallback or "").strip()
    if not username:
        return fallback
    return group_nicknames.get(username) or names.get(username) or fallback or username


def is_resolved_member_display(username: str, display_name: str) -> bool:
    """判断显示名是否已经脱离原始账号占位。"""
    username = (username or "").strip()
    display_name = (display_name or "").strip()
    if not username or not display_name or display_name == username:
        return False
    if display_name.startswith(("wxid_", "gh_")) or display_name.endswith("@chatroom"):
        return False
    return True


def collect_member_aliases_from_messages(messages: list[StructuredMessage]) -> dict[str, str]:
    """从已结构化消息中汇总可展示的成员别名。"""
    aliases: dict[str, str] = {}

    def add_alias(username: str, display_name: str) -> None:
        """把一个成员 ID 与有效显示名加入别名表。"""
        username = (username or "").strip()
        display_name = (display_name or "").strip()
        if is_resolved_member_display(username, display_name):
            aliases[username] = display_name

    for message in messages:
        metadata = message.metadata or {}
        add_alias(message.sender_username, message.sender)
        for username_key, name_key in (
            ("reply_to_username", "reply_to_name"),
            ("pat_from_username", "pat_from_name"),
            ("pat_to_username", "pat_to_name"),
            ("redpacket_sender_username", "redpacket_sender_name"),
            ("redpacket_receiver_username", "redpacket_receiver_name"),
        ):
            add_alias(metadata.get(username_key, ""), metadata.get(name_key, ""))
    return aliases


def enrich_interaction_metadata(
    metadata: dict[str, Any],
    names: dict[str, str],
    group_nicknames: dict[str, str],
) -> dict[str, Any]:
    """用联系人名和群昵称补全互动类消息的展示字段。"""
    if not metadata:
        return metadata

    kind = metadata.get("interaction_kind", "")
    if kind == "reply":
        metadata["reply_to_name"] = resolve_member_display_name(
            metadata.get("reply_to_username", ""),
            names,
            group_nicknames,
            metadata.get("reply_to_name", ""),
        )
    elif kind == "pat":
        metadata["pat_from_name"] = resolve_member_display_name(
            metadata.get("pat_from_username", ""),
            names,
            group_nicknames,
            metadata.get("pat_from_name", ""),
        )
        metadata["pat_to_name"] = resolve_member_display_name(
            metadata.get("pat_to_username", ""),
            names,
            group_nicknames,
            metadata.get("pat_to_name", ""),
        )
    elif kind in {"direct_redpacket", "redpacket"}:
        metadata["redpacket_sender_name"] = resolve_member_display_name(
            metadata.get("redpacket_sender_username", ""),
            names,
            group_nicknames,
            metadata.get("redpacket_sender_name", ""),
        )
        metadata["redpacket_receiver_name"] = resolve_member_display_name(
            metadata.get("redpacket_receiver_username", ""),
            names,
            group_nicknames,
            metadata.get("redpacket_receiver_name", ""),
        )
    return metadata


def fetch_structured_messages(
    chat_ref: str,
    start_time: str,
    end_time: str,
    batch_size: int = 500,
) -> tuple[dict[str, Any], list[StructuredMessage]]:
    """从微信数据库查询时间窗内消息，并转换为结构化消息列表。"""
    ctx = wechat_mcp._resolve_chat_context(chat_ref)
    if not ctx:
        raise ValueError(f"找不到聊天对象: {chat_ref}")
    if not ctx["message_tables"]:
        raise ValueError(f"{ctx['display_name']} 没有可查询的消息表")

    start_ts, end_ts = wechat_mcp._parse_time_range(start_time, end_time)
    names = wechat_mcp.get_contact_names()
    group_nicknames = get_group_nickname_map(ctx["username"]) if ctx["is_group"] else {}
    collected: list[StructuredMessage] = []
    seen_ids: set[str] = set()
    dedupe_fingerprints: set[tuple[Any, ...]] = set()

    for table_ctx in wechat_mcp._iter_table_contexts(ctx):
        conn = wechat_mcp.sqlite3.connect(table_ctx["db_path"])
        try:
            id_to_username = wechat_mcp._load_name2id_maps(conn)
            fetch_offset = 0
            while True:
                rows = wechat_mcp._query_messages(
                    conn,
                    table_ctx["table_name"],
                    start_ts=start_ts,
                    end_ts=end_ts,
                    limit=batch_size,
                    offset=fetch_offset,
                )
                if not rows:
                    break
                fetch_offset += len(rows)

                for row in rows:
                    local_id, local_type, create_time, real_sender_id, content, ct = row
                    content = wechat_mcp._decompress_content(content, ct)
                    if content is None:
                        content = "(无法解压)"
                    metadata = extract_rich_message_metadata(
                        content,
                        local_type,
                        table_ctx["is_group"],
                    )
                    sender, text = wechat_mcp._format_message_text(
                        local_id,
                        local_type,
                        content,
                        table_ctx["is_group"],
                        table_ctx["username"],
                        table_ctx["display_name"],
                        names,
                    )
                    if metadata.get("analysis_text"):
                        text = metadata["analysis_text"]
                    sender_label = wechat_mcp._resolve_sender_label(
                        real_sender_id,
                        sender,
                        table_ctx["is_group"],
                        table_ctx["username"],
                        table_ctx["display_name"],
                        names,
                        id_to_username,
                    )
                    normalized_text = normalize_text(text or "(无内容)", max_len=MAX_LINE_TEXT_LEN)
                    sender_username, sender_display = resolve_sender_identity(
                        table_ctx,
                        names,
                        group_nicknames,
                        id_to_username,
                        real_sender_id,
                        sender,
                        normalized_text,
                    )
                    metadata = enrich_interaction_metadata(metadata, names, group_nicknames)
                    fingerprint = (
                        ctx["username"],
                        create_time,
                        sender_username or sender_label or sender_display,
                        wechat_mcp.format_msg_type(local_type),
                        normalized_text,
                    )
                    if fingerprint in dedupe_fingerprints:
                        continue
                    dedupe_fingerprints.add(fingerprint)

                    message_id_seed = (
                        f"{ctx['username']}|{create_time}|{sender_username}|{local_type}|{normalized_text}"
                    )
                    message_id = "m_" + hashlib.sha1(message_id_seed.encode("utf-8")).hexdigest()[:16]
                    if message_id in seen_ids:
                        continue
                    seen_ids.add(message_id)
                    collected.append(
                        StructuredMessage(
                            id=message_id,
                            local_id=local_id,
                            timestamp=create_time,
                            time=format_ts(create_time),
                            sender_username=sender_username or "",
                            sender=sender_display or sender_label or "unknown",
                            text=normalized_text,
                            msg_type=wechat_mcp.format_msg_type(local_type),
                            chat_id=ctx["username"],
                            chat_name=ctx["display_name"],
                            table_name=table_ctx["table_name"],
                            metadata=metadata,
                        )
                    )

                if len(rows) < batch_size:
                    break
        finally:
            conn.close()

    # 群聊报表优先使用“本群昵称”，避免被联系人备注覆盖。
    member_aliases = {**collect_member_aliases_from_messages(collected), **group_nicknames}
    for message in collected:
        alias = member_aliases.get(message.sender_username or "")
        if alias and not is_resolved_member_display(message.sender_username, message.sender):
            message.sender = alias

    collected.sort(key=lambda item: (item.timestamp, item.local_id, item.id))
    return ctx, collected
