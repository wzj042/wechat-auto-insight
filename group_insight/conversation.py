"""消息抽取、清洗、分类、分片与本地统计工具。

本模块连接微信 MCP 查询层与后续 LLM 流水线：先把原始数据库行整理成
StructuredMessage，再按分析口径过滤、分片并压缩成 prompt 友好的载荷。
"""
from __future__ import annotations

from .models import *
from .settings import _GROUP_NICKNAME_CACHE


def slugify(value: str) -> str:
    """把群名等任意文本转换为适合目录名的短标识。"""
    value = value.strip()
    value = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "group-insight"


def format_ts(ts: int) -> str:
    """将秒级时间戳格式化为报表使用的分钟级时间。"""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def normalize_text(value: str, max_len: int | None = None) -> str:
    """折叠连续空白，并在需要时按最大长度截断文本。"""
    value = re.sub(r"\s+", " ", value or "").strip()
    if max_len and len(value) > max_len:
        cut = max(0, max_len - 3)
        open_pos = value.rfind("[[user:", 0, cut)
        close_pos = value.find("]]", open_pos + 7) if open_pos >= 0 else -1
        if open_pos >= 0 and (close_pos < 0 or close_pos + 2 > cut):
            cut = open_pos
        return value[:cut].rstrip() + "..."
    return value


def strip_wechat_emoji_shortcodes(value: str) -> str:
    """移除微信表情短码，避免低信息噪声进入词频统计。"""
    return WECHAT_EMOJI_SHORTCODE_PATTERN.sub(" ", value or "")


def collapse_text(value: str, max_len: int | None = None) -> str:
    """统一调用 normalize_text，保留旧命名下的文本压缩入口。"""
    return normalize_text(value, max_len=max_len)


def parse_xml_root_with_limit(content: str, max_len: int) -> ET.Element | None:
    """在长度和安全字符检查通过后解析 XML 根节点。"""
    content = (content or "").strip()
    if not content or len(content) > max_len:
        return None
    unsafe_re = getattr(wechat_mcp, "_XML_UNSAFE_RE", None)
    if unsafe_re is not None and unsafe_re.search(content):
        return None
    try:
        return ET.fromstring(content)
    except ET.ParseError:
        return None


def parse_int(value: Any, fallback: int = 0) -> int:
    """容错地把输入转换为整数，失败时返回 fallback。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def summarize_record_items(items: list[dict[str, str]], max_items: int = 4) -> str:
    """把合并聊天记录中的若干条目压缩成一行摘要。"""
    snippets = []
    for item in items[:max_items]:
        name = collapse_text(item.get("name", ""), max_len=24)
        text = collapse_text(item.get("text", ""), max_len=60)
        if not text:
            continue
        snippets.append(f"{name}: {text}" if name else text)
    return "；".join(snippets)


def build_rich_card_preview(metadata: dict[str, Any], fallback_text: str) -> str:
    """根据富媒体元数据生成适合进入 LLM 的可读预览文本。"""
    kind = metadata.get("rich_kind", "")
    title = collapse_text(metadata.get("title", ""), max_len=90)
    summary = collapse_text(metadata.get("summary", ""), max_len=120)
    source = collapse_text(metadata.get("source", ""), max_len=30)
    if kind == "link_card":
        parts = [f"[链接] {title}" if title else "[链接]"]
        if summary:
            parts.append(f"摘要：{summary}")
        if source:
            parts.append(f"来源：{source}")
        return "；".join(parts)
    if kind == "merged_chat":
        parts = [f"[聊天记录] {title}" if title else "[聊天记录]"]
        if summary:
            parts.append(f"摘要：{summary}")
        item_summary = summarize_record_items(metadata.get("items", []))
        if item_summary:
            parts.append(f"包含：{item_summary}")
        return "；".join(parts)
    return fallback_text


def parse_pat_title_names(title: str) -> tuple[str, str]:
    """从拍一拍标题里解析发起人与目标显示名。"""
    match = re.search(r'(?:"([^"]+)"|(我))\s*拍了拍\s*"([^"]+)"', title or "")
    if not match:
        return "", ""
    return (match.group(1) or match.group(2) or "").strip(), match.group(3).strip()


def parse_query_param(url: str, name: str) -> str:
    """从 URL 查询串中读取指定参数的第一个值。"""
    if not url:
        return ""
    try:
        values = urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get(name, [])
    except ValueError:
        return ""
    return values[0].strip() if values else ""


def extract_rich_message_metadata(
    raw_content: str,
    local_type: int,
    is_group: bool,
) -> dict[str, Any]:
    """解析微信 appmsg XML，提取链接、合并聊天、回复、拍一拍和红包等元数据。"""
    if not raw_content:
        return {}
    base_type, sub_type = wechat_mcp._split_msg_type(local_type)
    if base_type != 49:
        return {}

    _, message_content = wechat_mcp._parse_message_content(raw_content, local_type, is_group)
    if "<appmsg" not in (message_content or ""):
        return {}

    root = parse_xml_root_with_limit(message_content, APPMSG_XML_MAX_LEN)
    if root is None:
        return {}
    appmsg = root.find(".//appmsg")
    if appmsg is None:
        return {}

    title = collapse_text(appmsg.findtext("title") or "", max_len=120)
    summary = collapse_text(appmsg.findtext("des") or "", max_len=180)
    source = collapse_text(appmsg.findtext("sourcedisplayname") or "", max_len=40)
    url = collapse_text((appmsg.findtext("url") or "").replace("&amp;", "&"), max_len=400)
    app_type = parse_int(appmsg.findtext("type") or sub_type, parse_int(sub_type, 0))

    if app_type == 57:
        ref = appmsg.find(".//refermsg")
        reply_to_username = ""
        reply_to_display_name = ""
        reply_to_content = ""
        if ref is not None:
            reply_to_username = (ref.findtext("chatusr") or "").strip()
            if not reply_to_username:
                reply_to_username = (ref.findtext("fromusr") or "").strip()
            reply_to_display_name = collapse_text(ref.findtext("displayname") or "", max_len=60)
            reply_to_content = collapse_text(ref.findtext("content") or "", max_len=160)
        return {
            "interaction_kind": "reply",
            "reply_to_username": reply_to_username,
            "reply_to_name": reply_to_display_name,
            "reply_to_content": reply_to_content,
            "app_type": app_type,
        }

    if app_type == 62:
        pat_actor_name, pat_target_name = parse_pat_title_names(title)
        patinfo = appmsg.find(".//patinfo")
        return {
            "interaction_kind": "pat",
            "title": title,
            "pat_from_username": (patinfo.findtext("fromusername") or "").strip() if patinfo is not None else "",
            "pat_to_username": (patinfo.findtext("pattedusername") or "").strip() if patinfo is not None else "",
            "pat_from_name": pat_actor_name,
            "pat_to_name": pat_target_name,
            "app_type": app_type,
        }

    if app_type == 2001:
        native_url = appmsg.findtext(".//wcpayinfo/nativeurl") or ""
        sender_username = (root.findtext("fromusername") or "").strip()
        if not sender_username:
            sender_username = parse_query_param(native_url, "sendusername")
        receiver_username = (appmsg.findtext(".//wcpayinfo/exclusive_recv_username") or "").strip()
        return {
            "interaction_kind": "direct_redpacket" if receiver_username else "redpacket",
            "title": title,
            "summary": summary,
            "redpacket_sender_username": sender_username,
            "redpacket_receiver_username": receiver_username,
            "redpacket_memo": collapse_text(appmsg.findtext(".//wcpayinfo/receivertitle") or "", max_len=80),
            "app_type": app_type,
        }

    if app_type == 5:
        metadata = {
            "rich_kind": "link_card",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "app_type": app_type,
        }
        metadata["analysis_text"] = build_rich_card_preview(metadata, f"[链接] {title}" if title else "[链接]")
        return metadata

    if app_type == 19:
        items: list[dict[str, str]] = []
        recorditem = appmsg.findtext("recorditem") or ""
        if recorditem:
            ri_root = parse_xml_root_with_limit(recorditem, RECORDITEM_XML_MAX_LEN)
            if ri_root is not None:
                for dataitem in ri_root.findall(".//dataitem"):
                    name = collapse_text(dataitem.findtext("sourcename") or "", max_len=24)
                    text = collapse_text(dataitem.findtext("datadesc") or "", max_len=80)
                    if not text:
                        continue
                    items.append({"name": name, "text": text})
                    if len(items) >= 8:
                        break
        metadata = {
            "rich_kind": "merged_chat",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "items": items,
            "app_type": app_type,
        }
        metadata["analysis_text"] = build_rich_card_preview(metadata, f"[聊天记录] {title}" if title else "[聊天记录]")
        return metadata

    if app_type in (33, 36, 44):
        metadata = {
            "rich_kind": "link_card",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "app_type": app_type,
        }
        metadata["analysis_text"] = build_rich_card_preview(metadata, f"[小程序] {title}" if title else "[小程序]")
        return metadata

    if app_type == 6:
        return {
            "rich_kind": "file_card",
            "title": title,
            "summary": summary,
            "source": source,
            "url": url,
            "app_type": app_type,
        }

    return {}


def has_meaningful_rich_content(message: StructuredMessage) -> bool:
    """判断富媒体消息是否有足够内容进入语义分析。"""
    metadata = message.metadata or {}
    kind = metadata.get("rich_kind", "")
    if kind == "link_card":
        return bool(metadata.get("title") or metadata.get("summary"))
    if kind == "merged_chat":
        return bool(metadata.get("title") or metadata.get("summary") or metadata.get("items"))
    return False


def ensure_dir(path: Path) -> Path:
    """创建目录并返回 Path，供各阶段输出目录复用。"""
    path.mkdir(parents=True, exist_ok=True)
    return path


def make_user_placeholder(sender_username: str) -> str:
    """把成员账号转换为模型可保留的成员占位符。"""
    sender_username = (sender_username or "").strip()
    return f"[[user:{sender_username}]]" if sender_username else ""


def safe_json_loads(payload: str) -> Any:
    """解析模型返回的 JSON，并在前后夹杂文本时尝试抽取对象。"""
    payload = (payload or "").strip()
    if not payload:
        raise ValueError("empty json payload")
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload)
        payload = re.sub(r"\s*```$", "", payload)

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        candidate = extract_json_object(payload)
        if candidate:
            return json.loads(candidate)
        raise


def extract_json_object(payload: str) -> str:
    """从任意文本中提取第一个括号配平的 JSON 对象片段。"""
    start = payload.find("{")
    if start < 0:
        return ""

    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(payload)):
        char = payload[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return payload[start : index + 1]
    return ""


def write_json(path: Path, payload: Any) -> None:
    """以 UTF-8 和缩进格式写入 JSON 文件。"""
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
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


def extract_topic_tokens(text: str) -> set[str]:
    """抽取用于分片相似度和词云统计的主题 token。"""
    normalized = normalize_text(text)
    normalized = re.sub(r"\[(图片|表情|语音|链接/文件|通话)[^\]]*\]", " ", normalized)
    tokens: set[str] = set()
    for match in TOKEN_PATTERN.findall(normalized):
        token = match.lower()
        if len(token) >= 2:
            tokens.add(token)
        if re.fullmatch(r"[\u4e00-\u9fff]{4,}", token):
            for index in range(len(token) - 1):
                tokens.add(token[index : index + 2])
    return tokens


def estimate_text_tokens(text: str) -> int:
    """用字符数粗略估算 prompt token 数。"""
    text = (text or "").strip()
    if not text:
        return 0

    cjk_chars = len(SINGLE_CJK_PATTERN.findall(text))
    words = WORD_TOKEN_PATTERN.findall(text)
    word_token_estimate = sum(max(1, math.ceil(len(word) / 4)) for word in words)
    consumed = cjk_chars + sum(len(word) for word in words)
    remaining_chars = max(0, len(text) - consumed)
    remaining_estimate = math.ceil(remaining_chars * 0.5)
    cjk_estimate = math.ceil(cjk_chars * 0.65)
    return max(1, cjk_estimate + word_token_estimate + remaining_estimate)


def estimate_message_tokens(message: StructuredMessage) -> int:
    """估算单条结构化消息进入模型时占用的 token。"""
    base = estimate_text_tokens(message.text)
    sender_cost = max(1, math.ceil(len(message.sender) / 3))
    return base + sender_cost + 8


def estimate_deepseek_cost_usd(
    input_tokens: int,
    output_tokens: int,
    *,
    cache_hit: bool = False,
) -> float:
    """按 DeepSeek 价格参数估算一次请求成本。"""
    input_rate = (
        DEEPSEEK_INPUT_CACHE_HIT_USD_PER_M_TOKEN
        if cache_hit
        else DEEPSEEK_INPUT_CACHE_MISS_USD_PER_M_TOKEN
    )
    return (
        (input_tokens * input_rate)
        + (output_tokens * DEEPSEEK_OUTPUT_USD_PER_M_TOKEN)
    ) / 1_000_000


def estimate_deepseek_usage_cost_usd(usage: dict[str, Any]) -> float:
    """根据接口 usage 字段估算 DeepSeek 实际计费成本。"""
    hit_tokens = int(usage.get("prompt_cache_hit_tokens") or 0)
    miss_tokens = int(usage.get("prompt_cache_miss_tokens") or 0)
    prompt_tokens = int(usage.get("prompt_tokens") or 0)
    completion_tokens = int(usage.get("completion_tokens") or 0)
    if not hit_tokens and not miss_tokens:
        miss_tokens = prompt_tokens
    return (
        (hit_tokens * DEEPSEEK_INPUT_CACHE_HIT_USD_PER_M_TOKEN)
        + (miss_tokens * DEEPSEEK_INPUT_CACHE_MISS_USD_PER_M_TOKEN)
        + (completion_tokens * DEEPSEEK_OUTPUT_USD_PER_M_TOKEN)
    ) / 1_000_000


def format_usd(value: float) -> str:
    """把美元成本格式化为固定精度字符串。"""
    if value < 0.0001:
        return f"${value:.6f}"
    if value < 0.01:
        return f"${value:.5f}"
    return f"${value:.4f}"


def estimate_prompt_tokens(system_prompt: str, user_prompt: str) -> int:
    """估算 system 与 user prompt 的总 token。"""
    return estimate_text_tokens(system_prompt) + estimate_text_tokens(user_prompt)


def direct_final_max_tokens_for_client(
    client: Any | None,
    system_prompt: str,
    user_prompt: str,
    requested_max_tokens: int,
) -> int:
    """根据客户端上下文窗口动态收缩 direct-final 输出预算。"""
    if client is None or client.provider != "deepseek":
        return requested_max_tokens
    prompt_tokens = estimate_prompt_tokens(system_prompt, user_prompt)
    available = DEEPSEEK_CONTEXT_WINDOW_TOKENS - prompt_tokens - 1024
    if available < DEFAULT_DIRECT_FINAL_MIN_TOKENS:
        return requested_max_tokens
    return max(DEFAULT_DIRECT_FINAL_MIN_TOKENS, min(requested_max_tokens, available))


def parse_context_length_error(value: str) -> tuple[int, int, int, int] | None:
    """从模型上下文长度错误文本中提取限制和请求 token。"""
    match = re.search(
        r"maximum context length is (\d+) tokens\. However, you requested (\d+) tokens \((\d+) in the messages, (\d+) in the completion\)",
        value,
    )
    if not match:
        return None
    return (
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
        int(match.group(4)),
    )


def log_llm_request_estimate(
    stage: str,
    client: Any | None,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
) -> None:
    """打印一次 LLM 请求的 token 和成本估算日志。"""
    input_tokens = estimate_prompt_tokens(system_prompt, user_prompt)
    provider = client.provider if client else "none"
    model = client.model if client else ""
    message = (
        f"[LLMEstimate] {stage} provider={provider}"
        f"{('/' + model) if model else ''} "
        f"input~{input_tokens} tokens, output<= {max_tokens} tokens"
    )
    if provider == "deepseek":
        miss_cost = estimate_deepseek_cost_usd(input_tokens, max_tokens, cache_hit=False)
        hit_cost = estimate_deepseek_cost_usd(input_tokens, max_tokens, cache_hit=True)
        message += (
            f", DeepSeek cost~{format_usd(miss_cost)}"
            f" (cache hit~{format_usd(hit_cost)})"
        )
    else:
        message += ", cost estimate unavailable for this provider"
    print(message, flush=True)


def extract_word_cloud_terms(messages: list[StructuredMessage], top_n: int = 40) -> list[dict[str, Any]]:
    """从有效消息中提取高频词云条目。"""
    counter: Counter[str] = Counter()
    for message in messages:
        if not is_analysis_message(message):
            continue
        text = normalize_text(message.text)
        text = strip_wechat_emoji_shortcodes(text)
        text = re.sub(r"↳\s*回复\s*[^:：]+[:：]", " ", text)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"&[a-zA-Z]+;", " ", text)
        text = re.sub(r"\[(图片|表情|语音|链接/文件|系统|视频)[^\]]*\]", " ", text)
        if jieba is not None:
            tokens = [token.strip().lower() for token in jieba.lcut(text)]
        else:
            tokens = [token.lower() for token in extract_topic_tokens(text)]
        for token in tokens:
            token = token.strip()
            if len(token) < 2:
                continue
            if token in WORD_CLOUD_STOPWORDS:
                continue
            if re.fullmatch(r"[\W_]+", token):
                continue
            if token.startswith("http"):
                continue
            if token in {"有氧运动聊天", "回复有氧运动聊天"}:
                continue
            if re.fullmatch(r"\d+", token):
                continue
            if re.fullmatch(r"[a-z0-9_]{2,}", token):
                continue
            if re.fullmatch(r"([\u4e00-\u9fff])\1{1,}", token):
                continue
            counter[token] += 1
    return [
        {"word": word, "count": count}
        for word, count in counter.most_common(top_n)
    ]


def build_time_segment_breakdown(messages: list[StructuredMessage]) -> list[dict[str, Any]]:
    """按早中晚等本地时间段统计有效消息分布。"""
    segments = [
        ("凌晨", 0, 5),
        ("早晨", 6, 9),
        ("上午", 10, 11),
        ("下午", 12, 17),
        ("傍晚", 18, 20),
        ("夜间", 21, 23),
    ]
    counts = {label: 0 for label, _, _ in segments}
    for message in messages:
        if not is_analysis_message(message):
            continue
        hour = datetime.fromtimestamp(message.timestamp).hour
        for label, start_hour, end_hour in segments:
            if start_hour <= hour <= end_hour:
                counts[label] += 1
                break
    return [{"label": label, "count": counts[label]} for label, _, _ in segments]


def build_chunk(index: int, messages: list[StructuredMessage]) -> MessageChunk:
    """把一组消息封装成带范围和计数的 MessageChunk。"""
    if not messages:
        raise ValueError("messages must not be empty")
    start_ts = messages[0].timestamp
    end_ts = messages[-1].timestamp
    char_count = sum(len(message.sender) + len(message.text) + 16 for message in messages)
    return MessageChunk(
        id=f"shard-{index:03d}",
        index=index,
        start_ts=start_ts,
        end_ts=end_ts,
        start_time=format_ts(start_ts),
        end_time=format_ts(end_ts),
        message_count=len(messages),
        char_count=char_count,
        messages=messages[:],
    )


def topic_similarity(a: set[str], b: set[str]) -> float:
    """计算两个主题 token 集合的 Jaccard 相似度。"""
    if not a or not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0


def is_substantive_message(message: StructuredMessage) -> bool:
    """判断消息是否属于有内容价值的对话消息。"""
    if message.sender == "unknown":
        return False
    if message.msg_type in {"系统"}:
        return False
    return True


def classify_message_category(message: StructuredMessage) -> str:
    """把结构化消息归入文本、回复、系统、红包等分析类别。"""
    text = message.text or ""
    metadata = message.metadata or {}
    rich_kind = metadata.get("rich_kind", "")
    if message.msg_type == "系统":
        return "system"
    if message.msg_type == "语音":
        return "voice"
    if message.msg_type == "视频":
        return "video"
    if message.msg_type == "表情":
        return "emoji"
    if message.msg_type == "图片":
        return "image"
    if message.msg_type == "文本":
        return "text"
    if message.msg_type == "链接/文件":
        if "拍了拍" in text:
            return "pat"
        if "微信红包" in text:
            return "redpacket"
        if rich_kind == "merged_chat":
            return "merged_chat"
        if rich_kind == "link_card":
            return "link_card"
        if "聊天记录" in text:
            return "merged_chat"
        if text.strip() in {"[链接/文件]", "[链接]"}:
            return "bare_link_file"
        if "↳ 回复 " in text:
            return "reply"
        if text.startswith("[链接/文件]") or text.startswith("[链接]"):
            return "link_card"
    return "other"


def get_message_category_labels() -> dict[str, str]:
    """返回内部消息类别到中文标签的映射。"""
    return {
        "text": "文本",
        "reply": "回复",
        "emoji": "表情",
        "image": "图片",
        "voice": "语音",
        "video": "视频",
        "pat": "拍一拍",
        "system": "系统消息",
        "redpacket": "红包",
        "merged_chat": "合并聊天记录",
        "bare_link_file": "占位链接/文件",
        "link_card": "链接卡片",
        "other": "其他",
    }


def is_effective_conversation_message(message: StructuredMessage) -> bool:
    """判断消息是否进入本地统计和模型分析口径。"""
    if message.sender == "unknown":
        return False
    category = classify_message_category(message)
    if category in {"link_card", "merged_chat"}:
        return has_meaningful_rich_content(message)
    return category in {"text", "reply", "emoji", "image", "voice", "video"}


def is_analysis_message(message: StructuredMessage) -> bool:
    """返回消息是否进入 LLM 分析输入。"""
    return is_effective_conversation_message(message)


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


def build_chunks(
    messages: list[StructuredMessage],
    max_messages: int,
    max_chars: int,
    max_minutes: int,
    hard_gap_minutes: int = 90,
    soft_gap_minutes: int = 18,
    low_similarity_threshold: float = 0.08,
    min_chunk_messages: int = 24,
) -> list[MessageChunk]:
    """按消息数、字符数、时间跨度和话题变化切分分析片段。"""
    if not messages:
        return []

    chunks: list[MessageChunk] = []
    current: list[StructuredMessage] = []
    current_chars = 0
    chunk_index = 1
    chunk_start_ts = messages[0].timestamp
    chunk_topic_tokens: set[str] = set()
    low_similarity_streak = 0
    prev_message: StructuredMessage | None = None

    for message in messages:
        next_chars = current_chars + len(message.sender) + len(message.text) + 16
        elapsed_minutes = ((message.timestamp - chunk_start_ts) / 60 if current else 0)
        gap_minutes = ((message.timestamp - prev_message.timestamp) / 60 if prev_message else 0)
        message_tokens = extract_topic_tokens(message.text)
        similarity = topic_similarity(message_tokens, chunk_topic_tokens)

        topic_break = (
            bool(current)
            and len(current) >= min_chunk_messages
            and gap_minutes >= soft_gap_minutes
            and similarity < low_similarity_threshold
            and low_similarity_streak >= 1
        )
        hard_gap_break = bool(current) and gap_minutes >= hard_gap_minutes
        # 分片既要控制 prompt 体积，也要尽量在长时间停顿或话题明显变化处切开。
        should_flush = bool(
            current
            and (
                len(current) >= max_messages
                or next_chars > max_chars
                or elapsed_minutes > max_minutes
                or hard_gap_break
                or topic_break
            )
        )
        if should_flush:
            chunks.append(build_chunk(chunk_index, current))
            chunk_index += 1
            current = []
            current_chars = 0
            chunk_start_ts = message.timestamp
            chunk_topic_tokens = set()
            low_similarity_streak = 0

        if not current:
            chunk_start_ts = message.timestamp
        current.append(message)
        current_chars += len(message.sender) + len(message.text) + 16
        chunk_topic_tokens.update(message_tokens)
        if similarity < low_similarity_threshold and gap_minutes >= soft_gap_minutes:
            low_similarity_streak += 1
        else:
            low_similarity_streak = 0
        prev_message = message

    if current:
        chunks.append(build_chunk(chunk_index, current))

    return chunks


def build_analysis_chunks(
    messages: list[StructuredMessage],
    max_messages: int,
    max_chars: int,
    max_minutes: int,
    hard_gap_minutes: int,
    soft_gap_minutes: int,
    low_similarity_threshold: float,
    min_chunk_messages: int,
    direct_token_threshold: int,
    direct_max_bytes: int,
) -> tuple[list[MessageChunk], dict[str, Any]]:
    """根据 token/字节预算选择 direct_range 或分片分析计划。"""
    analysis_messages = [message for message in messages if is_analysis_message(message)]
    if not analysis_messages:
        return [], {
            "strategy": "range-first",
            "direct_token_threshold": direct_token_threshold,
            "direct_max_bytes": direct_max_bytes,
            "analysis_message_count": 0,
            "estimated_tokens": 0,
            "estimated_direct_payload_bytes": 0,
            "mode": "empty",
            "range_direct": False,
            "shard_count": 0,
        }

    estimated_tokens = sum(estimate_message_tokens(message) for message in analysis_messages)
    direct_chunk = build_chunk(1, analysis_messages)
    direct_payload_bytes = estimate_chunk_payload_bytes(direct_chunk)
    byte_limit_allows_direct = direct_max_bytes <= 0 or direct_payload_bytes <= direct_max_bytes
    # 小时间窗优先 direct_range，避免 map/reduce 把同一话题过度切碎。
    if estimated_tokens <= direct_token_threshold and byte_limit_allows_direct:
        chunk = direct_chunk
        return [chunk], {
            "strategy": "range-first",
            "direct_token_threshold": direct_token_threshold,
            "direct_max_bytes": direct_max_bytes,
            "analysis_message_count": len(analysis_messages),
            "estimated_tokens": estimated_tokens,
            "estimated_direct_payload_bytes": direct_payload_bytes,
            "mode": "direct_range",
            "range_direct": True,
            "shard_count": 1,
            "range": {
                "start": analysis_messages[0].time,
                "end": analysis_messages[-1].time,
            },
        }

    return build_sharded_range_chunks(
        messages,
        max_messages=max_messages,
        max_chars=max_chars,
        max_minutes=max_minutes,
        hard_gap_minutes=hard_gap_minutes,
        soft_gap_minutes=soft_gap_minutes,
        low_similarity_threshold=low_similarity_threshold,
        min_chunk_messages=min_chunk_messages,
        direct_token_threshold=direct_token_threshold,
        direct_max_bytes=direct_max_bytes,
        estimated_direct_payload_bytes=direct_payload_bytes,
    )


def build_sharded_range_chunks(
    messages: list[StructuredMessage],
    max_messages: int,
    max_chars: int,
    max_minutes: int,
    hard_gap_minutes: int,
    soft_gap_minutes: int,
    low_similarity_threshold: float,
    min_chunk_messages: int,
    direct_token_threshold: int,
    direct_max_bytes: int,
    fallback_reason: str = "",
    estimated_direct_payload_bytes: int = 0,
) -> tuple[list[MessageChunk], dict[str, Any]]:
    """强制按分片模式构造消息块和分析计划。"""
    analysis_messages = [message for message in messages if is_analysis_message(message)]
    if not analysis_messages:
        return [], {
            "strategy": "range-first",
            "direct_token_threshold": direct_token_threshold,
            "direct_max_bytes": direct_max_bytes,
            "analysis_message_count": 0,
            "estimated_tokens": 0,
            "estimated_direct_payload_bytes": 0,
            "mode": "empty",
            "range_direct": False,
            "shard_count": 0,
        }

    estimated_tokens = sum(estimate_message_tokens(message) for message in analysis_messages)
    chunks = build_chunks(
        analysis_messages,
        max_messages=max_messages,
        max_chars=max_chars,
        max_minutes=max_minutes,
        hard_gap_minutes=hard_gap_minutes,
        soft_gap_minutes=soft_gap_minutes,
        low_similarity_threshold=low_similarity_threshold,
        min_chunk_messages=min_chunk_messages,
    )
    for index, chunk in enumerate(chunks, start=1):
        chunk.id = f"shard-{index:03d}"
        chunk.index = index

    plan = {
        "strategy": "range-first",
        "direct_token_threshold": direct_token_threshold,
        "direct_max_bytes": direct_max_bytes,
        "analysis_message_count": len(analysis_messages),
        "estimated_tokens": estimated_tokens,
        "estimated_direct_payload_bytes": estimated_direct_payload_bytes,
        "mode": "sharded_range",
        "range_direct": False,
        "shard_count": len(chunks),
        "range": {
            "start": analysis_messages[0].time,
            "end": analysis_messages[-1].time,
        },
    }
    if fallback_reason:
        # direct_range 失败后的回退会记录原因，方便排查后续为何出现多次 LLM 调用。
        plan["fallback_reason"] = fallback_reason
        plan["fallback_from_direct_range"] = True
    return chunks, plan


def estimate_reduce_call_count(item_count: int, fan_in: int) -> int:
    """估算按 fan-in 逐轮 reduce 需要的模型调用次数。"""
    fan_in = max(2, fan_in)
    total = 0
    current = item_count
    while current > fan_in:
        groups = math.ceil(current / fan_in)
        total += groups
        current = groups
    return total


def build_topic_retry_chunks(chunk: MessageChunk) -> list[MessageChunk]:
    """在单个 map 片段失败时生成更小的主题重试分片。"""
    return build_chunks(
        chunk.messages,
        max_messages=min(220, max(80, chunk.message_count // 2)),
        max_chars=min(12000, max(5000, chunk.char_count // 2)),
        max_minutes=120,
        hard_gap_minutes=45,
        soft_gap_minutes=10,
        low_similarity_threshold=0.14,
        min_chunk_messages=10,
    )


def normalize_rank_name(name: str) -> str:
    """清洗排行榜中的成员名，过滤 unknown 等无效值。"""
    name = collapse_text(name or "", max_len=80)
    return "" if name in {"", "unknown"} else name


def ranked_counter(counter: Counter[str], limit: int = 10) -> list[dict[str, Any]]:
    """把计数器转换为带排名的字典列表。"""
    items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return [
        {"rank": index, "name": name, "count": count}
        for index, (name, count) in enumerate(items, start=1)
    ]


def build_interaction_rankings(messages: list[StructuredMessage]) -> dict[str, list[dict[str, Any]]]:
    """统计拍一拍、定向红包和回复等互动榜单。"""
    pat_sender_counts: Counter[str] = Counter()
    pat_target_counts: Counter[str] = Counter()
    redpacket_receiver_counts: Counter[str] = Counter()
    reply_sender_counts: Counter[str] = Counter()

    for message in messages:
        category = classify_message_category(message)
        metadata = message.metadata or {}

        if category == "pat":
            pat_sender = normalize_rank_name(metadata.get("pat_from_name", "") or metadata.get("pat_from_username", ""))
            pat_target = normalize_rank_name(metadata.get("pat_to_name", "") or metadata.get("pat_to_username", ""))
            if not pat_sender:
                pat_sender, fallback_target = parse_pat_title_names(message.text)
                pat_sender = normalize_rank_name(message.sender if pat_sender == "我" else pat_sender)
                pat_target = pat_target or normalize_rank_name(fallback_target)
            if not pat_sender:
                pat_sender = normalize_rank_name(message.sender)
            if pat_sender:
                pat_sender_counts[pat_sender] += 1
            if pat_target:
                pat_target_counts[pat_target] += 1

        if metadata.get("interaction_kind") == "direct_redpacket":
            receiver = normalize_rank_name(
                metadata.get("redpacket_receiver_name", "") or metadata.get("redpacket_receiver_username", "")
            )
            if receiver:
                redpacket_receiver_counts[receiver] += 1

        if category == "reply":
            reply_to_username = metadata.get("reply_to_username", "")
            reply_to_name = normalize_rank_name(metadata.get("reply_to_name", ""))
            is_self_reply = bool(reply_to_username and message.sender_username and reply_to_username == message.sender_username)
            if not is_self_reply and reply_to_name and reply_to_name == message.sender:
                is_self_reply = True
            sender = normalize_rank_name(message.sender)
            if sender and not is_self_reply:
                reply_sender_counts[sender] += 1

    return {
        "pat_sender": ranked_counter(pat_sender_counts),
        "pat_target": ranked_counter(pat_target_counts),
        "direct_redpacket_receiver": ranked_counter(redpacket_receiver_counts),
        "reply_sender": ranked_counter(reply_sender_counts),
    }


def build_local_stats(messages: list[StructuredMessage]) -> dict[str, Any]:
    """生成报表所需的本地精确统计数据。"""
    category_labels = get_message_category_labels()
    category_counts = Counter(classify_message_category(message) for message in messages)
    substantive_messages = [message for message in messages if is_substantive_message(message)]
    effective_messages = [message for message in messages if is_effective_conversation_message(message)]
    member_aliases = collect_member_aliases_from_messages(messages)

    def message_sender_display(message: StructuredMessage) -> str:
        """为统计输出选择成员当前最合适的显示名。"""
        sender_id = message.sender_username or ""
        alias = member_aliases.get(sender_id, "")
        if alias and not is_resolved_member_display(sender_id, message.sender):
            return alias
        return message.sender

    sender_counts = Counter(message_sender_display(message) for message in effective_messages)
    type_counts = Counter(message.msg_type for message in messages)
    hour_counts = Counter(
        datetime.fromtimestamp(message.timestamp).strftime("%H:00")
        for message in effective_messages
        if is_analysis_message(message)
    )

    top_speakers = [
        {"rank": index, "name": name, "message_count": count}
        for index, (name, count) in enumerate(sender_counts.most_common(10), start=1)
    ]
    speaker_directory = []
    seen_speaker_ids: set[str] = set()
    for message in effective_messages:
        sender_id = message.sender_username or ""
        if not sender_id or sender_id in seen_speaker_ids:
            continue
        seen_speaker_ids.add(sender_id)
        speaker_directory.append(
            {
                "sender_id": sender_id,
                "sender_name": message_sender_display(message),
                "mention_token": make_user_placeholder(sender_id),
            }
        )
    excluded_categories = ["pat", "system", "bare_link_file", "redpacket", "other"]
    excluded_count = sum(category_counts.get(category, 0) for category in excluded_categories)
    raw_char_count = sum(len(message.text or "") for message in messages)
    effective_char_count = sum(len(message.text or "") for message in effective_messages)
    return {
        "message_count": len(messages),
        "analysis_message_count": len(effective_messages),
        "substantive_message_count": len(substantive_messages),
        "effective_message_count": len(effective_messages),
        "excluded_message_count": excluded_count,
        "raw_char_count": raw_char_count,
        "effective_char_count": effective_char_count,
        "participant_count": len(sender_counts),
        "known_speakers": [name for name, _ in sender_counts.most_common()],
        "member_aliases": [
            {
                "sender_id": sender_id,
                "sender_name": sender_name,
                "mention_token": make_user_placeholder(sender_id),
            }
            for sender_id, sender_name in sorted(member_aliases.items())
        ],
        "speaker_directory": speaker_directory,
        "type_breakdown": [
            {"type": message_type, "count": count}
            for message_type, count in type_counts.most_common()
        ],
        "category_breakdown": [
            {"type": category_labels[category], "count": count}
            for category, count in category_counts.most_common()
        ],
        "effective_breakdown": [
            {"type": category_labels[category], "count": category_counts[category]}
            for category in ["text", "reply", "emoji", "image", "voice", "video", "link_card", "merged_chat"]
            if category_counts.get(category, 0)
        ],
        "excluded_breakdown": [
            {"type": category_labels[category], "count": category_counts[category]}
            for category in excluded_categories
            if category_counts.get(category, 0)
        ],
        "top_speakers": top_speakers,
        "interaction_rankings": build_interaction_rankings(messages),
        "busiest_hours": [
            {"hour": hour, "count": count}
            for hour, count in hour_counts.most_common(6)
        ],
        "time_segment_breakdown": build_time_segment_breakdown(messages),
        "word_cloud": extract_word_cloud_terms(messages),
        "unknown_message_count": sum(1 for message in messages if message.sender == "unknown"),
        "system_message_count": sum(1 for message in messages if message.msg_type == "系统"),
        "first_message_time": messages[0].time if messages else "",
        "last_message_time": messages[-1].time if messages else "",
    }


def serialize_messages(messages: list[StructuredMessage]) -> list[dict[str, Any]]:
    """把结构化消息转换为可写入 JSON 的字典列表。"""
    return [asdict(message) for message in messages]


def get_chunk_topic_keywords(messages: list[StructuredMessage], top_n: int = 12) -> list[str]:
    """提取分片中的高频主题关键词。"""
    counter: Counter[str] = Counter()
    for message in messages:
        if not is_analysis_message(message):
            continue
        counter.update(extract_topic_tokens(message.text))
    return [token for token, _ in counter.most_common(top_n)]


def chunk_payload(chunk: MessageChunk) -> dict[str, Any]:
    """生成 map 阶段使用的完整分片 JSON 输入。"""
    analysis_messages = [message for message in chunk.messages if is_analysis_message(message)]
    member_directory = []
    seen_members: set[str] = set()
    for message in analysis_messages:
        sender_id = message.sender_username or ""
        if not sender_id or sender_id in seen_members:
            continue
        seen_members.add(sender_id)
        member_directory.append(
            {
                "sender_id": sender_id,
                "sender_name": message.sender,
                "mention_token": make_user_placeholder(sender_id),
            }
        )
    return {
        "shard_id": chunk.id,
        "time_range": {
            "start": chunk.start_time,
            "end": chunk.end_time,
        },
        "message_count": chunk.message_count,
        "analysis_message_count": len(analysis_messages),
        "topic_keywords": get_chunk_topic_keywords(chunk.messages),
        "member_directory": member_directory,
        "messages": [
            {
                "id": message.id,
                "time": message.time,
                "sender_id": message.sender_username or "",
                "sender": message.sender,
                "type": message.msg_type,
                "text": message.text,
                "metadata": (
                    {
                        "rich_kind": message.metadata.get("rich_kind", ""),
                        "title": message.metadata.get("title", ""),
                        "summary": message.metadata.get("summary", ""),
                        "source": message.metadata.get("source", ""),
                        "items": message.metadata.get("items", [])[:4],
                    }
                    if message.metadata
                    else {}
                ),
            }
            for message in analysis_messages
        ],
    }


def estimate_chunk_payload_bytes(chunk: MessageChunk) -> int:
    """估算分片 JSON 载荷的 UTF-8 字节数。"""
    return len(json.dumps(chunk_payload(chunk), ensure_ascii=False).encode("utf-8"))


def compact_direct_chunk_payload(chunk: MessageChunk) -> dict[str, Any]:
    """生成 direct-final 模式使用的紧凑消息文本载荷。"""
    analysis_messages = [message for message in chunk.messages if is_analysis_message(message)]
    message_lines = []
    for message in analysis_messages:
        sender_ref = make_user_placeholder(message.sender_username) or message.sender
        message_lines.append(f"{message.time}|{sender_ref}|{message.msg_type}|{message.text}")

    return {
        "shard_id": chunk.id,
        "time_range": f"{chunk.start_time} -> {chunk.end_time}",
        "message_count": chunk.message_count,
        "analysis_message_count": len(analysis_messages),
        "messages_text": "\n".join(message_lines),
    }


def compact_prompt_stats(stats: dict[str, Any]) -> dict[str, Any]:
    """压缩统计字段，减少 prompt 中的非必要体积。"""
    return {
        "message_count": stats.get("message_count", 0),
        "effective_message_count": stats.get("effective_message_count", 0),
        "excluded_message_count": stats.get("excluded_message_count", 0),
        "participant_count": stats.get("participant_count", 0),
        "raw_char_count": stats.get("raw_char_count", 0),
        "effective_char_count": stats.get("effective_char_count", 0),
        "first_message_time": stats.get("first_message_time", ""),
        "last_message_time": stats.get("last_message_time", ""),
        "top_speakers": stats.get("top_speakers", [])[:10],
        "interaction_rankings": stats.get("interaction_rankings", {}),
        "effective_breakdown": stats.get("effective_breakdown", []),
        "time_segment_breakdown": stats.get("time_segment_breakdown", []),
        "word_cloud": stats.get("word_cloud", [])[:20],
    }


def indexed_analysis_messages(chunk: MessageChunk) -> list[tuple[int, StructuredMessage]]:
    """为有效分析消息生成从 1 开始的索引。"""
    return [
        (index, message)
        for index, message in enumerate(
            [item for item in chunk.messages if is_analysis_message(item)],
            start=1,
        )
    ]


def compact_topic_index_payload(chunk: MessageChunk) -> dict[str, Any]:
    """生成 topic-first 主题规划阶段的索引化消息载荷。"""
    message_lines = []
    for index, message in indexed_analysis_messages(chunk):
        sender_ref = make_user_placeholder(message.sender_username) or message.sender
        message_lines.append(f"{index}|{message.time}|{sender_ref}|{message.msg_type}|{message.text}")
    return {
        "time_range": f"{chunk.start_time} -> {chunk.end_time}",
        "analysis_message_count": len(message_lines),
        "messages_text": "\n".join(message_lines),
    }


def compact_topic_section_payload(topic: dict[str, Any], messages: list[StructuredMessage]) -> dict[str, Any]:
    """生成 topic-first 单主题 section 分析载荷。"""
    message_lines = []
    for message in messages:
        sender_ref = make_user_placeholder(message.sender_username) or message.sender
        message_lines.append(f"{message.id}|{message.time}|{sender_ref}|{message.msg_type}|{message.text}")
    return {
        "topic": {
            "topic_id": topic.get("topic_id", ""),
            "title": topic.get("title", ""),
            "summary": topic.get("summary", ""),
            "start_time": topic.get("start_time", ""),
            "end_time": topic.get("end_time", ""),
            "priority": topic.get("priority", ""),
        },
        "message_count": len(messages),
        "messages_text": "\n".join(message_lines),
    }
