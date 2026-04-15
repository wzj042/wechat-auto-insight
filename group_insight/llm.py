"""LLM 客户端封装与群聊日报 prompt 构造。

这里统一 DeepSeek、智谱等模型的 JSON 调用接口，并集中维护 map、reduce、final、
direct-final 和 topic-first 模式使用的结构化提示词。
"""
from __future__ import annotations

from .conversation import *
from .settings import _ZHIPU_LAST_CALL_AT, _ZHIPU_RATE_LOCK


class LLMClientProtocol:
    """所有 LLM 客户端需要实现的最小 JSON 对话协议。"""
    provider: str
    model: str

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        """调用模型并返回解析后的 JSON 对象。"""
        raise NotImplementedError


def is_rate_limit_error(exc: Exception) -> bool:
    """判断异常文本是否表示模型接口限频。"""
    text = str(exc or "").lower()
    return any(token in text for token in ["429", "1302", "rate limit", "速率限制"])


class DeepSeekClient(LLMClientProtocol):
    """DeepSeek Chat Completions JSON 调用客户端。"""
    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_DEEPSEEK_MODEL,
        api_url: str = DEFAULT_API_URL,
        timeout: int = 180,
        max_retries: int = 3,
    ) -> None:
        """初始化客户端配置和限频/重试参数。"""
        self.provider = "deepseek"
        self.api_key = api_key
        self.model = model
        self.api_url = api_url
        self.timeout = timeout
        self.max_retries = max_retries

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        """调用模型并返回解析后的 JSON 对象。"""
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            content = ""
            try:
                content = self._request_content(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                )
                if not content.strip():
                    raise ValueError("DeepSeek 返回空内容")
                return safe_json_loads(content)
            except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, ValueError, RuntimeError) as exc:
                last_error = exc
                if isinstance(exc, json.JSONDecodeError):
                    try:
                        repaired = self._repair_json(
                            broken_json=content,
                            max_tokens=max_tokens,
                        )
                        if repaired:
                            return safe_json_loads(repaired)
                    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, ValueError, RuntimeError) as repair_exc:
                        last_error = repair_exc
                if attempt >= self.max_retries:
                    break
                time.sleep(attempt * 2)
        raise RuntimeError(f"DeepSeek 调用失败: {last_error}") from last_error

    def _request_content(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> str:
        """发送一次 HTTP 请求并返回模型原始文本内容。"""
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.api_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            if detail:
                raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {detail[:1000]}") from exc
            raise
        parsed = json.loads(raw)
        usage = parsed.get("usage", {})
        if usage:
            cost = estimate_deepseek_usage_cost_usd(usage)
            print(
                "[LLMUsage] deepseek "
                f"prompt={usage.get('prompt_tokens', 0)} "
                f"cache_hit={usage.get('prompt_cache_hit_tokens', 0)} "
                f"cache_miss={usage.get('prompt_cache_miss_tokens', 0)} "
                f"completion={usage.get('completion_tokens', 0)} "
                f"cost~{format_usd(cost)}",
                flush=True,
            )
        return (
            parsed.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )

    def _repair_json(self, broken_json: str, max_tokens: int) -> str:
        """在 DeepSeek 返回 JSON 截断或损坏时请求模型修复。"""
        candidate = extract_json_object(broken_json) or broken_json
        candidate = candidate.strip()
        if not candidate:
            raise ValueError("empty broken json")

        repair_system_prompt = """
你是一个 JSON 修复器。你会收到一段损坏或截断的 JSON。

要求：
1. 只输出一个合法 JSON 对象。
2. 不要添加 markdown、解释、注释。
3. 尽量保留原字段和原值。
4. 若局部截断无法恢复，删除损坏字段或把该字段改为空数组/空字符串，但必须保持整体 JSON 合法。
5. 所有括号、引号、逗号都必须正确闭合。
""".strip()
        repair_user_prompt = f"""
请把下面这段损坏 JSON 修复成合法 JSON 对象，只输出修复后的 JSON：

{candidate}
""".strip()
        return self._request_content(
            system_prompt=repair_system_prompt,
            user_prompt=repair_user_prompt,
            max_tokens=min(max_tokens, 4096),
            temperature=0.0,
        )


class ZhipuClient(LLMClientProtocol):
    """智谱模型 JSON 调用客户端，带串行限频和指数退避。"""
    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_ZHIPU_MODEL,
        max_retries: int = 8,
        min_interval_seconds: float = 1.2,
        rate_limit_base_delay: float = 4.0,
        rate_limit_max_delay: float = 90.0,
    ) -> None:
        """初始化客户端配置和限频/重试参数。"""
        try:
            from zhipuai_tool import create_zhipu_client
        except Exception as exc:
            raise RuntimeError(f"无法导入 zhipuai_tool: {exc}") from exc

        self.provider = "zhipu"
        self.model = model
        self.max_retries = max_retries
        self.min_interval_seconds = min_interval_seconds
        self.rate_limit_base_delay = rate_limit_base_delay
        self.rate_limit_max_delay = rate_limit_max_delay
        self.client = create_zhipu_client(api_key, max_retries=0)
        setattr(self.client, "MODEL_TEXT", model)

    def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 4096,
        temperature: float = 0.2,
    ) -> dict[str, Any]:
        """调用模型并返回解析后的 JSON 对象。"""
        global _ZHIPU_LAST_CALL_AT
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with _ZHIPU_RATE_LOCK:
                    now = time.time()
                    wait_seconds = self.min_interval_seconds - (now - _ZHIPU_LAST_CALL_AT)
                    if wait_seconds > 0:
                        time.sleep(wait_seconds)
                    _ZHIPU_LAST_CALL_AT = time.time()
                result = self.client.text_chat(
                    prompt=user_prompt,
                    messages=[{"role": "system", "content": system_prompt}],
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                return safe_json_loads(result.get("content", ""))
            except (json.JSONDecodeError, ValueError, RuntimeError, TypeError) as exc:
                last_error = exc
                if is_rate_limit_error(exc) and attempt < self.max_retries:
                    delay = min(self.rate_limit_max_delay, self.rate_limit_base_delay * (2 ** (attempt - 1)))
                    print(f"[ZhipuBackoff] rate limited, waiting {delay:.1f}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(delay)
                    continue
            except Exception as exc:
                last_error = exc
                if is_rate_limit_error(exc) and attempt < self.max_retries:
                    delay = min(self.rate_limit_max_delay, self.rate_limit_base_delay * (2 ** (attempt - 1)))
                    print(f"[ZhipuBackoff] rate limited, waiting {delay:.1f}s before retry {attempt + 1}/{self.max_retries}")
                    time.sleep(delay)
                    continue
        raise RuntimeError(f"Zhipu 调用失败: {last_error}")


MAP_SCHEMA_EXAMPLE = {
    "shard_id": "shard-001",
    "time_range": {"start": "2026-04-08 09:00", "end": "2026-04-08 10:30"},
    "summary": "该时间片主要围绕一个或多个连续话题展开。",
    "theme_cards": [
        {
            "title": "主题标题",
            "summary": "该主题在这个时间片里的简明总结。",
            "evidence_ids": ["m_xxx", "m_yyy"],
        }
    ],
    "highlight_sections": [
        {
            "title": "一个可单独成段的话题簇标题",
            "start_time": "2026-04-08 09:12",
            "end_time": "2026-04-08 09:35",
            "summary": "这个话题簇发生了什么。",
            "bullets": ["关键点1", "关键点2"],
            "evidence_ids": ["m_xxx", "m_yyy"],
        }
    ],
    "participant_notes": [
        {
            "name": "[[user:wxid_xxx]]",
            "observation": "[[user:wxid_xxx]] 在本片段中的作用或表现。",
            "evidence_ids": ["m_xxx"],
        }
    ],
    "quotes": [
        {
            "speaker": "[[user:wxid_xxx]]",
            "time": "2026-04-08 09:23",
            "quote": "一句值得引用的话",
            "message_id": "m_xxx",
            "why_it_matters": "为什么值得引用",
        }
    ],
    "decisions": [{"content": "已达成的结论", "evidence_ids": ["m_xxx"]}],
    "action_items": [
        {
            "owner": "[[user:wxid_xxx]] 或留空",
            "task": "[[user:wxid_xxx]] 相关待办事项",
            "deadline": "时间或留空",
            "status_hint": "紧急/一般/观察",
            "evidence_ids": ["m_xxx"],
        }
    ],
    "open_questions": [{"question": "未解决的问题", "evidence_ids": ["m_xxx"]}],
    "mood": {
        "label": "活跃/理性/轻松/焦虑/冲突等",
        "reason": "判断依据",
        "evidence_ids": ["m_xxx"],
    },
}


REDUCE_SCHEMA_EXAMPLE = {
    "bundle_id": "reduce-001",
    "summary": "多片段合并后的摘要",
    "theme_cards": [
        {"title": "核心主题", "summary": "主题归纳", "source_refs": ["shard-001"]}
    ],
    "highlight_sections": [
        {
            "title": "重要话题簇",
            "start_time": "2026-04-08 09:12",
            "end_time": "2026-04-08 10:30",
            "summary": "合并后的话题总结",
            "bullets": ["关键点1", "关键点2"],
            "source_refs": ["shard-001", "shard-002"],
        }
    ],
    "participant_notes": [
        {"name": "[[user:wxid_xxx]]", "observation": "[[user:wxid_xxx]] 的角色观察", "source_refs": ["shard-001"]}
    ],
    "quotes": [
        {
            "speaker": "[[user:wxid_xxx]]",
            "time": "2026-04-08 09:23",
            "quote": "一句代表性话语",
            "source_refs": ["shard-001"],
        }
    ],
    "decisions": [{"content": "结论", "source_refs": ["shard-001"]}],
    "action_items": [
        {
            "owner": "[[user:wxid_xxx]] 或留空",
            "task": "[[user:wxid_xxx]] 相关待办事项",
            "deadline": "时间或留空",
            "status_hint": "紧急/一般/观察",
            "source_refs": ["shard-001"],
        }
    ],
    "open_questions": [{"question": "未决问题", "source_refs": ["shard-001"]}],
    "risk_flags": ["潜在风险或争议点"],
    "mood": {"label": "整体氛围", "reason": "原因", "source_refs": ["shard-001"]},
}


FINAL_REPORT_SCHEMA_EXAMPLE = {
    "headline": "一句报告总标题",
    "tagline": "一句短副标题",
    "lead_summary": "1-2 段的默认总结",
    "theme_cards": [
        {"title": "主题一", "summary": "适合展示在摘要卡片中的简短文本"}
    ],
    "sections": [
        {
            "title": "话题簇标题",
            "start_time": "2026-04-08 09:12",
            "end_time": "2026-04-08 10:30",
            "summary": "这个话题簇的核心结论",
            "bullets": ["要点1", "要点2"],
            "takeaway": "一句点评或收束",
        }
    ],
    "participant_insights": [
        {"name": "[[user:wxid_xxx]]", "insight": "[[user:wxid_xxx]] 的关键作用或状态"}
    ],
    "quotes": [
        {
            "speaker": "[[user:wxid_xxx]]",
            "time": "2026-04-08 09:23",
            "quote": "一句可放进报告的原话",
            "why_it_matters": "为什么重要",
        }
    ],
    "decisions": ["已明确的结论"],
    "action_items": [
        {"owner": "[[user:wxid_xxx]] 或留空", "task": "[[user:wxid_xxx]] 相关行动项", "deadline": "时间或留空"}
    ],
    "open_questions": ["未解决的问题"],
    "risk_flags": ["需要继续观察的风险或争议"],
    "mood": {"label": "整体氛围", "reason": "判断依据"},
}


def build_map_prompts(chat_name: str, chunk: MessageChunk) -> tuple[str, str]:
    """构造 map 阶段对单个消息片段的分析提示词。"""
    system_prompt = f"""
你是一个严谨的群聊分析师。请基于用户提供的群聊时间片消息做结构化分析，并只输出 json。

要求：
1. 只基于提供的消息内容，不要补充外部事实。
2. 所有数组字段都必须存在，没内容时返回空数组。
3. evidence_ids 必须引用输入消息中的 id。
4. 主题和亮点要偏“可直接上报表”的表达，不要写成学术论文。
5. 允许保留轻度口语化，但不能夸张、不能编造。
6. 严格控制输出长度，宁可少写，不要写长段落。
7. theme_cards 最多 3 条，highlight_sections 最多 4 条，participant_notes 最多 4 条。
8. quotes 最多 2 条，decisions/action_items/open_questions 各最多 3 条。
9. action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
10. 每个 highlight_sections.bullets 最多 2 条。
11. highlight_sections 表示“话题簇”而不是机械时间切段；如果同一时间窗口里存在多个不同话题，可以拆成多个 sections，时间范围允许重叠。
12. 不要只写最显眼的主线，持续时间较短但消息量可观、内容明确的次级话题也要覆盖，避免遗漏例如运动分享、生活分享、工具讨论这类支线。
13. 输入里会提供 member_directory；提到具体成员时，请统一使用对应的 `[[user:sender_id]]` 占位符，不要直接输出昵称。

输出 json schema 示例：
{json.dumps(MAP_SCHEMA_EXAMPLE, ensure_ascii=False, indent=2)}
""".strip()
    user_prompt = f"""
请分析群聊“{chat_name}”的一个时间片，并输出严格 json。

时间片数据：
{json.dumps(chunk_payload(chunk), ensure_ascii=False, indent=2)}
""".strip()
    return system_prompt, user_prompt


def build_reduce_prompts(bundle_id: str, items: list[dict[str, Any]]) -> tuple[str, str]:
    """构造 reduce 阶段合并多个片段结果的提示词。"""
    system_prompt = f"""
你是一个群聊分析 reducer。你会收到多个 shard 分析结果或中间 reduce 结果，请将它们合并成一个更高层摘要，并只输出 json。

要求：
1. 只整合输入中的已有信息，不要引入外部信息。
2. 去重同类主题、同类结论和重复行动项。
3. highlight_sections 应按“话题簇”整理，不要机械按时间线硬切；如果多个话题的主要活跃区间重叠，允许时间范围重叠。
4. source_refs 必须引用输入里的 shard_id 或 bundle_id。
5. risk_flags 至少覆盖明显争议、风险、未落地事项；没有则返回空数组。
6. 严格控制输出长度，宁可少写，不要写长段落。
7. theme_cards 最多 4 条，highlight_sections 最多 6 条，participant_notes 最多 6 条。
8. quotes 最多 3 条，decisions/action_items/open_questions 各最多 4 条。
9. 合并时检查是否遗漏持续但相对次级的话题，不要只保留最热主线。
10. 如果输入覆盖多个明显不同的活跃区间，highlight_sections 至少为每个输入 shard/bundle 保留一个非重复的话题 section，除非其内容与其他输入完全重复。
11. 不要让结果出现长时间空洞；如果 source_refs 对应的输入在某一时间段内明显活跃，合并结果应覆盖该时段。
12. 如果输入里出现 `[[user:sender_id]]` 占位符，输出时保留该占位符，不要改写成昵称。

输出 json schema 示例：
{json.dumps(REDUCE_SCHEMA_EXAMPLE, ensure_ascii=False, indent=2)}
""".strip()
    user_prompt = f"""
请把以下多个群聊分析结果合并为一个中间 bundle，并输出严格 json。

目标 bundle_id: {bundle_id}

输入：
{json.dumps(items, ensure_ascii=False, indent=2)}
""".strip()
    return system_prompt, user_prompt


def build_final_prompts(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    bundles: list[dict[str, Any]],
) -> tuple[str, str]:
    """构造 final 阶段生成最终日报结构的提示词。"""
    system_prompt = f"""
你是一个中文群聊洞察报表编辑。你会收到本地统计数据和一组最终 reduce bundles，请产出适合日报/周报页面渲染的最终结构化结果，并只输出 json。

要求：
1. 只基于输入，不得补充不存在的数字。
2. 主题卡片应短、清晰、适合视觉卡片展示。
3. sections 是报告主体，表示“话题簇”而不是机械时间段；数量控制在 6-12 段。
4. 报表语言要像运营洞察报告，不要写成泛泛总结。
5. action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
6. 严格控制输出长度，宁可少写，不要把每条细节都展开。
7. theme_cards 最多 4 条，participant_insights 最多 6 条，quotes 最多 4 条。
8. sections 每段 bullets 最多 2 条。
9. 如果多个话题的主要活跃时间交叠，允许不同 sections 的 start_time / end_time 重叠，不要为了避免重叠而把不同主题强行糅合成一段。
10. 请覆盖当日所有明显成型的话题，不要遗漏持续时间较短但消息量可观的讨论，例如下午的运动分享、生活分享、工具讨论等。
11. 如果输入 bundles 覆盖多个明显不同的活跃区间，sections 至少为每个 bundle/shard 保留一个非重复 section，除非两个输入本质上是同一话题。
12. 不要生成大段时间空洞；若输入在某个中段时窗存在明显活跃讨论，最终 sections 应覆盖该时段。
13. 如果输入里的 bundles 使用 `[[user:sender_id]]` 占位符，最终输出请保留这些占位符，不要改写成昵称。

最终 json schema 示例：
{json.dumps(FINAL_REPORT_SCHEMA_EXAMPLE, ensure_ascii=False, indent=2)}
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”生成最终结构化报表 json。

统计区间：{start_time} ~ {end_time}

本地精确统计：
{json.dumps(stats, ensure_ascii=False, indent=2)}

最终 reduce 输入：
{json.dumps(bundles, ensure_ascii=False, indent=2)}
""".strip()
    return system_prompt, user_prompt


def build_direct_final_prompts(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
) -> tuple[str, str]:
    """构造 direct_range 模式下直接生成最终日报的提示词。"""
    compact_stats = compact_prompt_stats(stats)
    system_prompt = """
你是一个中文群聊洞察报表编辑。你会收到本地统计数据和完整群聊消息，请直接产出适合日报/周报页面渲染的最终结构化结果，并只输出 json。

要求：
1. 只基于输入，不得补充不存在的数字。
2. 这是 direct_range 模式，请直接从原始消息提炼主题，不要先按连续时间片机械概括。
3. sections 是报告主体，表示“话题簇”而不是机械时间段；数量控制在 8-15 段，允许时间范围重叠。
4. 不要只保留抽象结论；保留关键人、具体事件、分歧点、工具/食物/运动/祝福等可复述细节。
5. 重复寒暄和刷屏内容可以合并，但持续时间较短且内容明确的话题也要覆盖。
6. sections 每段 bullets 最多 3 条；每条 bullet 应包含具体信息，不要写空泛评价。
7. theme_cards 最多 4 条，participant_insights 最多 8 条，quotes 最多 6 条。
8. action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
9. 消息中的 sender_ref 已是 `[[user:sender_id]]` 占位符；提到具体成员时保留该占位符，不要改写成昵称。
10. 输出必须是合法 JSON 对象，不要添加 markdown 或解释。
11. JSON 字段：headline, tagline, lead_summary, theme_cards, sections, participant_insights, quotes, decisions, action_items, open_questions, risk_flags, mood。
12. sections 字段：title, start_time, end_time, summary, bullets, takeaway。
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”生成最终结构化报表 json。

统计区间：{start_time} ~ {end_time}

紧凑统计：
{json.dumps(compact_stats, ensure_ascii=False, separators=(",", ":"))}

完整 direct_range 消息采用紧凑文本格式，字段顺序为：
time|sender_ref|message_type|text

{json.dumps(compact_direct_chunk_payload(chunk), ensure_ascii=False, separators=(",", ":"))}
""".strip()
    return system_prompt, user_prompt


def build_topic_plan_prompts(
    chat_name: str,
    start_time: str,
    end_time: str,
    stats: dict[str, Any],
    chunk: MessageChunk,
    max_topics: int,
) -> tuple[str, str]:
    """构造 topic-first 第一阶段主题聚类计划提示词。"""
    compact_stats = compact_prompt_stats(stats)
    payload = compact_topic_index_payload(chunk)
    system_prompt = f"""
你是一个群聊主题聚类器。你会收到完整群聊消息的紧凑索引，请只输出 JSON。

任务：
1. 按“话题簇”聚类，而不是按连续时间段切片。
2. 同一时间段内可以有多个 topic；同一条消息也可以被多个 topic 引用。
3. 覆盖主要话题，也保留短但内容明确的支线话题。
4. 每个 topic 的 message_indexes 必须引用输入里的 idx，按相关度和时间顺序列出。
5. topic 数量控制在 8-{max_topics} 个；如果内容不足可以少于 8 个。
6. 输出 JSON 字段：topics。
7. 每个 topic 字段：topic_id, title, summary, message_indexes, start_time, end_time, priority。
8. priority 只能是 major 或 minor。
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”生成主题聚类计划。

统计区间：{start_time} ~ {end_time}

紧凑统计：
{json.dumps(compact_stats, ensure_ascii=False, separators=(",", ":"))}

消息格式：
idx|time|sender_ref|message_type|text

输入消息：
{json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}
""".strip()
    return system_prompt, user_prompt


def build_topic_section_prompts(
    chat_name: str,
    topic: dict[str, Any],
    messages: list[StructuredMessage],
) -> tuple[str, str]:
    """构造 topic-first 单主题 section 分析提示词。"""
    payload = compact_topic_section_payload(topic, messages)
    system_prompt = """
你是一个群聊话题 section 分析器。你会收到一个 topic 的相关原始消息，请只输出 JSON。

要求：
1. 只分析当前 topic，不要扩展到无关话题。
2. 保留具体事件、分歧点、成员动作和可复述细节。
3. section 字段：title, start_time, end_time, summary, bullets, takeaway。
4. bullets 最多 3 条，必须具体。
5. quotes 最多 2 条，字段：speaker, time, quote, message_id, why_it_matters。
6. participant_insights 最多 3 条，字段：name, insight。
7. decisions/action_items/open_questions/risk_flags 只有在确实没有明确事项、问题或风险时才返回空数组，不要为省略而置空。
8. 提到成员时保留 `[[user:sender_id]]` 占位符。
9. 输出 JSON 字段：topic_id, section, participant_insights, quotes, decisions, action_items, open_questions, risk_flags。
""".strip()
    user_prompt = f"""
请为群聊“{chat_name}”的这个 topic 生成一个详细 section。

输入格式：
message_id|time|sender_ref|message_type|text

topic 与相关消息：
{json.dumps(payload, ensure_ascii=False, separators=(",", ":"))}
""".strip()
    return system_prompt, user_prompt
