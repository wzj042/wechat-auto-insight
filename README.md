# 微信能力工作区

这个仓库是一个本地父工作区，用来编排微信数据库处理、消息查询、群聊日报生成，以及 Windows PC 微信 UI 自动化发送。

当前工作区由三部分组成：

- `wechat-decrypt/`：微信 4.x 数据库密钥提取、SQLCipher 数据库解密、MCP 查询能力。
- `pywechat/`：Windows 10/11 下的 PC 微信 UI 自动化能力；其中 `pyweixin/` 适配微信 4.1+，`pywechat/` 主要适配微信 3.9.x。
- `group_insight/`：基于已解密消息生成群聊洞察日报，并可导出 HTML/PNG、通过 UI 自动化发送，默认走 DeepSeek 分析链路。

当前文档口径按收口后的运行原则维护：

- `group_insight` 只把仓库根目录 `.env` 视为受支持的本地配置入口。
- 发送、调度和分析流程以显式参数为准，支持 `--auto-time`、`--thinking`、`--allow-json-repair` 等开关，不再在总览文档里展开旧兼容参数。
- 缺少关键输入、依赖或运行条件时优先直接失败，避免静默兜底掩盖问题。

## 仓库定位

根目录是父仓库，不直接承载 `wechat-decrypt/` 和 `pywechat/` 的完整源码历史。

- `wechat-decrypt/` 和 `pywechat/` 通过 `.gitmodules` 记录来源。
- 父仓库只记录两个子模块的 gitlink 指针。
- 如果更新子模块源码，应先在各自仓库提交或切到目标 commit，再回到父仓库记录新的指针。

## 子模块职责

### `wechat-decrypt/`

负责数据库侧能力：

- 提取当前登录微信账号的数据库密钥
- 解密本地 SQLCipher 数据库
- 通过 MCP 提供最近会话、聊天记录、消息搜索、联系人、标签、图片解码等查询能力

常见入口：

```powershell
cd .\wechat-decrypt
python main.py
python main.py decrypt
```

### `pywechat/`

负责 Windows UI 自动化侧能力：

- 打开微信会话
- 发送文本、文件、图片
- 读取会话窗口和联系人相关控件

版本选择建议：

- 微信 4.1+：优先使用 `pywechat/pyweixin/`
- 微信 3.9.x：再看 `pywechat/pywechat/`

## 群聊日报模块

`group_insight/` 是当前父仓库内自维护的业务包，不是 Git 子模块。

它负责：

- 读取已解密微信消息
- 构造 LLM 分析流程（固定 map/reduce/final，含阶段缓存）
- 生成 JSON、HTML、PNG 报表
- 通过 Windows UI 自动化发送报表图片（支持多目标）
- 注册每日定时任务
- 可选异常告警邮件

详细运行说明见：

- `group_insight/README.md`

## 初始化

克隆或恢复仓库后，先初始化子模块：

```powershell
git submodule update --init --recursive
```

创建 Python 3.10 环境并安装根目录依赖：

```powershell
uv venv .venv --python 3.10
.\.venv\Scripts\Activate.ps1
uv pip install -r requirements.txt
```

如果不用 `uv`，也可以使用 `python -m venv .venv`，但后续命令仍建议在 `.venv` 激活状态下运行。

## 目录速查

- `README.md`：父工作区总览。
- `AGENTS.md`：给编码代理的维护说明。
- `group_insight/README.md`：群聊日报模块的运行说明。
- `group_insight/`：日报分析、渲染、发送与调度代码（含消息拉取、分片、统计、LLM 流水线、缓存）。
- `wechat-decrypt/`：数据库密钥提取、解密、MCP 查询。
- `pywechat/`：Windows 微信 UI 自动化。
- `.env.example`：`group_insight` 的 DeepSeek 环境变量模板。
- `requirements.txt`：根目录 Python 依赖。
- `reports/`：本地生成物目录。
