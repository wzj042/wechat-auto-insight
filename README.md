# README

用于在 Windows 10/11 上生成微信群聊日报，并在需要时通过 PC 微信 UI 自动化发送报表图片。

能力边界：

- `group_insight_report.py` 是兼容入口，实际逻辑在 `group_insight/`。
- `group_insight/` 负责读取已解密微信消息、构造 LLM 分析流程、生成 JSON/HTML/PNG，并可调用 RPA 发送。
- `wechat-decrypt/` 负责微信 4.x 数据库密钥提取、解密和 MCP 查询。
- `pywechat/pyweixin/` 负责 Windows PC 微信 UI 自动化。

## 初始化

克隆或恢复仓库后，先初始化子模块：

```powershell
git submodule update --init --recursive
```

创建 Python 3.10 环境并安装依赖：

```powershell
uv venv .venv --python 3.10
.\.venv\Scripts\Activate.ps1
uv pip install -r requirements.txt
```

如果不用 `uv`，也可以用普通 `python -m venv .venv`，但后续命令仍建议在 `.venv` 激活状态下运行。

## 配置 `.env`

复制样例文件：

```powershell
Copy-Item .env.example .env
```

至少填写一个模型提供方的 Key：

```dotenv
GROUP_INSIGHT_PROVIDER=deepseek
DEEPSEEK_API_KEY=sk-your-deepseek-api-key
DEEPSEEK_MODEL=deepseek-chat
DEEPSEEK_API_URL=https://api.deepseek.com/chat/completions

ZHIPUAI_API_KEY=your-zhipu-api-key
ZHIPUAI_MODEL=glm-4.5-flash
```

`.env` 是本机私有文件，不要提交。程序启动时会优先读取仓库根目录 `.env`，再兼容当前工作目录和父目录；已经存在于系统环境变量里的同名 Key 不会被覆盖。

验证 `.env` 是否能被日报脚本读到：

```powershell
.\.venv\Scripts\python.exe -c "import os; import group_insight.settings; print(bool(os.environ.get('DEEPSEEK_API_KEY') or os.environ.get('ZHIPUAI_API_KEY')))"
```

输出 `True` 表示至少一个模型 Key 已进入当前进程环境。

## 微信数据库准备

数据库侧依赖 `wechat-decrypt/`。第一次运行前确认：

- Windows PC 微信已登录并正在运行。
- 需要读取进程内存提取密钥时，PowerShell 通常要用管理员权限启动。
- `wechat-decrypt/config.json` 已存在；如果没有，执行时会自动填充
- `wechat-decrypt/all_keys.json` 和 `wechat-decrypt/decrypted/` 对应当前登录账号。

常用命令：

```powershell
cd .\wechat-decrypt
python main.py decrypt
cd ..
```

如果已经解密过数据库，只需要确认 `wechat-decrypt/config.json` 指向正确的 `db_dir`、`keys_file` 和 `decrypted_dir`。

## 生成日报

默认运行：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py
```

指定群聊和时间窗：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --chat "有氧运动聊天" --start "2026-04-14 23:59" --end "2026-04-15 23:59"
```

只验证读取、分片、渲染链路，不调用模型、不发送：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --dry-run --no-image --no-send-after-run
```

生成 HTML 但跳过 PNG：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --no-image --no-send-after-run
```

兼容入口 `group_insight_report.py` 会在发现仓库 `.venv` 存在时自动切换到 `.venv\Scripts\python.exe`。如果确实要使用当前解释器，可以临时设置：

```powershell
$env:GROUP_INSIGHT_NO_VENV_REDIRECT = "1"
```

输出目录默认在：

```text
reports/group_insight/
```

每次运行会生成 JSON、HTML、PNG 和阶段缓存；输入签名变化时会自动清理过期阶段缓存。

## RPA 发送前预热

自动发送 PNG 依赖 Windows 桌面会话、微信主窗口和 UI Automation 控件树。定时任务或首次启动前，建议先做一次交互式预热。


1、关闭微信
2、打开讲述人模式（即屏幕阅读器无障碍模式，保持至少5分钟，期间不操作也可以）
3、打开微信测试是否可获取到相关控件树


预热成功后，建议再打开一次发送目标会话，确认名称能被自动化库找到：

```powershell
$env:PYTHONPATH = "$PWD\pywechat;$env:PYTHONPATH"
@'
from pyweixin.WeChatTools import Navigator

target = "文件传输助手"
Navigator.open_dialog_window(friend=target, is_maximize=False, search_pages=0)
print("RPA target opened:", target)
'@ | python -
```

如果目标群不在最近会话列表里，可以把 `search_pages=0` 保持为顶部搜索，或先手动在微信里打开目标会话。

执行一次真实发送测试：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --dry-run --send-after-run --send-target "文件传输助手"
```

常见问题：

- 报 `微信未运行`：先手动启动并登录 PC 微信，再运行预热。
- 报找不到窗口或控件：确认没有锁屏、远程桌面没有断开、微信没有最小化到托盘。
- 报找不到会话：确认 `--send-target` 是微信里可搜索到的完整备注、好友名或群名。
- 定时任务能生成报表但不能发送：任务必须在用户已登录的交互式桌面会话中运行，不能依赖无人登录的后台会话。

## 自动发送

生成后发送到默认目标：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --send-after-run
```

指定一个或多个目标：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --send-after-run --send-target "文件传输助手" --send-target "有氧运动聊天"
```

只发送到文件传输助手兼容参数：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --send-to-filehelper
```

## 任务计划

注册每日任务：

```powershell
python .\schedule_group_insight_report.py --time 23:50
```

注册脚本默认优先把任务动作指向仓库 `.venv\Scripts\python.exe`。如果任务曾经用全局 Python 注册过，重新运行一次注册脚本即可更新。

先预览不写入任务计划：

```powershell
python .\schedule_group_insight_report.py --time 23:50 --args "--no-image" --dry-run
```

传递日报参数：

```powershell
python .\schedule_group_insight_report.py --time 23:50 --args "--chat 有氧运动聊天 --send-after-run"
```

使用最高权限注册：

```powershell
python .\schedule_group_insight_report.py --time 23:50 --highest
```

任务计划维护命令：

```powershell
# 查看任务是否存在
Get-ScheduledTask -TaskName GroupInsightReportDaily -ErrorAction SilentlyContinue

# 查看上次运行、下次运行、结果码
Get-ScheduledTaskInfo -TaskName GroupInsightReportDaily

# 查看详细信息
schtasks /Query /TN GroupInsightReportDaily /V /FO LIST

# 立刻手动运行一次
Start-ScheduledTask -TaskName GroupInsightReportDaily

# 禁用任务
Disable-ScheduledTask -TaskName GroupInsightReportDaily

# 重新启用任务
Enable-ScheduledTask -TaskName GroupInsightReportDaily

# 删除任务
Unregister-ScheduledTask -TaskName GroupInsightReportDaily -Confirm:$false
```

重新运行 `schedule_group_insight_report.py` 会 create-or-update 同名任务；修改任务名等于注册新任务，旧任务需要单独删除。

## 常见依赖问题

如果看到下面的错误：

```text
ImportError: cannot import name 'Sentinel' from 'typing_extensions'
```

通常说明当前使用的是全局 Python，里面的 `typing_extensions` 太旧，和 `mcp/pydantic_core` 不兼容。优先使用仓库 `.venv`：

```powershell
.\.venv\Scripts\python.exe .\group_insight_report.py --dry-run --no-image --no-send-after-run
```

也可以重新安装根目录依赖：

```powershell
uv pip install -r requirements.txt
```

如果必须修全局 Python：

```powershell
python -m pip install -U typing-extensions
```

## 目录速查

- `.env.example`：可提交的环境变量模板。
- `.env`：本机私有密钥，不提交。
- `requirements.txt`：根目录日报与 RPA 依赖。
- `pyrightconfig.json`：VSCode/Pyright 本地路径配置。
- `group_insight_report.py`：兼容入口。
- `group_insight/cli.py`：命令行参数和主流程。
- `group_insight/settings.py`：默认值、路径、`.env` 加载和 MCP 懒加载。
- `group_insight/conversation.py`：消息清洗、分类、统计和分片。
- `group_insight/pipeline.py`：map/reduce/final、direct-final、topic-first 分析流水线。
- `group_insight/rendering.py`：HTML 渲染和最终 payload。
- `group_insight/transport.py`：PNG 导出和 RPA 发送。
- `schedule_group_insight_report.py`：Windows 任务计划注册脚本。
