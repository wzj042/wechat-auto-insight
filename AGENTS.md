# AGENTS.md

## 仓库概况

- 当前目录是本地“微信能力”父工作区，根目录现在按父 Git 仓库管理。
- `wechat-decrypt/` 和 `pywechat/` 是独立上游仓库，在父仓库里作为依赖模块登记，不把它们的源码直接并入根目录历史。
- 根目录只维护本地编排脚本、运行说明、任务计划脚本、LLM 辅助工具和生成物边界。
- 这个工作区同时包含两类能力：
  - 数据库侧：微信 4.x 本地数据库密钥提取、SQLCipher 数据库解密、MCP 查询、Web 消息监控。
  - UI 自动化侧：Windows PC 微信发消息、读聊天窗口、联系人/群操作等自动化。

## Git 结构

- 根目录是父仓库；两个依赖模块通过 `.gitmodules` 记录来源：
  - `wechat-decrypt/` -> `https://github.com/ylytdeng/wechat-decrypt.git`
  - `pywechat/` -> `https://github.com/Hello-Mr-Crab/pywechat`
- 父仓库只记录两个依赖模块的 gitlink 指针。更新子模块源码后，需要在对应子仓库提交或切换到目标 commit，再回到父仓库记录新的指针。
- 克隆或恢复父仓库时，需要递归初始化依赖模块：
  - `git submodule update --init --recursive`
- 不要在父仓库里直接展开提交 `wechat-decrypt/` 或 `pywechat/` 的完整文件内容；这两个目录的改动应优先回到各自仓库处理。
- `.env`、缓存目录、编辑器状态和 `reports/` 都是本地状态或运行产物，默认不进入父仓库。

## 依赖模块角色

- `wechat-decrypt/main.py`
  - 微信数据库能力总入口。
  - `python main.py`：提取密钥并启动 Web UI。
  - `python main.py decrypt`：提取密钥并解密数据库到 `decrypted/`。
- `wechat-decrypt/mcp_server.py`
  - FastMCP 服务。
  - 负责查询最近会话、聊天记录、消息搜索、联系人、联系人标签、图片解码等。
- `wechat-decrypt/config.example.json`
  - 说明配置格式，关键字段是 `db_dir`、`keys_file`、`decrypted_dir`、`wechat_process`。
- `pywechat/pyweixin/`
  - 面向 Windows 10/11、Python 3.10+、微信 4.1+ 的 UI 自动化代码。
- `pywechat/pywechat/`
  - 旧版自动化代码，主要面向微信 3.9.x。
  - `pywechat/pywechat/__init__.py` 有较强环境限制，不要和 `pyweixin` 的适用范围混淆。

## 根目录文件梳理

- `AGENTS.md`
  - 当前工作区给编码代理看的维护说明，记录目录边界、Git 结构、运行前提和修改注意事项。
- `README`
  - Windows 任务计划的本地运维速查，包含查看、运行、禁用、启用、删除和重新注册任务的命令。
- `group_insight_report.py`
  - 根目录主业务脚本。
  - 从 `wechat-decrypt` 的 MCP 查询能力读取已解密微信消息，生成群聊洞察 JSON/HTML/PNG。
  - 支持 DeepSeek 和智谱模型，支持自动时间窗、分片/聚合、缓存、PNG 导出，以及通过 `pywechat/pyweixin` 自动发送报告图片。
- `schedule_group_insight_report.py`
  - Windows 任务计划注册脚本。
  - 默认每天运行 `group_insight_report.py`，支持 `--time`、`--task-name`、`--python`、`--script`、`--args`、`--highest`、`--dry-run`。
- `zhipuai_tool.py`
  - 智谱 AI SDK 的轻量封装，提供文本对话、图像理解和 `create_zhipu_client` 便捷函数。
- `test_zhipuai_features.py`
  - 智谱 AI 能力验证脚本，用于测试文本分析、主题聚类、图片理解等实验功能。
- `.gitmodules`
  - 父仓库依赖模块清单，记录 `wechat-decrypt/` 和 `pywechat/` 的路径与远端地址。
- `.gitignore`
  - 根目录忽略规则，覆盖 `.env`、Python 缓存、编辑器状态、助手历史、报表产物和常见 Windows 临时文件。
- `.env`
  - 本机私有环境变量，可能包含 `DEEPSEEK_API_KEY`、`ZHIPUAI_API_KEY` 等密钥，不得提交。
- `reports/`
  - `group_insight_report.py` 的生成物目录，通常包含快照、阶段缓存、JSON、HTML、PNG，不作为源码维护。
- `.claude/`、`.history/`、`.ruff_cache/`、`.vscode/`、`__pycache__/`
  - 本地工具、编辑器和 Python 缓存状态，不作为源码维护。

## 技术栈与运行前提

- 操作系统按 Windows 11 / PowerShell 使用。
- 前端项目如后续出现，统一使用 `pnpm` 做包管理。
- `wechat-decrypt` 依赖：
  - `pycryptodome`
  - `zstandard`
  - `mcp`
- `pywechat` / `pyweixin` 依赖：
  - `pywinauto`
  - `pyautogui`
  - `psutil`
  - `pywin32`
  - `pycaw`
  - `pillow`
  - `emoji`
- 根目录 LLM/报告脚本可能额外依赖：
  - `zhipuai`
  - `python-dotenv`
  - `Pillow`
  - `jieba`
  - `playwright` 或本机 Chrome/Edge，用于 HTML 转 PNG。
- 运行数据库相关能力前，通常需要已登录且正在运行的微信进程；在 Windows 上读取进程内存时往往需要管理员权限。
- 做数据库相关工作前，先确认 `wechat-decrypt/config.json`、`all_keys.json`、`decrypted/` 是否存在，并且对应当前登录的微信账号。
- 做 RPA/UI 自动化前，先确认目标微信版本落在哪套库上：
  - 微信 4.1+：优先看 `pyweixin`
  - 微信 3.9.x：再看 `pywechat`

## 本地定制痕迹

- 根目录脚本存在明显本地运营脚本特征，不是完整参数化的通用 CLI。
- 当前 `group_insight_report.py` 默认分析群聊为 `有氧运动聊天`，默认 provider 为 `deepseek`，默认输出到 `reports/group_insight/`。
- 历史说明里曾出现过其他群名、群聊 ID、`room_id` 等硬编码业务参数；改逻辑前要以当前脚本源码为准。
- 部分脚本会通过 `sys.path.insert(...)` 把根目录、`wechat-decrypt/` 或 `pywechat/` 加入导入路径。继续新增脚本时，不要扩散更多硬编码绝对路径。
- 如果后续要长期维护或复用，优先做下面两件事：
  - 把群参数、数据库路径、目标联系人改为命令行参数或配置项。
  - 把 import/path 关系整理清楚，避免继续依赖本机绝对路径。

## 修改建议

- 除非任务明确要求，优先修改根目录本地脚本，不要随意大改 `pywechat/` 和 `wechat-decrypt/` 里的上游代码。
- 子模块内如果有必要修改，先确认该改动是本地补丁、fork 补丁，还是要提交给上游；不要把子模块源码复制到父仓库绕开 Git 边界。
- 根目录报表脚本同时触碰数据库读取、LLM 调用、HTML/PNG 渲染和 UI 自动发送，改动时优先用小范围 dry-run 或 `--no-image`、`--no-send-after-run` 验证。
- `schedule_group_insight_report.py` 会写 Windows 任务计划；调试时优先使用 `--dry-run`。
- 根目录某些统计或实验脚本可能并不都基于真实消息解密结果。改逻辑前先读脚本本身，不要假设它们全部是准确生产实现。

## 读取代码时的注意点

- 当前终端里直接 `Get-Content` 查看中文 README/文档时，部分内容可能出现乱码；优先使用 `Get-Content -Encoding utf8`。
- 遇到说明文档乱码时，优先以这些信息源判断真实行为：
  - Python 源码中的入口函数和 import 关系
  - `requirements.txt`
  - `setup.py`
  - `config.example.json`
- 读取大文件时先用 `rg` 定位关键函数或参数，再分段查看，不要整文件读取超大脚本。
