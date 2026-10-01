# Newer-api

[English](README.md) | **简体中文**

自托管的多用户大模型 API 网关。把多个上游服务商统一到一组**公开模型 ID** 之后，用户可以用 OpenAI Chat Completions、OpenAI Responses 或 Anthropic Messages 客户端调用，并带有失败切换、用户额度、计费和完整请求日志。

基于 FastAPI + SQLite，无构建步骤，无外部依赖服务，管理后台为原生 JS。

## 功能特性

- **三种客户端协议**：`POST /v1/chat/completions`、`POST /v1/responses`、`POST /v1/messages`，另有 OpenAI 图像与语音接口。任意客户端格式都能路由到任意语言模型上游，网关负责文本、图片、函数工具和流式事件的跨格式转换。
- **公开/上游模型 ID 隔离**：下游只能看到你定义的公开模型 ID；响应、SSE、错误中不会泄露上游模型 ID、上游凭据或可重试的上游错误。
- **路由与失败切换**：一个公开模型可有多条路由（渠道 + 上游模型），每个模型可选“失败切换”（按顺序）或“随机负载”。失败时自动尝试下一条路由，直到首个有效流式事件发出为止。
- **同格式透传**：客户端与上游格式一致时转发原始请求字节，只替换顶层 `model` 和渠道鉴权。
- **多模态专用渠道**：OpenAI 图像生成/编辑、TTS、语音识别使用专用渠道。Chat / Responses / Anthropic 渠道只承载语言模型，后端强制校验。
- **用户、密钥与额度**：管理员与普通用户、按用户配置模型权限与余额、每用户多个 API Key、可选开放注册、兑换码、封禁/解封。
- **灵活计费**：按每百万 token 计价（输入、输出、缓存读取、缓存写入）或按次固定计价；TTS 按每百万输入字符计费。价格为 `0` 即免费模型。成功响应后才扣费。
- **完整日志**：压缩存入 SQLite，包含完整请求/响应、流式事件和媒体（Base64）。流式输出先暂存到磁盘再分块写库，长回复不会长期占用内存；密钥与 Cookie 会被遮盖；每条日志显示实际尝试过的每条路由及结果。
- **管理后台**：渠道、上游模型拉取、公开模型、路由、用户、密钥、日志、公告、站点外观（名称、货币、文案、标志、横幅、主题色、自定义 CSS），适配手机。
- **占用小**：约 1 GB 内存的机器即可流畅运行。

## 快速开始

要求：Python 3.10+。

```bash
git clone https://github.com/XMWML/Newer-api.git
cd Newer-api
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
```

首次启动会生成 `config.json`（权限 `0600`），包含 `admin` 账户的随机初始密码、会话密钥和 `secret_storage_key`，并在 `data/gateway.sqlite3` 创建数据库。

1. 从 `config.json` 读取初始管理员密码，访问 `http://127.0.0.1:8000` 以 `admin` 登录。可在“账户设置”修改密码；此后以数据库中的密码为准，配置文件仍保留初始值。
2. **添加渠道**：选择上游格式（Chat Completions、Responses、Anthropic Messages，或 OpenAI 图像 / TTS / 语音识别），填写上游地址与密钥。地址填根地址，如 `https://api.example.com` 或 `https://api.example.com/v1` 均可，网关会自动拼接 `/v1/...`；**不要**填到 `/chat/completions` 等具体接口。
3. **添加上游模型**：在渠道的“模型”里一键拉取，或手动填写。上游模型类型由渠道格式决定。语言模型的推理、视觉、图片输入、工具调用能力默认全选，可调整。
4. **创建公开模型**：在“下游模型”里导入或新建，再添加路由：先选渠道，再选该渠道的上游模型（或手填自定义 ID），用 ↑/↓ 调整顺序。“从渠道导入全部模型”可一次把该渠道所有同类型上游模型加为路由。
5. **设置价格**，见[计费](#计费)。
6. **创建用户**：分配模型权限和余额。用户可自行创建 API Key 并查看自己的日志。
7. 客户端指向网关：

```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer <下游密钥>" \
  -H "Content-Type: application/json" \
  -d '{"model":"<公开模型ID>","messages":[{"role":"user","content":"你好"}]}'
```

## 客户端接口

| 协议 | 接口 |
|---|---|
| OpenAI Chat Completions | `POST /v1/chat/completions` |
| OpenAI Responses | `POST /v1/responses` |
| Anthropic Messages | `POST /v1/messages` |
| OpenAI 图像 | `POST /v1/images/generations`、`POST /v1/images/edits` |
| OpenAI 语音 | `POST /v1/audio/speech`、`/v1/audio/transcriptions`、`/v1/audio/translations` |
| 模型列表 | `GET /v1/models`（该密钥可见的公开模型） |

使用 `Authorization: Bearer <密钥>` 鉴权；Anthropic 客户端也可用 `x-api-key: <密钥>`。跨格式转换支持常见的文本、图片与函数工具内容；目标格式没有等价语义的参数会让该路由**拒绝转换并尝试下一条路由**，而不是静默丢弃。图像和语音接口需要 OpenAI 兼容渠道。

## 路由与失败切换

- 每个新请求都会重新读取该模型已启用的路由，之前的失败不会停用渠道。
- “失败切换”从第一条路由开始；“随机负载”每次请求随机排列路由。某条路由成功后不再访问后面的路由。
- 命中可配置的 HTTP 状态码（默认 `403, 408, 409, 429, 500, 502, 503, 504`）或错误文本，以及连接 / 首包 / 空闲 / 总超时，都会切换。
- 在 HTTP 响应开始并收到首个有效流式事件之前可以切换；之后已发给客户端的内容无法撤回。
- 日志中的上游模型 ID 只向管理员显示，普通用户的日志接口会去掉该字段。

## 计费

每个模型有 `billing_mode`：

- **按量**：语言、图像、语音识别模型按上游 `usage` 每百万 token 计价。未命中缓存的输入用输入价格，缓存读取/写入价格留空时回退到输入价格；TTS 按请求 `input` 的每百万字符计价。
- **按次**：每次成功请求扣固定费用。适用于上游不返回 usage 的模型（如 DALL·E、Whisper）。

所有相关价格为 0 即免费模型，余额为零或负数的用户也可调用。费用在回答完成后扣除，因此最后一次请求可能使余额略为负数。管理员始终拥有全部模型和无限余额；也可给普通用户勾选“无限余额”。

## 安全说明

- 完整的下游 API Key 只向所属账户展示，兑换码只向管理员展示；二者用 `config.json` 中的 `secret_storage_key` 加密保存（数据库另存哈希用于验证）。旧版只存哈希的密钥与兑换码无法还原，需重新生成。
- **务必一起备份 `data/gateway.sqlite3` 与 `config.json`**。丢失 `secret_storage_key` 后无法再显示已加密的密钥与兑换码。上游 API Key 存在数据库中，备份也要妥善保护。切勿公开这两个文件。
- 重设用户密码会使其他登录会话失效；密码输入框使用 `autocomplete="new-password"`，避免浏览器自动填入已保存的密码。
- 限制：JSON 请求体最多 24 MB，非流式 JSON 上游响应最多 32 MB；multipart 文件和二进制响应使用暂存文件与分块日志。
- 请部署在支持 TLS 的反向代理之后，Uvicorn 只监听 `127.0.0.1`。

## 配置

| 环境变量 | 作用 | 默认值 |
|---|---|---|
| `NEWER_API_DB_PATH` | SQLite 数据库路径 | `./data/gateway.sqlite3` |
| `NEWER_API_CONFIG_PATH` | 引导配置路径 | `./config.json` |
| `NEWER_API_TEST_INSTANCE` | 标记为测试实例（`1`） | 未设置 |

渠道可选择通过 HTTP/HTTPS 代理连接上游（默认 `http://127.0.0.1:7890`，可按渠道修改）。超时、重试状态码和重试文本可在站点设置中调整。

### 外观与文案

管理后台“站点设置”可调整站点名称、货币名称（默认“喵币”）、欢迎语和主要页面文案；“全站文字替换”可用“原文案 → 新文案”的 JSON 映射替换其余界面文字。可上传标志、登录页插画、首页横幅（遮罩浓度可调）和浏览器图标，设置颜色与字体，或添加自定义 CSS。外观保存在数据库中，升级程序不会覆盖。“公告”页面可管理图文公告（草稿/发布）。

## 部署

示例文件位于 [`deploy/`](deploy)：

- `newer-api.service`：systemd 单元（按实际用户和路径修改）：
  ```bash
  sudo cp deploy/newer-api.service /etc/systemd/system/
  sudo systemctl enable --now newer-api
  journalctl -u newer-api -f
  ```
- `nginx.conf`：TLS 反向代理，已关闭缓冲，这是 SSE 流式和大文件上传所必需的。

## 项目结构

| 文件 | 作用 |
|---|---|
| `app.py` | FastAPI 路由、鉴权、渠道/模型/路由管理、计费、失败切换、请求头转发、日志、媒体接口 |
| `conversion.py` | Chat、Responses、Anthropic 之间的请求与非流式响应转换，统一用量统计 |
| `streaming.py` | SSE 解析、跨格式流转换、模型 ID 改写 |
| `db.py` | SQLite 结构、增量迁移、设置、密钥加密、压缩日志 |
| `static/` | 无构建步骤的原生 JS 前端（改 JS/CSS 后更新 `index.html` 中的 `?v=`） |
| `tests/` | 单元测试与集成测试 |

## 测试

单元测试：

```bash
.venv/bin/python -m unittest tests/test_conversion.py
```

集成脚本只连接带独立数据库和配置的**隔离测试实例**，内置 mock 上游，不会对非测试实例运行。

```bash
T=$(mktemp -d)
NEWER_API_DB_PATH=$T/gateway.sqlite3 NEWER_API_CONFIG_PATH=$T/config.json NEWER_API_TEST_INSTANCE=1 \
  .venv/bin/uvicorn app:app --host 127.0.0.1 --port 19188 &
NEWER_API_TEST_BASE=http://127.0.0.1:19188 NEWER_API_TEST_CONFIG=$T/config.json NEWER_API_TEST_DB=$T/gateway.sqlite3 \
  .venv/bin/python tests/integration.py
kill %1; rm -rf $T
```

## 许可证

[MIT](LICENSE)
