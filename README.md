# RelayGate

**English** | [简体中文](README.zh-CN.md)

A self-hosted, multi-user API gateway for LLM providers. Put several upstream providers behind one set of **public model IDs**, and let your users call them with OpenAI Chat Completions, OpenAI Responses or Anthropic Messages clients — with failover, per-user quotas, billing and full request logs.

Built with FastAPI + SQLite. No build step, no external services, and a plain-JS admin console.

## Features

- **Three client protocols** — `POST /v1/chat/completions`, `POST /v1/responses`, `POST /v1/messages`, plus OpenAI images and audio endpoints. Any client format can be routed to any language-model upstream; the gateway converts text, images, function tools and streaming events between formats.
- **Public / upstream model isolation** — downstream users only ever see the public model ID you define. Upstream model IDs, credentials and retryable upstream errors are never leaked in responses, SSE streams or errors.
- **Routing with failover** — one public model can have many routes (channel + upstream model). Choose *failover* (ordered) or *random load balancing* per model. A request falls through to the next route on failure until the first valid stream event has been sent.
- **Same-format passthrough** — when client and upstream formats match, the original request bytes are forwarded; only the top-level `model` (and channel auth) is replaced.
- **Multimodal channels** — dedicated channels for OpenAI image generation/editing, TTS and speech-to-text. Language channels (Chat / Responses / Anthropic) only carry language models; this is enforced in the backend.
- **Users, keys and quotas** — admin and regular users, per-user model permissions and balances, multiple API keys per user, optional open registration, redemption codes, ban/unban.
- **Flexible billing** — per-million-token pricing (input, output, cache read, cache write) or fixed price per request; TTS is billed per million input characters. Price `0` makes a model free. Cost is deducted after a successful response.
- **Complete logs** — compressed in SQLite: full request/response, stream events and media (Base64). Streaming output is staged to disk and written in chunks, so long replies don't sit in memory. Secrets and cookies are masked. Each log shows every attempted route and its result.
- **Admin console** — channels, upstream-model fetching, public models, routes, users, keys, logs, announcements, site branding (name, currency, copy, logo, banner, theme colors, custom CSS). Mobile friendly.
- **Small footprint** — runs comfortably on a ~1 GB RAM machine.

## Quick start

Requirements: Python 3.10+.

```bash
git clone https://github.com/XMWML/relaygate.git
cd relaygate
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
```

On first start the gateway creates `config.json` (mode `0600`) containing a random initial password for the `admin` account, a session secret and a `secret_storage_key`, and creates the SQLite database in `data/gateway.sqlite3`.

1. Read the initial admin password from `config.json` and sign in at `http://127.0.0.1:8000` as `admin`. You can change the password under *Account settings*; afterwards the database value wins and `config.json` keeps the original.
2. **Add a channel** — pick the upstream format (Chat Completions, Responses, Anthropic Messages, or OpenAI Images / TTS / Speech-to-text), enter the base URL and key. Use the root URL, e.g. `https://api.example.com` or `https://api.example.com/v1`; the gateway appends the `/v1/...` path itself. Do **not** enter a concrete endpoint such as `/chat/completions`.
3. **Add upstream models** — fetch the channel's model list in one click, or type custom IDs. The upstream model type follows the channel format. Language models get capability toggles (reasoning, vision, image input, tool calls) that default to all-on.
4. **Create public models** — import or create them under *Downstream models*, then add routes: pick a channel, then one of its upstream models (or a custom ID). Reorder routes with ↑/↓. *Import all models from channel* adds every compatible upstream model as a route in one go.
5. **Set prices** on the model (see [Billing](#billing)).
6. **Create users** — assign model permissions and balance. Users create their own API keys and view their own logs.
7. Point your client at the gateway:

```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Authorization: Bearer <downstream-key>" \
  -H "Content-Type: application/json" \
  -d '{"model":"<public-model-id>","messages":[{"role":"user","content":"Hello"}]}'
```

## Client endpoints

| Protocol | Endpoint |
|---|---|
| OpenAI Chat Completions | `POST /v1/chat/completions` |
| OpenAI Responses | `POST /v1/responses` |
| Anthropic Messages | `POST /v1/messages` |
| OpenAI Images | `POST /v1/images/generations`, `POST /v1/images/edits` |
| OpenAI Audio | `POST /v1/audio/speech`, `/v1/audio/transcriptions`, `/v1/audio/translations` |
| Model list | `GET /v1/models` (public models visible to the key) |

Authenticate with `Authorization: Bearer <key>`; Anthropic clients may use `x-api-key: <key>` instead. Cross-format conversion supports common text, image and function-tool content. Parameters with no equivalent in the target format are *rejected for that route* (and the next route is tried) rather than silently dropped. Image and audio endpoints need OpenAI-compatible channels.

## Routing and failover

- Each request re-reads the model's enabled routes; a past failure never disables a channel.
- *Failover* starts from the first route; *random* shuffles the routes for every request. After a route succeeds, later routes are not contacted.
- A route is retried on configurable HTTP statuses (default `403, 408, 409, 429, 500, 502, 503, 504`) or error texts, and on connect / first-token / idle / total timeouts.
- Switching is possible until the HTTP response has started and the first valid stream event has been received. After that, content already sent to the client cannot be taken back.
- Upstream model IDs are visible only to admins in the log UI; they are stripped from regular users' log API.

## Billing

Each model has a `billing_mode`:

- **Per token** — language, image and speech-to-text models are charged per million tokens from the upstream `usage`. Uncached input uses the input price; cache-read and cache-write prices fall back to the input price when left empty. TTS is charged per million characters of the request `input`.
- **Per request** — a fixed unit price per successful request. Use this for models whose upstream returns no usage (e.g. DALL·E, Whisper).

If every relevant price is `0` the model is free and users with zero or negative balance can call it. Charges are applied after the response completes, so a balance can go slightly negative on the last request. Admins always have access to all models and unlimited balance; users can also be flagged "unlimited balance".

## Security notes

- Full downstream API keys are shown only to their owner, and redemption codes only to admins. Both are stored encrypted with `secret_storage_key` from `config.json` (a hash is kept for verification). Keys and codes generated by very old versions (hash only) cannot be recovered and must be regenerated.
- **Back up `data/gateway.sqlite3` and `config.json` together.** Without `secret_storage_key` the encrypted keys and codes cannot be displayed again. Upstream API keys live in the database, so protect backups. Never publish either file.
- Resetting a user's password revokes their other sessions. The password field uses `autocomplete="new-password"` to keep browsers from auto-filling a saved password.
- Limits: JSON request body 24 MB, non-streaming JSON upstream response 32 MB. Multipart uploads and binary responses use temp files and chunked logs.
- Run behind a TLS-terminating reverse proxy. Bind Uvicorn to `127.0.0.1`.

## Configuration

| Environment variable | Purpose | Default |
|---|---|---|
| `NEWER_API_DB_PATH` | SQLite database path | `./data/gateway.sqlite3` |
| `NEWER_API_CONFIG_PATH` | Bootstrap config path | `./config.json` |
| `NEWER_API_TEST_INSTANCE` | Mark an instance as a test instance (`1`) | unset |

Channels can optionally connect through an HTTP/HTTPS proxy (default `http://127.0.0.1:7890`, editable per channel). Timeouts, retry statuses and retry texts are adjustable in site settings.

### Branding

The admin *Site settings* page lets you change the site name, currency name (default "喵币"), welcome text and main page copy. *Global text replace* accepts a JSON map of original → new strings for the rest of the UI. Upload a logo, login illustration, banner (with adjustable overlay) and favicon; set colors and fonts, or add custom CSS. Branding lives in the database, so upgrading never overwrites it. Announcements (text + image, draft/published) are managed under *Announcements*.

> The bundled UI strings are Chinese; use the text-replacement feature to localize them.

## Deployment

Example files are in [`deploy/`](deploy):

- `relaygate.service` — systemd unit (adjust user and paths):
  ```bash
  sudo cp deploy/relaygate.service /etc/systemd/system/
  sudo systemctl enable --now relaygate
  journalctl -u relaygate -f
  ```
- `nginx.conf` — TLS reverse proxy with buffering disabled, required for SSE streaming and large uploads.

## Project layout

| File | Role |
|---|---|
| `app.py` | FastAPI routes, auth, channels/models/routes admin, billing, failover, header forwarding, logs, media |
| `conversion.py` | Request / non-streaming response conversion between Chat, Responses and Anthropic; unified usage numbers |
| `streaming.py` | SSE parsing, cross-format stream conversion, model-ID rewriting |
| `db.py` | SQLite schema, incremental migrations, settings, secret encryption, compressed logs |
| `static/` | Build-free vanilla JS front end (bump the `?v=` query in `index.html` after editing JS/CSS) |
| `tests/` | Unit and integration tests |

## Testing

Unit tests:

```bash
.venv/bin/python -m unittest tests/test_conversion.py
```

The integration script talks to an **isolated test instance** with its own database and config, and has a built-in mock upstream. It refuses to run against a non-test instance.

```bash
T=$(mktemp -d)
NEWER_API_DB_PATH=$T/gateway.sqlite3 NEWER_API_CONFIG_PATH=$T/config.json NEWER_API_TEST_INSTANCE=1 \
  .venv/bin/uvicorn app:app --host 127.0.0.1 --port 19188 &
NEWER_API_TEST_BASE=http://127.0.0.1:19188 NEWER_API_TEST_CONFIG=$T/config.json NEWER_API_TEST_DB=$T/gateway.sqlite3 \
  .venv/bin/python tests/integration.py
kill %1; rm -rf $T
```

## License

[MIT](LICENSE)
