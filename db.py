import base64, gzip, hashlib, hmac, json, os, secrets, sqlite3, tempfile, time
from pathlib import Path
from cryptography.fernet import Fernet

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get('NEWER_API_DB_PATH',ROOT / 'data' / 'gateway.sqlite3')).resolve()
DATA = DB_PATH.parent
DATA.mkdir(exist_ok=True)
CONFIG_PATH = Path(os.environ.get('NEWER_API_CONFIG_PATH',ROOT / 'config.json')).resolve()

DEFAULT_SETTINGS = {
    'site_name': '模型 API 网关', 'tagline': '统一接入、管理与调用多渠道模型服务',
    'currency_name': '喵币', 'registration_open': False,
    'site_logo': '', 'login_image': '', 'hero_image': '', 'favicon': '',
    'hero_overlay_opacity': 0.22,
    'theme': {'accent':'#e493ad','accent_dark':'#c96d91','background':'#fbf9f8','ink':'#403d46','muted':'#8a8590','font':'-apple-system, BlinkMacSystemFont, "PingFang SC", sans-serif'},
    'custom_css': '',
    'text_overrides': {},
    'copy': {'login_art':'统一管理模型服务，安全、高效地接入 API。','login_button':'登录','register_button':'注册账号','dashboard_intro':'集中查看已授权模型、管理 API 密钥并了解账户用量。','dashboard_api_title':'API 接入','dashboard_api_hint':'通过统一接口调用您已获授权的模型。','dashboard_api_help':'兼容 OpenAI Chat Completions、Responses、Anthropic Messages，以及图像生成和语音接口。'},
    'retry_statuses': [403, 408, 409, 429, 500, 502, 503, 504],
    'retry_text': ['overloaded', 'temporarily unavailable', 'rate limit'],
    'connect_timeout': 10, 'first_token_timeout': 45, 'idle_timeout': 120,
    'total_timeout': 600, 'max_attempts_per_route': 1,
    'welcome_text': '欢迎使用模型 API 网关。',
    'footer_text': '模型 API 网关'
}

def conn():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA foreign_keys=ON')
    c.execute('PRAGMA busy_timeout=30000')
    return c

def initialize():
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps({'admin_username':'admin','admin_password':secrets.token_urlsafe(24),'session_secret':secrets.token_urlsafe(48)}, ensure_ascii=False, indent=2))
        os.chmod(CONFIG_PATH, 0o600)
    conf=json.loads(CONFIG_PATH.read_text())
    if not conf.get('secret_storage_key'):
        conf['secret_storage_key']=Fernet.generate_key().decode()
        CONFIG_PATH.write_text(json.dumps(conf,ensure_ascii=False,indent=2))
        os.chmod(CONFIG_PATH,0o600)
    with conn() as c:
        c.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, is_admin INTEGER NOT NULL DEFAULT 0, balance REAL NOT NULL DEFAULT 0, allowed_models TEXT NOT NULL DEFAULT '[]', created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, expires_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS api_keys(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, name TEXT NOT NULL, prefix TEXT NOT NULL, key_hash TEXT UNIQUE NOT NULL, allowed_models TEXT NOT NULL DEFAULT '[]', created_at INTEGER NOT NULL, last_used_at INTEGER, disabled INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY, name TEXT NOT NULL, format TEXT NOT NULL, base_url TEXT NOT NULL, api_key TEXT NOT NULL, extra_headers TEXT NOT NULL DEFAULT '{}', enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS upstream_models(id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE, model_id TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'language', capabilities TEXT NOT NULL DEFAULT '{}', UNIQUE(channel_id, model_id));
        CREATE TABLE IF NOT EXISTS models(id INTEGER PRIMARY KEY, public_id TEXT UNIQUE NOT NULL, kind TEXT NOT NULL DEFAULT 'language', capabilities TEXT NOT NULL DEFAULT '{}', input_price REAL NOT NULL DEFAULT 0, output_price REAL NOT NULL DEFAULT 0, unit_price REAL NOT NULL DEFAULT 0, mode TEXT NOT NULL DEFAULT 'failover', enabled INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS routes(id INTEGER PRIMARY KEY, model_id INTEGER NOT NULL REFERENCES models(id) ON DELETE CASCADE, channel_id INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE, upstream_model_id TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0, enabled INTEGER NOT NULL DEFAULT 1, UNIQUE(model_id,channel_id,upstream_model_id));
        CREATE TABLE IF NOT EXISTS redemption_codes(id INTEGER PRIMARY KEY, code_hash TEXT UNIQUE NOT NULL, prefix TEXT NOT NULL, amount REAL NOT NULL, max_uses INTEGER NOT NULL DEFAULT 1, uses INTEGER NOT NULL DEFAULT 0, expires_at INTEGER, created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS redemptions(id INTEGER PRIMARY KEY, code_id INTEGER NOT NULL REFERENCES redemption_codes(id), user_id INTEGER NOT NULL REFERENCES users(id), created_at INTEGER NOT NULL, UNIQUE(code_id,user_id));
        CREATE TABLE IF NOT EXISTS request_logs(id INTEGER PRIMARY KEY, created_at INTEGER NOT NULL, user_id INTEGER, key_id INTEGER, model_id TEXT, endpoint TEXT, status INTEGER, route_id INTEGER, channel_id INTEGER, duration_ms INTEGER, input_tokens INTEGER, output_tokens INTEGER, cost REAL, attempt_count INTEGER, summary TEXT, payload BLOB);
        CREATE TABLE IF NOT EXISTS announcements(id INTEGER PRIMARY KEY, title TEXT NOT NULL, body TEXT NOT NULL, image_url TEXT NOT NULL DEFAULT '', published INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_logs_user_created ON request_logs(user_id,created_at DESC);
        CREATE TABLE IF NOT EXISTS request_log_stream_chunks(log_id INTEGER NOT NULL REFERENCES request_logs(id) ON DELETE CASCADE, sequence INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(log_id,sequence));
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        ''')
        for k,v in DEFAULT_SETTINGS.items():
            c.execute('INSERT OR IGNORE INTO settings(key,value) VALUES (?,?)',(k,json.dumps(v,ensure_ascii=False)))
        migrations={
            'users': {'unlimited_balance':'INTEGER NOT NULL DEFAULT 0','all_models':'INTEGER NOT NULL DEFAULT 0','banned':'INTEGER NOT NULL DEFAULT 0'},
            'api_keys': {'encrypted_key':'TEXT'},
            'redemption_codes': {'encrypted_code':'TEXT'},
            'request_logs': {'payload_preview':'TEXT','route_trace':'TEXT','selected_channel_name':'TEXT','cache_read_tokens':'INTEGER','cache_write_tokens':'INTEGER'},
            'channels': {'use_proxy':'INTEGER NOT NULL DEFAULT 0','proxy_url':"TEXT NOT NULL DEFAULT ''"},
            'models': {'billing_mode':"TEXT NOT NULL DEFAULT 'token'",'cache_read_price':'REAL','cache_write_price':'REAL'},
        }
        for table,columns in migrations.items():
            existing={r['name'] for r in c.execute(f'PRAGMA table_info({table})')}
            for name,definition in columns.items():
                if name not in existing:
                    c.execute(f'ALTER TABLE {table} ADD COLUMN {name} {definition}')
                    # Older models charged unit_price on top of token prices; keep pure per-call models per-call.
                    if (table,name)==('models','billing_mode'):
                        c.execute("UPDATE models SET billing_mode='request' WHERE unit_price>0 AND input_price=0 AND output_price=0")
        c.execute('''UPDATE upstream_models SET kind=(SELECT format FROM channels WHERE channels.id=upstream_models.channel_id)
                     WHERE channel_id IN (SELECT id FROM channels WHERE format IN ('image','tts','transcription'))
                     AND kind!=(SELECT format FROM channels WHERE channels.id=upstream_models.channel_id)''')
        c.execute('''UPDATE upstream_models SET kind='language'
                     WHERE channel_id IN (SELECT id FROM channels WHERE format='anthropic') AND kind!='language' ''')
        c.execute('''UPDATE upstream_models SET kind='language'
                     WHERE channel_id IN (SELECT id FROM channels WHERE format IN ('chat','responses')) AND kind!='language' ''')
        if not c.execute('SELECT 1 FROM users WHERE is_admin=1').fetchone():
            c.execute('INSERT INTO users(username,password_hash,is_admin,balance,created_at) VALUES (?,?,?,?,?)',(conf['admin_username'],hash_password(conf['admin_password']),1,0,int(time.time())))
    os.chmod(DB_PATH,0o600)

def hash_password(password):
    salt=secrets.token_bytes(16)
    digest=hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 260000)
    return 'pbkdf2$'+base64.b64encode(salt).decode()+'$'+base64.b64encode(digest).decode()

def verify_password(password, encoded):
    try:
        _,salt,digest=encoded.split('$')
        expected=base64.b64decode(digest)
        actual=hashlib.pbkdf2_hmac('sha256',password.encode(),base64.b64decode(salt),260000)
        return hmac.compare_digest(actual,expected)
    except Exception: return False

def token_hash(token): return hashlib.sha256(token.encode()).hexdigest()
def make_token(): return secrets.token_urlsafe(40)

def _secret_box():
    return Fernet(json.loads(CONFIG_PATH.read_text())['secret_storage_key'].encode())

def encrypt_secret(value): return _secret_box().encrypt(value.encode()).decode()
def decrypt_secret(value): return _secret_box().decrypt(value.encode()).decode() if value else None

def setting(c,key):
    row=c.execute('SELECT value FROM settings WHERE key=?',(key,)).fetchone()
    return json.loads(row['value']) if row else DEFAULT_SETTINGS.get(key)

def settings(c): return {r['key']:json.loads(r['value']) for r in c.execute('SELECT key,value FROM settings')}

class BinaryLog:
    def __init__(self,file,size): self.file=file; self.size=size

def iter_log_json(value):
    if isinstance(value,BinaryLog):
        yield '"'
        value.file.seek(0); remainder=b''
        while chunk:=value.file.read(196608):
            block=remainder+chunk; whole=len(block)//3*3
            if whole: yield base64.b64encode(block[:whole]).decode()
            remainder=block[whole:]
        if remainder: yield base64.b64encode(remainder).decode()
        yield '"'
    elif isinstance(value,dict):
        yield '{'
        for index,(key,item) in enumerate(value.items()):
            if index: yield ','
            yield json.dumps(str(key),ensure_ascii=False)+':'
            yield from iter_log_json(item)
        yield '}'
    elif isinstance(value,(list,tuple)):
        yield '['
        for index,item in enumerate(value):
            if index: yield ','
            yield from iter_log_json(item)
        yield ']'
    else:
        yield from json.JSONEncoder(ensure_ascii=False,default=str).iterencode(value)

def log_preview(value, limit=1200):
    if isinstance(value,BinaryLog): return f'[Base64 二进制数据 {value.size} 字节，完整内容请下载日志]'
    if isinstance(value,str): return value[:limit]+(f'… [完整长度 {len(value)} 字符，请下载日志]' if len(value)>limit else '')
    if isinstance(value,bytes): return f'[二进制数据 {len(value)} 字节]'
    if isinstance(value,list):
        result=[log_preview(x,limit) for x in value[:80]]
        if len(value)>80: result.append(f'… 共 {len(value)} 项')
        return result
    if isinstance(value,dict): return {k:log_preview(v,limit) for k,v in value.items()}
    return value

def log_request(info,stream_file=None):
    payload=info.pop('payload',{})
    preview=json.dumps(log_preview(payload),ensure_ascii=False,default=str)
    with tempfile.TemporaryFile(dir=DATA) as compressed:
        with gzip.GzipFile(fileobj=compressed,mode='wb',compresslevel=6) as zipped:
            for piece in iter_log_json(payload):
                zipped.write(piece.encode())
        size=compressed.tell(); compressed.seek(0)
        cols=list(info)
        with conn() as c:
            cur=c.execute(f"INSERT INTO request_logs({','.join(cols)},payload_preview,payload) VALUES ({','.join('?' for _ in cols)},?,zeroblob(?))",list(info.values())+[preview,size])
            log_id=cur.lastrowid
            with c.blobopen('request_logs','payload',log_id) as blob:
                while piece:=compressed.read(262144): blob.write(piece)
            if stream_file is not None:
                stream_file.seek(0)
                for sequence,piece in enumerate(iter(lambda:stream_file.read(262144),b'')):
                    c.execute('INSERT INTO request_log_stream_chunks(log_id,sequence,payload) VALUES (?,?,?)',(log_id,sequence,gzip.compress(piece,compresslevel=6)))
    return log_id

def compact(value, limit=240):
    text=json.dumps(value,ensure_ascii=False,default=str) if not isinstance(value,str) else value
    return text[:limit] + ('…' if len(text)>limit else '')
