import asyncio, base64, codecs, gzip, hashlib, json, math, os, random, re, secrets, sqlite3, tempfile, time, zlib
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urljoin
import httpx
from fastapi import FastAPI, Request, Response, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from pydantic import BaseModel
from db import ROOT, DATA, conn, initialize, hash_password, verify_password, token_hash, make_token, setting, settings, log_request, log_preview, compact, encrypt_secret, decrypt_secret, BinaryLog
from conversion import ConversionError, convert_request, convert_response, usage_numbers
from streaming import UpstreamEvents, DownstreamEvents, parse_sse_buffer, has_convertible_output, rewrite_sse_models, downstream_stream_error

@asynccontextmanager
async def lifespan(app):
    initialize()
    yield
app=FastAPI(title='模型 API 网关',docs_url=None,redoc_url=None,lifespan=lifespan)

HOP={'host','connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailer','transfer-encoding','upgrade','content-length','cookie'}
RESPONSE_HOP=HOP|{'content-encoding','set-cookie'}
FORM_PATHS={'/v1/audio/transcriptions','/v1/audio/translations','/v1/images/edits'}
PATH_KINDS={'/v1/chat/completions':'language','/v1/responses':'language','/v1/messages':'language','/v1/images/generations':'image','/v1/images/edits':'image','/v1/audio/speech':'tts','/v1/audio/transcriptions':'transcription','/v1/audio/translations':'transcription'}
PATH_FORMATS={'/v1/chat/completions':'chat','/v1/responses':'responses','/v1/messages':'anthropic'}
CHANNEL_FORMATS=('chat','responses','anthropic','image','tts','transcription')
MEDIA_CHANNEL_FORMATS={'image','tts','transcription'}
LANGUAGE_CAPABILITIES=('reasoning','vision','image_input','tools')
LOGIN_FAILURES={}
MAX_JSON_BYTES=24_000_000
MAX_FORM_BYTES=128_000_000
MAX_JSON_RESPONSE_BYTES=32_000_000
DEFAULT_PROXY_URL='http://127.0.0.1:7890'
MODEL_KINDS=('language','tts','transcription','image')
BILLING_MODES=('token','request')
PRICE_FIELDS=('input_price','output_price','unit_price')
OPTIONAL_PRICE_FIELDS=('cache_read_price','cache_write_price')

def channel_supports_kind(channel_format,kind):
    if channel_format=='anthropic': return kind=='language'
    if channel_format in MEDIA_CHANNEL_FORMATS: return kind==channel_format
    # Chat/Responses channels carry language models only; media models need their dedicated channel format.
    return channel_format in ('chat','responses') and kind=='language'

def effective_model_kind(channel_format,stored_kind):
    if channel_format in MEDIA_CHANNEL_FORMATS: return channel_format
    if channel_format=='anthropic': return 'language'
    return stored_kind

def row(r): return dict(r) if r else None
def jload(x,default=None):
    try: return json.loads(x)
    except Exception: return default

def normalized_capabilities(kind,value):
    if kind!='language': return {}
    if isinstance(value,str): value=jload(value,{})
    caps=dict(value) if isinstance(value,dict) else {}
    if kind=='language':
        for key in LANGUAGE_CAPABILITIES:
            if caps.get(key) is None: caps[key]=True
    return caps

def route_kind_matches(c,channel_id,upstream_id,kind):
    upstream=c.execute('SELECT kind FROM upstream_models WHERE channel_id=? AND model_id=?',(channel_id,upstream_id)).fetchone()
    return upstream is None or upstream['kind']==kind

def remap_model_permissions(c,old_id,new_id=None):
    for table in ('users','api_keys'):
        for item in c.execute(f'SELECT id,allowed_models FROM {table}').fetchall():
            allowed=jload(item['allowed_models'],[])
            if not isinstance(allowed,list) or old_id not in allowed: continue
            updated=list(dict.fromkeys(new_id if value==old_id else value for value in allowed if new_id is not None or value!=old_id))
            c.execute(f'UPDATE {table} SET allowed_models=? WHERE id=?',(json.dumps(updated,ensure_ascii=False),item['id']))

def valid_price(value):
    try: price=float(value)
    except (TypeError,ValueError): raise HTTPException(400,'价格无效')
    if not math.isfinite(price) or price<0: raise HTTPException(400,'价格必须是非负有限数')
    return price

def optional_price(value):
    # Empty cache prices fall back to the input price.
    return None if value is None or value=='' else valid_price(value)

def model_is_free(model):
    if model.get('billing_mode')=='request': return not model['unit_price']
    return not any(model[k] for k in ('input_price','output_price','cache_read_price','cache_write_price') if model.get(k) is not None)

def usage_cost(model,usage):
    if model.get('billing_mode')=='request': return round(model['unit_price'] or 0,8)
    read_price=model['input_price'] if model.get('cache_read_price') is None else model['cache_read_price']
    write_price=model['input_price'] if model.get('cache_write_price') is None else model['cache_write_price']
    uncached=max(0,usage['input']-usage['cache_read']-usage['cache_write'])
    return round((uncached*model['input_price']+usage['cache_read']*read_price+usage['cache_write']*write_price+usage['output']*model['output_price'])/1_000_000,8)

def valid_proxy_url(value):
    value=(value or '').strip()
    if value and not re.fullmatch(r'https?://[^\s/?#]+/?',value): raise HTTPException(400,'代理地址需为 http://主机:端口')
    return value.rstrip('/')

def channel_proxy(channel):
    return (channel.get('proxy_url') or DEFAULT_PROXY_URL) if channel.get('use_proxy') else None

def api_error(status,message,kind='invalid_request_error'):
    return JSONResponse({'error':{'message':message,'type':kind,'code':str(status)}},status_code=status)

def now(): return int(time.time())

async def response_chunks_with_deadline(response,deadline):
    iterator=response.aiter_bytes()
    while True:
        remaining=deadline-time.monotonic()
        if remaining<=0: raise TimeoutError('总超时')
        try: yield await asyncio.wait_for(iterator.__anext__(),timeout=remaining)
        except StopAsyncIteration: return

def user_from_session(request):
    token=request.cookies.get('xf_session')
    if not token: raise HTTPException(401,'请先登录')
    with conn() as c:
        r=c.execute('SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>?',(token_hash(token),now())).fetchone()
    if not r: raise HTTPException(401,'登录已过期')
    if r['banned']: raise HTTPException(403,'账户已被封禁')
    return row(r)

def admin(request):
    u=user_from_session(request)
    if not u['is_admin']: raise HTTPException(403,'仅管理员可操作')
    return u

def origin_guard(request):
    if request.method not in ('GET','HEAD'):
        origin=request.headers.get('origin')
        host=request.headers.get('host','')
        if origin and not origin.endswith('://'+host): raise HTTPException(403,'请求来源无效')

def downstream_user(request):
    bearer=request.headers.get('authorization','')
    key=bearer[7:] if bearer.lower().startswith('bearer ') else request.headers.get('x-api-key','')
    if not key: raise HTTPException(401,'缺少 API Key')
    with conn() as c:
        r=c.execute('SELECT k.*,u.username,u.is_admin,u.balance,u.all_models,u.unlimited_balance,u.banned,u.allowed_models AS user_allowed FROM api_keys k JOIN users u ON u.id=k.user_id WHERE k.key_hash=? AND k.disabled=0 AND u.banned=0',(token_hash(key),)).fetchone()
        if r: c.execute('UPDATE api_keys SET last_used_at=? WHERE id=?',(now(),r['id']))
    if not r: raise HTTPException(401,'API Key 无效')
    return row(r)

def permitted(user,model_id):
    allowed=jload(user.get('user_allowed',user.get('allowed_models','[]')),[])
    if not (user['is_admin'] or user['all_models'] or model_id in allowed): return False
    if 'user_allowed' in user:
        key_allowed=jload(user['allowed_models'],[])
        if key_allowed and model_id not in key_allowed: return False
    return True

def public_model(c,model_id,kind):
    r=c.execute('SELECT * FROM models WHERE public_id=? AND kind=? AND enabled=1',(model_id,kind)).fetchone()
    return row(r)

def get_routes(c,model,mode):
    rows=[row(r) for r in c.execute('SELECT r.*,ch.name AS channel_name,ch.format,ch.base_url,ch.api_key,ch.extra_headers,ch.use_proxy,ch.proxy_url FROM routes r JOIN channels ch ON ch.id=r.channel_id WHERE r.model_id=? AND r.enabled=1 AND ch.enabled=1 ORDER BY r.priority,r.id',(model['id'],))]
    if mode=='balance': random.shuffle(rows)
    return rows

def replace_json_model(raw,model):
    """Replace only the top-level "model" value in raw JSON bytes, leaving every other byte untouched."""
    text=raw.decode('utf-8'); dec=json.JSONDecoder(); ws=' \t\r\n'
    i=len(text)-len(text.lstrip(ws))
    if text[i:i+1]!='{': raise ValueError('请求体必须为 JSON 对象')
    i+=1
    while True:
        while text[i] in ws or text[i]==',': i+=1
        if text[i]=='}': break
        key,i=json.decoder.scanstring(text,i+1)
        while text[i] in ws or text[i]==':': i+=1
        start=i; value,i=dec.raw_decode(text,i)
        if key=='model':
            if value==model: return raw
            return (text[:start]+json.dumps(model,ensure_ascii=False)+text[i:]).encode('utf-8')
    return raw

def header_forward(request,channel,source,target):
    h={}
    for k,v in request.headers.items():
        kl=k.lower()
        if kl in HOP or kl in ('authorization','x-api-key') or kl.startswith('x-forwarded-'): continue
        if source!=target and kl in ('anthropic-version','anthropic-beta','openai-organization','openai-project'): continue
        h[k]=v
    if target=='anthropic':
        h['x-api-key']=channel['api_key']
        if source!=target: h.setdefault('anthropic-version','2023-06-01')
    else: h['authorization']='Bearer '+channel['api_key']
    for k,v in jload(channel['extra_headers'],{}).items(): h[k]=v
    return h

def response_headers(response):
    return {k:v for k,v in response.headers.items() if k.lower() not in RESPONSE_HOP and not k.lower().startswith('access-control-') and 'model' not in k.lower() and 'upstream' not in k.lower()}

def upstream_url(channel,path):
    base=channel['base_url'].rstrip('/')
    # Both https://host and https://host/v1 accepted.
    if base.endswith('/v1'): base=base[:-3]
    return base+path

def path_for(target,source,path):
    if path in FORM_PATHS or path.startswith('/v1/images/') or path=='/v1/audio/speech': return path
    return {'chat':'/v1/chat/completions','responses':'/v1/responses','anthropic':'/v1/messages'}[target]

def try_charge(user,model,usage):
    usage={k:int(usage.get(k,0) or 0) for k in ('input','output','cache_read','cache_write')}
    cost=usage_cost(model,usage)
    if cost and not (user['is_admin'] or user['unlimited_balance']):
        with conn() as c: c.execute('UPDATE users SET balance=balance-? WHERE id=?',(cost,user['user_id']))
    return usage,cost

def redact_headers(headers): return {k:('[REDACTED]' if k.lower() in ('authorization','x-api-key','cookie','set-cookie') else v) for k,v in headers.items()}

def save_log(start,user,model,path,status,route,attempts,payload,usage,summary,stream_file=None):
    # Upstream model IDs go only into route_trace (admin-visible), never into the user-downloadable payload.
    route_models=payload.pop('_route_models',{}); trace=[{**a,'upstream_model':route_models.get(a.get('route_id'))} for a in payload.get('attempts',[])]
    used,cost=try_charge(user,model,usage) if status==200 else ({'input':0,'output':0,'cache_read':0,'cache_write':0},0)
    try:
        log_request({'created_at':int(start),'user_id':user['user_id'],'key_id':user['id'],'model_id':model['public_id'],'endpoint':path,'status':status,'route_id':route['id'] if route else None,'channel_id':route['channel_id'] if route else None,'duration_ms':int((time.time()-start)*1000),'input_tokens':used['input'],'output_tokens':used['output'],'cache_read_tokens':used['cache_read'],'cache_write_tokens':used['cache_write'],'cost':cost,'attempt_count':attempts,'summary':summary,'route_trace':json.dumps(trace,ensure_ascii=False),'selected_channel_name':route['channel_name'] if route else None,'payload':payload},stream_file)
    except Exception as e: print('log error',repr(e),flush=True)

@app.get('/health')
async def health(): return {'ok':True,'test_instance':True} if os.environ.get('NEWER_API_TEST_INSTANCE')=='1' else {'ok':True}
@app.get('/')
async def home(): return FileResponse(ROOT/'static'/'index.html',headers={'Cache-Control':'no-store'})
@app.get('/static/{name}')
async def static(name:str):
    if name not in ('app.js','style.css'): raise HTTPException(404)
    return FileResponse(ROOT/'static'/name,headers={'Cache-Control':'no-store'})
@app.get('/media/{name}')
async def media(name:str):
    if not re.fullmatch(r'[0-9a-f]{32}\.(png|jpg|webp|gif)',name): raise HTTPException(404)
    path=DATA/'assets'/name
    if not path.is_file(): raise HTTPException(404)
    return FileResponse(path,headers={'Cache-Control':'public, max-age=31536000, immutable'})
@app.post('/api/admin/assets')
async def upload_asset(request:Request,file:UploadFile=File(...)):
    origin_guard(request); admin(request)
    content=await file.read(5_000_001)
    if len(content)>5_000_000: raise HTTPException(413,'图片不能超过 5 MB')
    if content.startswith(b'\x89PNG\r\n\x1a\n'): ext='png'
    elif content.startswith(b'\xff\xd8\xff'): ext='jpg'
    elif content.startswith(b'RIFF') and content[8:12]==b'WEBP': ext='webp'
    elif content.startswith((b'GIF87a',b'GIF89a')): ext='gif'
    else: raise HTTPException(400,'仅支持 PNG、JPG、WebP、GIF 图片')
    folder=DATA/'assets'; folder.mkdir(parents=True,exist_ok=True)
    name=secrets.token_hex(16)+'.'+ext; path=folder/name; path.write_bytes(content); os.chmod(path,0o600)
    return {'url':'/media/'+name}

@app.post('/api/login')
async def login(request:Request):
    origin_guard(request); data=await request.json()
    if not isinstance(data,dict) or not isinstance(data.get('username',''),str) or not isinstance(data.get('password',''),str):
        raise HTTPException(400,'登录信息无效')
    client_ip=request.client.host if request.client else 'unknown'
    current=time.monotonic()
    failures=[moment for moment in LOGIN_FAILURES.get(client_ip,[]) if current-moment<900]
    if len(failures)>=10: raise HTTPException(429,'登录尝试过于频繁，请稍后再试')
    with conn() as c: u=c.execute('SELECT * FROM users WHERE username=?',(data.get('username',''),)).fetchone()
    if not u or not verify_password(data.get('password',''),u['password_hash']):
        failures.append(current); LOGIN_FAILURES[client_ip]=failures
        if len(LOGIN_FAILURES)>4096: LOGIN_FAILURES.pop(next(iter(LOGIN_FAILURES)))
        raise HTTPException(401,'用户名或密码错误')
    LOGIN_FAILURES.pop(client_ip,None)
    if u['banned']: raise HTTPException(403,'账户已被封禁')
    tok=make_token()
    with conn() as c: c.execute('INSERT INTO sessions(token_hash,user_id,expires_at) VALUES (?,?,?)',(token_hash(tok),u['id'],now()+30*86400))
    resp=JSONResponse({'ok':True,'user':{'id':u['id'],'username':u['username'],'is_admin':bool(u['is_admin'])}})
    resp.set_cookie('xf_session',tok,httponly=True,secure=request.headers.get('x-forwarded-proto')=='https',samesite='lax',max_age=30*86400,path='/')
    return resp
@app.post('/api/logout')
async def logout(request:Request):
    origin_guard(request); tok=request.cookies.get('xf_session')
    if tok:
        with conn() as c: c.execute('DELETE FROM sessions WHERE token_hash=?',(token_hash(tok),))
    resp=JSONResponse({'ok':True}); resp.delete_cookie('xf_session'); return resp
@app.post('/api/register')
async def register(request:Request):
    origin_guard(request); data=await request.json()
    with conn() as c:
        if not setting(c,'registration_open'): raise HTTPException(403,'注册暂未开放')
        username=str(data.get('username','')).strip(); password=str(data.get('password',''))
        if not re.fullmatch(r'[A-Za-z0-9_]{3,32}',username) or not password: raise HTTPException(400,'用户名需 3-32 位字母数字下划线，密码不能为空')
        try: c.execute('INSERT INTO users(username,password_hash,created_at) VALUES (?,?,?)',(username,hash_password(password),now()))
        except Exception: raise HTTPException(409,'用户名已存在')
    return {'ok':True}
@app.get('/api/public')
async def public():
    with conn() as c: s=settings(c)
    return {k:s[k] for k in ('site_name','tagline','currency_name','registration_open','welcome_text','footer_text','site_logo','login_image','hero_image','hero_overlay_opacity','favicon','theme','custom_css','copy','text_overrides')}
@app.get('/api/announcements')
async def announcements(request:Request):
    user_from_session(request)
    with conn() as c: return [row(r) for r in c.execute('SELECT id,title,body,image_url,created_at,updated_at FROM announcements WHERE published=1 ORDER BY created_at DESC,id DESC')]
@app.get('/api/admin/announcements')
async def admin_announcements(request:Request):
    admin(request)
    with conn() as c: return [row(r) for r in c.execute('SELECT * FROM announcements ORDER BY created_at DESC,id DESC')]
def announcement_data(d):
    if not isinstance(d,dict): raise HTTPException(400,'公告内容无效')
    title=d.get('title',''); body=d.get('body',''); image=d.get('image_url','')
    if not isinstance(title,str) or not 1<=len(title.strip())<=120: raise HTTPException(400,'公告标题需为 1 至 120 字')
    if not isinstance(body,str) or not 1<=len(body.strip())<=10000: raise HTTPException(400,'公告正文需为 1 至 10000 字')
    if not isinstance(image,str) or (image and not re.fullmatch(r'/media/[0-9a-f]{32}\.(png|jpg|webp|gif)',image)): raise HTTPException(400,'公告图片必须是已上传素材')
    if image and not (DATA/'assets'/image.rsplit('/',1)[-1]).is_file(): raise HTTPException(400,'公告图片不存在')
    if type(d.get('published',True)) is not bool: raise HTTPException(400,'发布状态无效')
    return title.strip(),body.strip(),image,int(d.get('published',True))
@app.post('/api/admin/announcements')
async def create_announcement(request:Request):
    origin_guard(request); admin(request); title,body,image,published=announcement_data(await request.json())
    with conn() as c: cur=c.execute('INSERT INTO announcements(title,body,image_url,published,created_at,updated_at) VALUES (?,?,?,?,?,?)',(title,body,image,published,now(),now()))
    return {'id':cur.lastrowid}
@app.put('/api/admin/announcements/{id}')
async def update_announcement(id:int,request:Request):
    origin_guard(request); admin(request); title,body,image,published=announcement_data(await request.json())
    with conn() as c:
        result=c.execute('UPDATE announcements SET title=?,body=?,image_url=?,published=?,updated_at=? WHERE id=?',(title,body,image,published,now(),id))
        if not result.rowcount: raise HTTPException(404,'公告不存在')
    return {'ok':True}
@app.delete('/api/admin/announcements/{id}')
async def delete_announcement(id:int,request:Request):
    origin_guard(request); admin(request)
    with conn() as c:
        result=c.execute('DELETE FROM announcements WHERE id=?',(id,))
        if not result.rowcount: raise HTTPException(404,'公告不存在')
    return {'ok':True}
@app.get('/api/me')
async def me(request:Request):
    u=user_from_session(request)
    return {'id':u['id'],'username':u['username'],'is_admin':bool(u['is_admin']),'balance':u['balance'],'unlimited_balance':bool(u['unlimited_balance'] or u['is_admin']),'all_models':bool(u['all_models'] or u['is_admin']),'allowed_models':jload(u['allowed_models'],[])}
@app.post('/api/me/password')
async def change_password(request:Request):
    origin_guard(request); u=user_from_session(request); d=await request.json()
    if not verify_password(d.get('old_password',''),u['password_hash']): raise HTTPException(400,'原密码错误')
    if not isinstance(d.get('new_password'),str) or not d['new_password']: raise HTTPException(400,'新密码不能为空')
    with conn() as c: c.execute('UPDATE users SET password_hash=? WHERE id=?',(hash_password(d['new_password']),u['id']))
    return {'ok':True}
@app.get('/api/me/models')
async def my_models(request:Request):
    u=user_from_session(request)
    with conn() as c: rows=[row(r) for r in c.execute('SELECT * FROM models WHERE enabled=1 ORDER BY kind,public_id')]
    return [{**r,'capabilities':normalized_capabilities(r['kind'],r['capabilities'])} for r in rows if permitted(u,r['public_id'])]
@app.get('/api/me/keys')
async def keys(request:Request,summary:bool=False):
    u=user_from_session(request)
    columns='id,name,prefix,allowed_models,created_at,last_used_at,disabled' + ('' if summary else ',encrypted_key')
    with conn() as c: rows=[row(r) for r in c.execute(f'SELECT {columns} FROM api_keys WHERE user_id=? ORDER BY id DESC',(u['id'],))]
    for r in rows:
        r['allowed_models']=jload(r['allowed_models'],[])
        if not summary: r['key']=decrypt_secret(r.pop('encrypted_key'))
    return JSONResponse(rows,headers={'Cache-Control':'no-store'})
@app.post('/api/me/keys')
async def create_key(request:Request):
    origin_guard(request); u=user_from_session(request); d=await request.json()
    allowed=d.get('allowed_models') or []
    if not isinstance(allowed,list) or any(not permitted(u,m) for m in allowed): raise HTTPException(400,'包含不可用模型')
    key='sk-xf-'+secrets.token_urlsafe(32)
    with conn() as c:
        cur=c.execute('INSERT INTO api_keys(user_id,name,prefix,key_hash,encrypted_key,allowed_models,created_at) VALUES (?,?,?,?,?,?,?)',(u['id'],str(d.get('name','我的密钥'))[:80],key[:14],token_hash(key),encrypt_secret(key),json.dumps(allowed),now()))
    return {'id':cur.lastrowid,'key':key}
@app.put('/api/me/keys/{key_id}')
async def update_key(key_id:int,request:Request):
    origin_guard(request); u=user_from_session(request); d=await request.json()
    allowed=d.get('allowed_models',[])
    if not isinstance(allowed,list) or any(not isinstance(m,str) or not permitted(u,m) for m in allowed): raise HTTPException(400,'包含不可用模型')
    name=str(d.get('name','')).strip()
    if not name or len(name)>80: raise HTTPException(400,'密钥名称需为 1 至 80 个字符')
    with conn() as c:
        result=c.execute('UPDATE api_keys SET name=?,allowed_models=? WHERE id=? AND user_id=?',(name,json.dumps(allowed),key_id,u['id']))
        if not result.rowcount: raise HTTPException(404,'密钥不存在')
    return {'ok':True}
@app.delete('/api/me/keys/{key_id}')
async def delete_key(key_id:int,request:Request):
    origin_guard(request); u=user_from_session(request)
    with conn() as c: c.execute('DELETE FROM api_keys WHERE id=? AND user_id=?',(key_id,u['id']))
    return {'ok':True}
@app.post('/api/me/redeem')
async def redeem(request:Request):
    origin_guard(request); u=user_from_session(request); d=await request.json(); code=str(d.get('code','')).strip().upper()
    with conn() as c:
        r=c.execute('SELECT * FROM redemption_codes WHERE code_hash=?',(token_hash(code),)).fetchone()
        if not r or r['uses']>=r['max_uses'] or (r['expires_at'] and r['expires_at']<now()): raise HTTPException(400,'兑换码无效或已用完')
        if c.execute('SELECT 1 FROM redemptions WHERE code_id=? AND user_id=?',(r['id'],u['id'])).fetchone(): raise HTTPException(400,'你已兑换过此码')
        c.execute('INSERT INTO redemptions(code_id,user_id,created_at) VALUES (?,?,?)',(r['id'],u['id'],now()))
        c.execute('UPDATE redemption_codes SET uses=uses+1 WHERE id=?',(r['id'],))
        c.execute('UPDATE users SET balance=balance+? WHERE id=?',(r['amount'],u['id']))
    return {'ok':True,'amount':r['amount']}

@app.get('/api/admin/settings')
async def admin_settings(request:Request):
    admin(request)
    with conn() as c: return settings(c)
@app.put('/api/admin/settings')
async def save_settings(request:Request):
    origin_guard(request); admin(request); d=await request.json()
    from db import DEFAULT_SETTINGS
    with conn() as c:
        for k,v in d.items():
            if k not in DEFAULT_SETTINGS: continue
            if k in ('retry_statuses',) and (not isinstance(v,list) or any(not isinstance(x,int) or x<100 or x>599 for x in v)): raise HTTPException(400,'重试状态码无效')
            if k=='retry_text' and (not isinstance(v,list) or any(not isinstance(x,str) for x in v)): raise HTTPException(400,'错误文本规则无效')
            if k=='max_attempts_per_route' and (type(v) is not int or not 1<=v<=3): raise HTTPException(400,'每条路由尝试次数应为 1 到 3')
            if k.endswith('timeout') and (not isinstance(v,(int,float)) or v<=0 or v>3600): raise HTTPException(400,'超时设置无效')
            if k in ('site_logo','login_image','hero_image','favicon') and v and not re.fullmatch(r'/media/[0-9a-f]{32}\.(png|jpg|webp|gif)',str(v)): raise HTTPException(400,'图片地址必须来自素材上传')
            if k=='custom_css' and (not isinstance(v,str) or len(v)>100000): raise HTTPException(400,'自定义样式过长')
            if k=='hero_overlay_opacity' and (not isinstance(v,(int,float)) or isinstance(v,bool) or not 0<=v<=1): raise HTTPException(400,'横幅透明度应在 0 到 1 之间')
            if k in ('theme','copy','text_overrides') and (not isinstance(v,dict) or len(json.dumps(v))>30000): raise HTTPException(400,'自定义配置无效')
            c.execute('INSERT INTO settings(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(k,json.dumps(v,ensure_ascii=False)))
    return {'ok':True}
@app.get('/api/admin/channels')
async def channels(request:Request):
    admin(request)
    with conn() as c: rows=[row(r) for r in c.execute('SELECT id,name,format,base_url,extra_headers,enabled,use_proxy,proxy_url,created_at,CASE WHEN api_key="" THEN 0 ELSE 1 END AS has_key FROM channels ORDER BY id DESC')]
    for r in rows: r['extra_headers']=jload(r['extra_headers'],{})
    return rows
@app.post('/api/admin/channels')
async def add_channel(request:Request):
    origin_guard(request); admin(request); d=await request.json()
    if d.get('format') not in CHANNEL_FORMATS: raise HTTPException(400,'渠道格式无效')
    if not isinstance(d.get('base_url'),str) or not re.match(r'^https?://',d['base_url']): raise HTTPException(400,'渠道地址无效')
    if not isinstance(d.get('extra_headers',{}),dict): raise HTTPException(400,'额外请求头无效')
    proxy_url=valid_proxy_url(d.get('proxy_url',''))
    with conn() as c:
        cur=c.execute('INSERT INTO channels(name,format,base_url,api_key,extra_headers,enabled,use_proxy,proxy_url,created_at) VALUES (?,?,?,?,?,?,?,?,?)',(d.get('name','新渠道'),d['format'],d['base_url'].rstrip('/'),d.get('api_key',''),json.dumps(d.get('extra_headers',{})),int(d.get('enabled',True)),int(bool(d.get('use_proxy',False))),proxy_url,now()))
    return {'id':cur.lastrowid}
@app.put('/api/admin/channels/{id}')
async def update_channel(id:int,request:Request):
    origin_guard(request); admin(request); d=await request.json()
    if 'format' in d and d['format'] not in CHANNEL_FORMATS: raise HTTPException(400,'渠道格式无效')
    if 'base_url' in d and (not isinstance(d['base_url'],str) or not re.match(r'^https?://',d['base_url'])): raise HTTPException(400,'渠道地址无效')
    if 'extra_headers' in d and not isinstance(d['extra_headers'],dict): raise HTTPException(400,'额外请求头无效')
    if 'proxy_url' in d: d['proxy_url']=valid_proxy_url(d['proxy_url'])
    if 'use_proxy' in d: d['use_proxy']=int(bool(d['use_proxy']))
    if 'format' in d:
        with conn() as c:
            if not c.execute('SELECT 1 FROM channels WHERE id=?',(id,)).fetchone(): raise HTTPException(404,'渠道不存在')
            for route in c.execute('SELECT m.kind FROM routes r JOIN models m ON m.id=r.model_id WHERE r.channel_id=?',(id,)):
                if not channel_supports_kind(d['format'],route['kind']): raise HTTPException(400,'新渠道格式与现有模型路由不兼容')
    fields=[]; values=[]
    for k in ('name','format','base_url','api_key','extra_headers','enabled','use_proxy','proxy_url'):
        if k in d:
            if k=='api_key' and not d[k]: continue
            fields.append(k+'=?'); values.append(json.dumps(d[k]) if k=='extra_headers' else d[k])
    if fields:
        with conn() as c:
            c.execute(f'UPDATE channels SET {",".join(fields)} WHERE id=?',values+[id])
            if d.get('format') in MEDIA_CHANNEL_FORMATS or d.get('format')=='anthropic':
                c.execute('UPDATE upstream_models SET kind=? WHERE channel_id=?',(effective_model_kind(d['format'],'language'),id))
            elif d.get('format') in ('chat','responses'):
                c.execute("UPDATE upstream_models SET kind='language' WHERE channel_id=? AND kind!='language'",(id,))
    return {'ok':True}
@app.delete('/api/admin/channels/{id}')
async def delete_channel(id:int,request:Request):
    origin_guard(request); admin(request)
    with conn() as c: c.execute('DELETE FROM channels WHERE id=?',(id,))
    return {'ok':True}
@app.get('/api/admin/channels/{id}/models')
async def upstream_models(id:int,request:Request):
    admin(request)
    with conn() as c:
        channel=c.execute('SELECT format FROM channels WHERE id=?',(id,)).fetchone()
        rows=[row(r) for r in c.execute('SELECT * FROM upstream_models WHERE channel_id=? ORDER BY model_id',(id,))]
    for r in rows:
        r['kind']=effective_model_kind(channel['format'],r['kind']) if channel else r['kind']
        r['capabilities']=normalized_capabilities(r['kind'],r['capabilities'])
    return rows
@app.post('/api/admin/channels/{id}/models')
async def add_upstream_model(id:int,request:Request):
    origin_guard(request); admin(request); d=await request.json()
    kind=d.get('kind','language')
    if kind not in MODEL_KINDS: raise HTTPException(400,'上游模型类型无效')
    with conn() as c:
        channel=c.execute('SELECT format FROM channels WHERE id=?',(id,)).fetchone()
        if not channel: raise HTTPException(404,'渠道不存在')
        if not channel_supports_kind(channel['format'],kind): raise HTTPException(400,'此渠道格式不支持该模型类型')
        if not isinstance(d.get('model_id'),str) or not d['model_id'].strip(): raise HTTPException(400,'上游模型 ID 不能为空')
        if c.execute('SELECT 1 FROM routes r JOIN models m ON m.id=r.model_id WHERE r.channel_id=? AND r.upstream_model_id=? AND m.kind!=? LIMIT 1',(id,d['model_id'],kind)).fetchone():
            raise HTTPException(400,'此上游模型已有其他类型的公开模型路由')
        c.execute('INSERT INTO upstream_models(channel_id,model_id,kind,capabilities) VALUES (?,?,?,?) ON CONFLICT(channel_id,model_id) DO UPDATE SET kind=excluded.kind,capabilities=excluded.capabilities',(id,d['model_id'],kind,json.dumps(normalized_capabilities(kind,d.get('capabilities',{})))))
    return {'ok':True}
@app.post('/api/admin/channels/{id}/fetch-models')
async def fetch_models(id:int,request:Request):
    origin_guard(request); admin(request)
    with conn() as c: channel=row(c.execute('SELECT * FROM channels WHERE id=?',(id,)).fetchone())
    if not channel: raise HTTPException(404,'渠道不存在')
    path='/v1/models'; headers={'x-api-key':channel['api_key'],'anthropic-version':'2023-06-01'} if channel['format']=='anthropic' else {'authorization':'Bearer '+channel['api_key']}
    headers.update(jload(channel['extra_headers'],{}))
    try:
        pages=[]; cursor=None; seen_cursors=set()
        async with httpx.AsyncClient(timeout=25,follow_redirects=False,proxy=channel_proxy(channel)) as client:
            for _ in range(100):
                params={'limit':1000} if channel['format']=='anthropic' else {}
                if cursor: params['after_id']=cursor
                resp=await client.get(upstream_url(channel,path),headers=headers,params=params); resp.raise_for_status(); d=resp.json()
                pages.append(d)
                if not d.get('has_more'): break
                next_cursor=d.get('last_id')
                if not next_cursor or next_cursor in seen_cursors: raise ValueError('上游分页游标无效')
                seen_cursors.add(next_cursor); cursor=next_cursor
            else: raise ValueError('模型页数超过 100')
    except Exception as e: raise HTTPException(502,'拉取失败：'+str(e)[:180])
    items=[item for page in pages for item in (page.get('data',page.get('models',[])) if isinstance(page,dict) else [])]
    count=0
    with conn() as c:
        for item in items:
            mid=item.get('id') if isinstance(item,dict) else None
            if not mid: continue
            caps=item.get('capabilities') or item.get('supported_features') or {}
            if not isinstance(caps,dict): caps={'raw':caps}
            # The channel format decides the upstream model kind; Chat/Responses/Anthropic channels carry language models only.
            kind=effective_model_kind(channel['format'],'language')
            if c.execute('SELECT 1 FROM routes r JOIN models m ON m.id=r.model_id WHERE r.channel_id=? AND r.upstream_model_id=? AND m.kind!=? LIMIT 1',(id,mid,kind)).fetchone():
                continue
            c.execute('INSERT INTO upstream_models(channel_id,model_id,kind,capabilities) VALUES (?,?,?,?) ON CONFLICT(channel_id,model_id) DO UPDATE SET kind=excluded.kind,capabilities=excluded.capabilities',(id,mid,kind,json.dumps(normalized_capabilities(kind,caps),ensure_ascii=False)))
            count+=1
    return {'ok':True,'count':count}
@app.get('/api/admin/models')
async def models(request:Request):
    admin(request)
    with conn() as c:
        rows=[row(r) for r in c.execute('SELECT * FROM models ORDER BY id DESC')]
        for r in rows:
            r['capabilities']=normalized_capabilities(r['kind'],r['capabilities'])
            r['routes']=[row(q) for q in c.execute('SELECT r.*,ch.name AS channel_name,ch.format FROM routes r JOIN channels ch ON ch.id=r.channel_id WHERE r.model_id=? ORDER BY r.priority,r.id',(r['id'],))]
    return rows
@app.post('/api/admin/models')
async def add_model(request:Request):
    origin_guard(request); admin(request); d=await request.json()
    if d.get('kind','language') not in MODEL_KINDS or d.get('mode','failover') not in ('failover','balance'): raise HTTPException(400,'模型类型或模式无效')
    if d.get('billing_mode','token') not in BILLING_MODES: raise HTTPException(400,'计费方式无效')
    if not isinstance(d.get('public_id'),str) or not d['public_id'].strip(): raise HTTPException(400,'模型 ID 不能为空')
    with conn() as c:
        prices=[valid_price(d.get(k,0)) for k in PRICE_FIELDS]+[optional_price(d.get(k)) for k in OPTIONAL_PRICE_FIELDS]
        try: cur=c.execute('INSERT INTO models(public_id,kind,capabilities,input_price,output_price,unit_price,cache_read_price,cache_write_price,billing_mode,mode,enabled,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(d['public_id'],d.get('kind','language'),json.dumps(normalized_capabilities(d.get('kind','language'),d.get('capabilities',{})),ensure_ascii=False),*prices,d.get('billing_mode','token'),d.get('mode','failover'),int(d.get('enabled',True)),now()))
        except sqlite3.IntegrityError: raise HTTPException(409,'模型 ID 已存在')
    return {'id':cur.lastrowid}
@app.post('/api/admin/models/import')
async def import_models(request:Request):
    origin_guard(request); admin(request); d=await request.json()
    try: channel_id=int(d.get('channel_id',0))
    except (TypeError,ValueError): raise HTTPException(400,'渠道 ID 无效')
    ids=d.get('model_ids') or []
    if not isinstance(ids,list) or any(not isinstance(mid,str) or not mid for mid in ids): raise HTTPException(400,'模型 ID 列表无效')
    with conn() as c:
        ch=c.execute('SELECT id,format FROM channels WHERE id=?',(channel_id,)).fetchone()
        if not ch: raise HTTPException(404,'渠道不存在')
        if not ids: ids=[r['model_id'] for r in c.execute('SELECT model_id FROM upstream_models WHERE channel_id=?',(channel_id,))]
        count=0
        for mid in ids:
            up=c.execute('SELECT * FROM upstream_models WHERE channel_id=? AND model_id=?',(channel_id,mid)).fetchone()
            kind=effective_model_kind(ch['format'],up['kind'] if up else 'unclassified')
            if kind=='unclassified': continue
            caps=json.dumps(normalized_capabilities(kind,up['capabilities'] if up else '{}'),ensure_ascii=False)
            if not channel_supports_kind(ch['format'],kind): continue
            existing=c.execute('SELECT id,kind FROM models WHERE public_id=?',(mid,)).fetchone()
            if existing and existing['kind']!=kind: continue
            c.execute('INSERT OR IGNORE INTO models(public_id,kind,capabilities,created_at) VALUES (?,?,?,?)',(mid,kind,caps,now()))
            model=c.execute('SELECT id FROM models WHERE public_id=?',(mid,)).fetchone()
            c.execute('INSERT OR IGNORE INTO routes(model_id,channel_id,upstream_model_id,priority) VALUES (?,?,?,?)',(model['id'],channel_id,mid,0)); count+=1
    return {'ok':True,'count':count}
@app.put('/api/admin/models/{id}')
async def update_model(id:int,request:Request):
    origin_guard(request); admin(request); d=await request.json(); fields=[]; values=[]
    if 'mode' in d and d['mode'] not in ('failover','balance'): raise HTTPException(400,'路由模式无效')
    if 'kind' in d and d['kind'] not in MODEL_KINDS: raise HTTPException(400,'模型类型无效')
    if 'billing_mode' in d and d['billing_mode'] not in BILLING_MODES: raise HTTPException(400,'计费方式无效')
    if 'kind' in d:
        with conn() as c:
            for route in c.execute('SELECT r.channel_id,r.upstream_model_id,ch.format FROM routes r JOIN channels ch ON ch.id=r.channel_id WHERE r.model_id=?',(id,)):
                if not channel_supports_kind(route['format'],d['kind']) or not route_kind_matches(c,route['channel_id'],route['upstream_model_id'],d['kind']):
                    raise HTTPException(400,'模型类型与现有上游路由不兼容')
    capability_kind=d.get('kind')
    if 'capabilities' in d and not capability_kind:
        with conn() as c: existing=c.execute('SELECT kind FROM models WHERE id=?',(id,)).fetchone()
        capability_kind=existing['kind'] if existing else 'language'
    for k in ('public_id','kind','capabilities',*PRICE_FIELDS,*OPTIONAL_PRICE_FIELDS,'billing_mode','mode','enabled'):
        if k in d:
            fields.append(k+'=?')
            if k=='capabilities': values.append(json.dumps(normalized_capabilities(capability_kind,d[k]),ensure_ascii=False))
            elif k in PRICE_FIELDS: values.append(valid_price(d[k]))
            elif k in OPTIONAL_PRICE_FIELDS: values.append(optional_price(d[k]))
            else: values.append(d[k])
    if 'kind' in d and d['kind']!='language' and 'capabilities' not in d:
        fields.append('capabilities=?'); values.append('{}')
    if fields:
        with conn() as c:
            previous=c.execute('SELECT public_id FROM models WHERE id=?',(id,)).fetchone()
            if not previous: raise HTTPException(404,'模型不存在')
            if 'public_id' in d and (not isinstance(d['public_id'],str) or not d['public_id'].strip()): raise HTTPException(400,'模型 ID 不能为空')
            try: c.execute(f'UPDATE models SET {",".join(fields)} WHERE id=?',values+[id])
            except sqlite3.IntegrityError: raise HTTPException(409,'模型 ID 已存在')
            if 'public_id' in d and d['public_id']!=previous['public_id']:
                remap_model_permissions(c,previous['public_id'],d['public_id'])
    return {'ok':True}
@app.delete('/api/admin/models/{id}')
async def delete_model(id:int,request:Request):
    origin_guard(request); admin(request)
    with conn() as c:
        current=c.execute('SELECT public_id FROM models WHERE id=?',(id,)).fetchone()
        if current:
            c.execute('DELETE FROM models WHERE id=?',(id,))
            remap_model_permissions(c,current['public_id'])
    return {'ok':True}
@app.post('/api/admin/models/{id}/routes')
async def add_route(id:int,request:Request):
    origin_guard(request); admin(request); d=await request.json()
    with conn() as c:
        model=c.execute('SELECT kind FROM models WHERE id=?',(id,)).fetchone(); channel=c.execute('SELECT format FROM channels WHERE id=?',(d.get('channel_id'),)).fetchone()
        if not model or not channel: raise HTTPException(404,'模型或渠道不存在')
        if not channel_supports_kind(channel['format'],model['kind']): raise HTTPException(400,'该渠道格式不支持此模型类型')
        if not isinstance(d.get('upstream_model_id'),str) or not d['upstream_model_id'].strip(): raise HTTPException(400,'上游模型 ID 不能为空')
        if not route_kind_matches(c,d['channel_id'],d['upstream_model_id'],model['kind']): raise HTTPException(400,'上游模型类型与公开模型不一致')
        priority=c.execute('SELECT COALESCE(MAX(priority),-1)+1 FROM routes WHERE model_id=?',(id,)).fetchone()[0]
        try: cur=c.execute('INSERT INTO routes(model_id,channel_id,upstream_model_id,priority,enabled) VALUES (?,?,?,?,?)',(id,d['channel_id'],d['upstream_model_id'],priority,int(d.get('enabled',True))))
        except sqlite3.IntegrityError: raise HTTPException(409,'路由已存在')
    return {'id':cur.lastrowid}
@app.post('/api/admin/models/{id}/routes/import')
async def import_channel_routes(id:int,request:Request):
    origin_guard(request); admin(request); d=await request.json()
    try: channel_id=int(d.get('channel_id',0))
    except (TypeError,ValueError): raise HTTPException(400,'渠道 ID 无效')
    with conn() as c:
        model=c.execute('SELECT kind FROM models WHERE id=?',(id,)).fetchone(); channel=c.execute('SELECT format FROM channels WHERE id=?',(channel_id,)).fetchone()
        if not model or not channel: raise HTTPException(404,'模型或渠道不存在')
        if not channel_supports_kind(channel['format'],model['kind']): raise HTTPException(400,'该渠道格式不支持此模型类型')
        priority=c.execute('SELECT COALESCE(MAX(priority),-1)+1 FROM routes WHERE model_id=?',(id,)).fetchone()[0]; count=0
        for up in c.execute('SELECT model_id,kind FROM upstream_models WHERE channel_id=? ORDER BY model_id',(channel_id,)).fetchall():
            if effective_model_kind(channel['format'],up['kind'])!=model['kind']: continue
            if c.execute('INSERT OR IGNORE INTO routes(model_id,channel_id,upstream_model_id,priority) VALUES (?,?,?,?)',(id,channel_id,up['model_id'],priority)).rowcount:
                priority+=1; count+=1
    return {'ok':True,'count':count}
@app.put('/api/admin/models/{id}/routes/reorder')
async def reorder_routes(id:int,request:Request):
    origin_guard(request); admin(request); d=await request.json(); ids=d.get('route_ids')
    with conn() as c:
        current=[r['id'] for r in c.execute('SELECT id FROM routes WHERE model_id=?',(id,))]
        if not isinstance(ids,list) or len(ids)!=len(current) or set(ids)!=set(current): raise HTTPException(400,'路由列表不匹配')
        for priority,route_id in enumerate(ids): c.execute('UPDATE routes SET priority=? WHERE id=? AND model_id=?',(priority,route_id,id))
    return {'ok':True}
@app.put('/api/admin/routes/{id}')
async def update_route(id:int,request:Request):
    origin_guard(request); admin(request); d=await request.json(); fields=[]; values=[]
    for k in ('channel_id','upstream_model_id','priority','enabled'):
        if k in d: fields.append(k+'=?'); values.append(d[k])
    if fields:
        with conn() as c:
            current=c.execute('SELECT m.kind,ch.format FROM routes r JOIN models m ON m.id=r.model_id JOIN channels ch ON ch.id=r.channel_id WHERE r.id=?',(id,)).fetchone()
            if not current: raise HTTPException(404,'路由不存在')
            channel_id=int(d.get('channel_id',c.execute('SELECT channel_id FROM routes WHERE id=?',(id,)).fetchone()['channel_id']))
            channel=c.execute('SELECT format FROM channels WHERE id=?',(channel_id,)).fetchone()
            if not channel: raise HTTPException(404,'渠道不存在')
            if not channel_supports_kind(channel['format'],current['kind']): raise HTTPException(400,'该渠道格式不支持此模型类型')
            upstream_id=d.get('upstream_model_id',c.execute('SELECT upstream_model_id FROM routes WHERE id=?',(id,)).fetchone()['upstream_model_id'])
            if not isinstance(upstream_id,str) or not upstream_id.strip(): raise HTTPException(400,'上游模型 ID 不能为空')
            if not route_kind_matches(c,channel_id,upstream_id,current['kind']): raise HTTPException(400,'上游模型类型与公开模型不一致')
            try: c.execute(f'UPDATE routes SET {",".join(fields)} WHERE id=?',values+[id])
            except sqlite3.IntegrityError: raise HTTPException(409,'路由已存在')
    return {'ok':True}
@app.delete('/api/admin/routes/{id}')
async def delete_route(id:int,request:Request):
    origin_guard(request); admin(request)
    with conn() as c: c.execute('DELETE FROM routes WHERE id=?',(id,))
    return {'ok':True}
@app.get('/api/admin/users')
async def users(request:Request):
    admin(request)
    with conn() as c: rows=[row(r) for r in c.execute('SELECT id,username,is_admin,balance,unlimited_balance,all_models,banned,allowed_models,created_at FROM users ORDER BY id')]
    for r in rows:
        r['allowed_models']=jload(r['allowed_models'],[])
        r['unlimited_balance']=bool(r['unlimited_balance'] or r['is_admin'])
        r['all_models']=bool(r['all_models'] or r['is_admin'])
    return rows
def user_fields(d,create=False):
    result={}
    if create or 'balance' in d:
        try: result['balance']=float(d.get('balance',0))
        except (TypeError,ValueError): raise HTTPException(400,'余额无效')
        if not math.isfinite(result['balance']): raise HTTPException(400,'余额必须是有限数')
    if create or 'allowed_models' in d:
        allowed=d.get('allowed_models',[])
        if not isinstance(allowed,list) or any(not isinstance(x,str) for x in allowed): raise HTTPException(400,'模型列表无效')
        with conn() as c: valid={r['public_id'] for r in c.execute('SELECT public_id FROM models')}
        if any(x not in valid for x in allowed): raise HTTPException(400,'包含不存在的模型')
        result['allowed_models']=json.dumps(allowed,ensure_ascii=False)
    for k in ('all_models','unlimited_balance','banned'):
        if create or k in d: result[k]=int(bool(d.get(k,False)))
    if 'password' in d:
        if not str(d['password']): raise HTTPException(400,'密码不能为空')
        result['password_hash']=hash_password(str(d['password']))
    return result
@app.post('/api/admin/users')
async def add_user(request:Request):
    origin_guard(request); admin(request); d=await request.json()
    username=str(d.get('username','')).strip(); password=str(d.get('password',''))
    if not re.fullmatch(r'[A-Za-z0-9_]{3,32}',username) or not password: raise HTTPException(400,'用户名需 3-32 位字母数字下划线，密码不能为空')
    fields=user_fields(d,True); fields['username']=username; fields['password_hash']=hash_password(password); fields['created_at']=now()
    with conn() as c:
        try: cur=c.execute(f"INSERT INTO users({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",list(fields.values()))
        except __import__('sqlite3').IntegrityError: raise HTTPException(409,'用户名已存在')
    return {'id':cur.lastrowid}
@app.put('/api/admin/users/{id}')
async def update_user(id:int,request:Request):
    origin_guard(request); actor=admin(request); d=await request.json(); fields=user_fields(d)
    with conn() as c:
        target=c.execute('SELECT is_admin FROM users WHERE id=?',(id,)).fetchone()
        if not target: raise HTTPException(404,'用户不存在')
        if target['is_admin']:
            fields.pop('all_models',None); fields.pop('unlimited_balance',None)
            if fields.get('banned'): raise HTTPException(400,'不能封禁管理员')
        if id==actor['id'] and fields.get('banned'): raise HTTPException(400,'不能封禁自己')
        if fields: c.execute(f"UPDATE users SET {','.join(k+'=?' for k in fields)} WHERE id=?",list(fields.values())+[id])
        if 'password_hash' in fields: c.execute('DELETE FROM sessions WHERE user_id=? AND token_hash!=?',(id,token_hash(request.cookies.get('xf_session') or '')))
    return {'ok':True}
@app.delete('/api/admin/users/{id}')
async def delete_user(id:int,request:Request):
    origin_guard(request); actor=admin(request)
    if id==actor['id']: raise HTTPException(400,'不能删除自己')
    with conn() as c:
        target=c.execute('SELECT is_admin FROM users WHERE id=?',(id,)).fetchone()
        if not target: raise HTTPException(404,'用户不存在')
        if target['is_admin']: raise HTTPException(400,'不能删除管理员')
        c.execute('DELETE FROM redemptions WHERE user_id=?',(id,))
        c.execute('DELETE FROM users WHERE id=?',(id,))
    return {'ok':True}
@app.get('/api/admin/codes')
async def codes(request:Request):
    admin(request)
    with conn() as c: rows=[row(r) for r in c.execute('SELECT id,prefix,encrypted_code,amount,max_uses,uses,expires_at,created_at FROM redemption_codes ORDER BY id DESC')]
    for r in rows: r['code']=decrypt_secret(r.pop('encrypted_code'))
    return JSONResponse(rows,headers={'Cache-Control':'no-store'})
@app.post('/api/admin/codes')
async def create_codes(request:Request):
    origin_guard(request); admin(request); d=await request.json(); n=min(max(int(d.get('count',1)),1),100); amount=float(d.get('amount',0)); max_uses=int(d.get('max_uses',1)); expires=d.get('expires_at')
    if not math.isfinite(amount) or amount<=0 or max_uses<1: raise HTTPException(400,'金额和使用次数需大于零')
    results=[]
    with conn() as c:
        for _ in range(n):
            code='XF-'+secrets.token_hex(4).upper()+'-'+secrets.token_hex(4).upper()
            c.execute('INSERT INTO redemption_codes(code_hash,prefix,encrypted_code,amount,max_uses,expires_at,created_at) VALUES (?,?,?,?,?,?,?)',(token_hash(code),code[:7],encrypt_secret(code),amount,max_uses,expires,now())); results.append(code)
    return {'codes':results}
@app.delete('/api/admin/codes/{id}')
async def delete_code(id:int,request:Request):
    origin_guard(request); admin(request)
    with conn() as c:
        if c.execute('SELECT 1 FROM redemptions WHERE code_id=?',(id,)).fetchone(): raise HTTPException(409,'已有兑换记录，不能删除')
        result=c.execute('DELETE FROM redemption_codes WHERE id=?',(id,))
        if not result.rowcount: raise HTTPException(404,'兑换码不存在')
    return {'ok':True}
@app.get('/api/admin/logs')
async def logs(request:Request,limit:int=50,offset:int=0,user_id:int|None=None):
    admin(request); limit=min(max(limit,1),100); offset=max(offset,0)
    sql='SELECT l.id,l.created_at,l.user_id,u.username,l.key_id,l.model_id,l.endpoint,l.status,l.route_id,l.channel_id,l.duration_ms,l.input_tokens,l.output_tokens,l.cache_read_tokens,l.cache_write_tokens,l.cost,l.attempt_count,l.summary,l.route_trace,l.selected_channel_name FROM request_logs l LEFT JOIN users u ON u.id=l.user_id'
    params=[]
    if user_id: sql+=' WHERE l.user_id=?'; params.append(user_id)
    sql+=' ORDER BY l.id DESC LIMIT ? OFFSET ?'; params.extend([limit,offset])
    with conn() as c: rows=[row(r) for r in c.execute(sql,params)]
    for item in rows: item['route_trace']=jload(item['route_trace'],[])
    return rows
@app.get('/api/admin/logs/{id}')
async def log_detail(id:int,request:Request,download:bool=False):
    admin(request)
    return render_log_detail(id,download)

@app.get('/api/me/logs')
async def my_logs(request:Request,limit:int=50,offset:int=0):
    u=user_from_session(request); limit=min(max(limit,1),100); offset=max(offset,0)
    with conn() as c:
        rows=[row(r) for r in c.execute('SELECT l.id,l.created_at,l.user_id,u.username,l.key_id,l.model_id,l.endpoint,l.status,l.route_id,l.channel_id,l.duration_ms,l.input_tokens,l.output_tokens,l.cache_read_tokens,l.cache_write_tokens,l.cost,l.attempt_count,l.summary,l.route_trace,l.selected_channel_name FROM request_logs l LEFT JOIN users u ON u.id=l.user_id WHERE l.user_id=? ORDER BY l.id DESC LIMIT ? OFFSET ?',(u['id'],limit,offset))]
    for item in rows: item['route_trace']=user_route_trace(jload(item['route_trace'],[]))
    return rows
@app.get('/api/me/logs/{id}')
async def my_log_detail(id:int,request:Request,download:bool=False):
    u=user_from_session(request)
    return render_log_detail(id,download,u['id'])

def user_route_trace(trace): return [{k:v for k,v in a.items() if k!='upstream_model'} for a in trace]

def render_log_detail(id,download,user_id=None):
    sql='SELECT id,created_at,user_id,key_id,model_id,endpoint,status,route_id,channel_id,duration_ms,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,cost,attempt_count,summary,route_trace,selected_channel_name,payload_preview FROM request_logs WHERE id=?'
    params=[id]
    if user_id is not None: sql+=' AND user_id=?'; params.append(user_id)
    with conn() as c:
        r=c.execute(sql,params).fetchone()
        chunks=c.execute('SELECT COUNT(*) FROM request_log_stream_chunks WHERE log_id=?',(id,)).fetchone()[0] if r else 0
    if not r: raise HTTPException(404,'日志不存在')
    d=row(r); preview=d.pop('payload_preview'); d['route_trace']=jload(d['route_trace'],[])
    if user_id is not None: d['route_trace']=user_route_trace(d['route_trace'])
    if download:
        async def download_json():
            yield json.dumps(d,ensure_ascii=False,default=str).encode()[:-1]+b',"payload":'
            with conn() as c:
                with c.blobopen('request_logs','payload',id,readonly=True) as blob:
                    unzip=zlib.decompressobj(wbits=31)
                    while part:=blob.read(262144):
                        if decoded:=unzip.decompress(part): yield decoded
                        await asyncio.sleep(0)
                    if tail:=unzip.flush(): yield tail
                if chunks:
                    yield b',"response_stream":"'
                    decoder=codecs.getincrementaldecoder('utf-8')('replace')
                    for chunk in c.execute('SELECT payload FROM request_log_stream_chunks WHERE log_id=? ORDER BY sequence',(id,)):
                        text=decoder.decode(gzip.decompress(chunk['payload']))
                        if text: yield json.dumps(text,ensure_ascii=False)[1:-1].encode()
                        await asyncio.sleep(0)
                    if tail:=decoder.decode(b'',final=True): yield json.dumps(tail,ensure_ascii=False)[1:-1].encode()
                    yield b'"'
            yield b'}'
        return StreamingResponse(download_json(),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="request-{id}.json"','Cache-Control':'no-store'})
    if preview is None:
        with conn() as c: legacy=c.execute('SELECT payload FROM request_logs WHERE id=?',(id,)).fetchone()['payload']
        d['payload']=json.loads(gzip.decompress(legacy))
        d['payload']=log_preview(d['payload'])
    else: d['payload']=json.loads(preview)
    if chunks:
        with conn() as c:
            first=c.execute('SELECT payload FROM request_log_stream_chunks WHERE log_id=? ORDER BY sequence LIMIT 1',(id,)).fetchone()
        d['response_stream_preview']=gzip.decompress(first['payload'])[:3000].decode('utf-8','replace')+'… [完整流请下载 JSON]'
    return d

@app.get('/v1/models')
async def list_public_models(request:Request):
    u=downstream_user(request)
    with conn() as c: rows=[row(r) for r in c.execute('SELECT public_id,created_at,kind,capabilities FROM models WHERE enabled=1 ORDER BY public_id')]
    rows=[r for r in rows if permitted(u,r['public_id'])]
    if request.headers.get('anthropic-version'):
        return {'data':[{'id':r['public_id'],'type':'model','display_name':r['public_id'],'created_at':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(r['created_at'])),'capabilities':normalized_capabilities(r['kind'],r['capabilities'])} for r in rows],'first_id':rows[0]['public_id'] if rows else None,'last_id':rows[-1]['public_id'] if rows else None,'has_more':False}
    return {'object':'list','data':[{'id':r['public_id'],'object':'model','created':r['created_at'],'owned_by':'xiaofei','capabilities':normalized_capabilities(r['kind'],r['capabilities']),'kind':r['kind']} for r in rows]}

@app.post('/v1/chat/completions')
@app.post('/v1/responses')
@app.post('/v1/messages')
@app.post('/v1/images/generations')
@app.post('/v1/images/edits')
@app.post('/v1/audio/speech')
@app.post('/v1/audio/transcriptions')
@app.post('/v1/audio/translations')
async def gateway(request:Request):
    start=time.time(); path=request.url.path; kind=PATH_KINDS[path]; source=PATH_FORMATS.get(path,'chat'); u=downstream_user(request)
    form=path in FORM_PATHS; body=None; form_items=None; upload_spools=[]
    def close_uploads():
        for file in upload_spools: file.close()
    try:
        if form:
            form_data=await request.form(); model_id=str(form_data.get('model','')); form_items=[]
            total_file_bytes=0
            for k,v in form_data.multi_items():
                if hasattr(v,'filename'):
                    staged=tempfile.SpooledTemporaryFile(max_size=262144,mode='w+b',dir=DATA); upload_spools.append(staged)
                    digest=hashlib.sha256(); size=0
                    while chunk:=await v.read(262144):
                        size+=len(chunk); total_file_bytes+=len(chunk)
                        if total_file_bytes>MAX_FORM_BYTES: raise HTTPException(413,'上传文件总大小超过 128 MB')
                        staged.write(chunk); digest.update(chunk)
                    staged.seek(0)
                    form_items.append((k,('file',v.filename,staged,v.content_type,size,digest.hexdigest())))
                else: form_items.append((k,str(v)))
        else:
            if int(request.headers.get('content-length','0'))>MAX_JSON_BYTES: raise HTTPException(413,'JSON 请求体超过 24 MB')
            chunks=[]; size=0
            async for chunk in request.stream():
                size+=len(chunk)
                if size>MAX_JSON_BYTES: raise HTTPException(413,'JSON 请求体超过 24 MB')
                chunks.append(chunk)
            raw_body=b''.join(chunks)
            body=json.loads(raw_body or b'{}')
            if not isinstance(body,dict): raise ValueError('请求体必须为 JSON 对象')
            model_id=body.get('model','')
    except HTTPException:
        close_uploads(); raise
    except Exception:
        close_uploads(); return api_error(400,'请求体无效')
    if not isinstance(model_id,str) or not model_id:
        close_uploads(); return api_error(400,'缺少有效的模型 ID')
    if not permitted(u,model_id):
        close_uploads(); return api_error(403,'此密钥不可使用该模型')
    with conn() as c:
        model=public_model(c,model_id,kind); cfg=settings(c)
        routes=get_routes(c,model,model['mode']) if model else []
    if not model:
        close_uploads(); return api_error(404,'模型不存在或类型不匹配')
    if not (u['is_admin'] or u['unlimited_balance']) and u['balance']<=0 and not model_is_free(model):
        close_uploads(); return api_error(402,'余额不足')
    if not routes:
        close_uploads(); return api_error(503,f'{model_id}模型无可用渠道','service_unavailable')
    stream=bool((body and (body.get('stream') or body.get('stream_format')=='sse')) or (form and str(form_data.get('stream','')).lower()=='true'))
    log_payload={'request_headers':redact_headers(dict(request.headers)),'request_body':body if not form else {'fields':[(k,{'filename':v[1],'bytes':v[4],'sha256':v[5],'base64':BinaryLog(v[2],v[4])} if isinstance(v,tuple) else v) for k,v in form_items]},'route_plan':[{'route_id':r['id'],'channel_name':r['channel_name']} for r in routes],'attempts':[],'_route_models':{r['id']:r['upstream_model_id'] for r in routes}}
    attempt_count=0; last_error=''
    # Speech synthesis is metered by input characters; other kinds use upstream usage tokens.
    tts_usage={'input':len(body.get('input')) if isinstance(body.get('input'),str) else 0} if kind=='tts' and body else None
    for route in routes:
        target=route['format'] if kind=='language' else 'chat'
        if not channel_supports_kind(route['format'],kind): continue
        try:
            if not form and (source==target or kind!='language'):
                # Same format: forward the client's exact bytes; only the top-level model value may change.
                converted=None; raw_upstream=replace_json_model(raw_body,route['upstream_model_id'])
            else:
                converted=convert_request(source,target,body) if kind=='language' else dict(body or {})
                converted['model']=route['upstream_model_id']
        except (ConversionError,ValueError) as e:
            log_payload['attempts'].append({'route_id':route['id'],'channel_name':route['channel_name'],'result':'skipped','error':str(e)})
            last_error=str(e); continue
        max_repeat=max(1,min(3,int(cfg.get('max_attempts_per_route',1))))
        for repeat in range(max_repeat):
            attempt_count+=1
            attempt={'number':attempt_count,'route_id':route['id'],'channel_name':route['channel_name'],'result':'failed'}
            log_payload['attempts'].append(attempt)
            url=upstream_url(route,path_for(target,source,path))
            headers=header_forward(request,route,source,target)
            timeout=httpx.Timeout(connect=float(cfg['connect_timeout']),read=float(cfg['idle_timeout']),write=60,pool=10)
            client=httpx.AsyncClient(timeout=timeout,follow_redirects=False,proxy=channel_proxy(route))
            client.headers.clear()
            total_deadline=time.monotonic()+float(cfg['total_timeout'])
            try:
                if form:
                    fields=[]; files=[]
                    for k,v in form_items:
                        if isinstance(v,tuple):
                            v[2].seek(0)
                            files.append((k,(v[1],v[2],v[3])))
                        else: fields.append((k,route['upstream_model_id'] if k=='model' else str(v)))
                    headers.pop('content-type',None)
                    field_map={}
                    for key,value in fields:
                        if key in field_map:
                            if not isinstance(field_map[key],list): field_map[key]=[field_map[key]]
                            field_map[key].append(value)
                        else: field_map[key]=value
                    req=client.build_request('POST',url,params=request.url.query or None,headers=headers,data=field_map,files=files)
                else:
                    headers.pop('content-length',None)
                    if source==target and kind=='language':
                        # Preserve all client fields and their JSON values; only the model ID changes.
                        pass
                    req=client.build_request('POST',url,params=request.url.query or None,headers=headers,**({'json':converted} if converted is not None else {'content':raw_upstream}))
                resp=await asyncio.wait_for(client.send(req,stream=True),timeout=max(0.1,total_deadline-time.monotonic()))
                if resp.status_code<200 or resp.status_code>=300:
                    sample_bytes=b''
                    async for chunk in response_chunks_with_deadline(resp,total_deadline):
                        sample_bytes+=chunk[:max(0,2048-len(sample_bytes))]
                        if len(sample_bytes)>=2048: break
                    sample=sample_bytes.decode('utf-8','replace')
                    last_error=f'HTTP {resp.status_code}: {sample[:200]}'
                    attempt.update(status=resp.status_code,error=sample[:2048])
                    await resp.aclose(); await client.aclose()
                    retry_status=resp.status_code in cfg['retry_statuses']
                    retry_text=any(x.lower() in sample.lower() for x in cfg['retry_text'])
                    if repeat+1<max_repeat and (retry_status or retry_text): continue
                    break
                if stream:
                    first_chunks=[]; first_events=[]; sse_buffer=b''; iterator=resp.aiter_bytes(); meaningful=False; first_bytes=0
                    deadline=min(total_deadline,time.monotonic()+float(cfg['first_token_timeout']))
                    while not meaningful:
                        remaining=deadline-time.monotonic()
                        if remaining<=0: raise TimeoutError('首 token 超时')
                        try: chunk=await asyncio.wait_for(iterator.__anext__(),timeout=remaining)
                        except StopAsyncIteration: raise ValueError('上游流未返回内容')
                        first_chunks.append(chunk)
                        first_bytes+=len(chunk)
                        if first_bytes>4_000_000: raise ValueError('首个有效事件前数据过大')
                        if target==source:
                            probe,_=parse_sse_buffer(b''.join(first_chunks))
                            if any(e.get('type') in ('error','response.failed') or e.get('error') for e in probe): raise ValueError('上游在首个输出前返回流式错误')
                            meaningful=any(e.get('type')!='error' for e in probe) if kind!='language' else any((e.get('choices') and any((q.get('delta') or {}).get('content') or (q.get('delta') or {}).get('tool_calls') for q in e['choices'])) or (e.get('type')=='content_block_start' and (e.get('content_block') or {}).get('type') in ('text','tool_use')) or (e.get('type')=='response.output_item.added' and (e.get('item') or {}).get('type')=='function_call') or e.get('type') in ('content_block_delta','response.output_text.delta','response.function_call_arguments.delta') for e in probe)
                        else:
                            events,sse_buffer=parse_sse_buffer(sse_buffer+chunk)
                            first_events.extend(events)
                            if any(e.get('type') in ('error','response.failed') or e.get('error') for e in events): raise ValueError('上游在首个输出前返回流式错误')
                            meaningful=any(has_convertible_output(e) for e in first_events)
                    async def stream_body():
                        nonlocal sse_buffer
                        raw_stream=tempfile.SpooledTemporaryFile(max_size=262144,mode='w+b',dir=DATA)
                        for first_chunk in first_chunks: raw_stream.write(first_chunk)
                        status=200; usage={'input':0,'output':0}; summary='成功'
                        attempt.update(status=200,result='succeeded')
                        try:
                            if source==target:
                                event_parser=UpstreamEvents(target); wire=b''; parsebuf=b''
                                for b in first_chunks:
                                    events,parsebuf=parse_sse_buffer(parsebuf+b)
                                    for e in events: event_parser.feed(e)
                                    frames,wire=rewrite_sse_models(wire+b,model_id)
                                    for frame in frames: yield frame
                                while True:
                                    if time.monotonic()>=total_deadline: raise TimeoutError('总超时')
                                    try: b=await asyncio.wait_for(iterator.__anext__(),timeout=max(0.1,total_deadline-time.monotonic()))
                                    except StopAsyncIteration: break
                                    raw_stream.write(b)
                                    events,parsebuf=parse_sse_buffer(parsebuf+b)
                                    for e in events: event_parser.feed(e)
                                    frames,wire=rewrite_sse_models(wire+b,model_id)
                                    for frame in frames: yield frame
                                usage=tts_usage or event_parser.usage
                            else:
                                parser=UpstreamEvents(target); emitter=DownstreamEvents(source,model_id,bool((body.get('stream_options') or {}).get('include_usage')))
                                for e in first_events:
                                    parsed_items=parser.feed(e); emitter.in_tokens=parser.input_tokens
                                    for item in parsed_items:
                                        for b in emitter.push(item): yield b
                                while True:
                                    if time.monotonic()>=total_deadline: raise TimeoutError('总超时')
                                    try: chunk=await asyncio.wait_for(iterator.__anext__(),timeout=max(0.1,total_deadline-time.monotonic()))
                                    except StopAsyncIteration: break
                                    raw_stream.write(chunk)
                                    events,sse_buffer=parse_sse_buffer(sse_buffer+chunk)
                                    for e in events:
                                        parsed_items=parser.feed(e); emitter.in_tokens=parser.input_tokens
                                        for item in parsed_items:
                                            for b in emitter.push(item): yield b
                                usage=parser.usage
                                for b in emitter.end(): yield b
                        except Exception as e:
                            status=502; summary='流传输中断：'+str(e)[:160]
                            attempt.update(result='interrupted',error=str(e)[:2048])
                            for event in downstream_stream_error(source,model_id): yield event
                        finally:
                            await resp.aclose(); await client.aclose()
                            log_payload['response_stream']={'bytes':raw_stream.tell(),'storage':'完整流见日志下载'}
                            try: await asyncio.to_thread(save_log,start,u,model,path,status,route,attempt_count,log_payload,usage,summary,raw_stream)
                            finally:
                                raw_stream.close(); close_uploads()
                    h=response_headers(resp); h['cache-control']='no-cache'; h['x-accel-buffering']='no'
                    return StreamingResponse(stream_body(),status_code=200,media_type='text/event-stream',headers=h)
                h=response_headers(resp)
                if kind!='language' and 'json' not in h.get('content-type','').lower():
                    staged=tempfile.SpooledTemporaryFile(max_size=262144,mode='w+b',dir=DATA)
                    digest=hashlib.sha256(); response_size=0; sample_bytes=b''
                    try:
                        async for chunk in response_chunks_with_deadline(resp,total_deadline):
                            response_size+=len(chunk); staged.write(chunk); digest.update(chunk)
                            if len(sample_bytes)<8192: sample_bytes+=chunk[:8192-len(sample_bytes)]
                        await resp.aclose(); await client.aclose()
                        if h.get('content-type','').lower().startswith('text/plain') and any(x.lower() in sample_bytes.decode('utf-8','replace').lower() for x in cfg['retry_text']):
                            last_error='上游返回错误内容'; attempt.update(status=200,error=sample_bytes[:2048].decode('utf-8','replace'))
                            staged.close()
                            if repeat+1<max_repeat: continue
                            break
                        log_payload['response_headers']=redact_headers(dict(resp.headers))
                        log_payload['response_body']={'bytes':response_size,'sha256':digest.hexdigest(),'base64':BinaryLog(staged,response_size)}
                        attempt.update(status=200,result='succeeded')
                        await asyncio.to_thread(save_log,start,u,model,path,200,route,attempt_count,log_payload,tts_usage or {},'成功')
                        staged.seek(0); close_uploads()
                        async def binary_body():
                            try:
                                while piece:=await asyncio.to_thread(staged.read,262144): yield piece
                            finally: staged.close()
                        return StreamingResponse(binary_body(),status_code=200,headers=h)
                    except Exception:
                        staged.close(); raise
                raw_parts=[]; response_size=0
                async for chunk in response_chunks_with_deadline(resp,total_deadline):
                    response_size+=len(chunk)
                    if response_size>MAX_JSON_RESPONSE_BYTES: raise ValueError('上游 JSON 响应超过 32 MB')
                    raw_parts.append(chunk)
                raw=b''.join(raw_parts)
                await resp.aclose(); await client.aclose()
                response_text=raw[:8192].decode('utf-8','replace')
                response_obj=None
                if 'json' in h.get('content-type','').lower() or (kind=='language' and raw.lstrip().startswith((b'{',b'['))):
                    try: response_obj=json.loads(raw)
                    except ValueError: pass
                is_error=isinstance(response_obj,dict) and (response_obj.get('error') or response_obj.get('type')=='error')
                if not is_error and kind=='language' and isinstance(response_obj,dict):
                    expected={'chat':'choices','responses':'output','anthropic':'content'}[target]
                    is_error=expected not in response_obj and any(x.lower() in response_text.lower() for x in cfg['retry_text'])
                if not is_error and h.get('content-type','').lower().startswith('text/plain'):
                    is_error=any(x.lower() in response_text.lower() for x in cfg['retry_text'])
                if is_error:
                    last_error='上游返回错误内容：'+response_text[:200]
                    attempt.update(status=200,error=response_text[:2048])
                    if repeat+1<max_repeat: continue
                    break
                if kind=='language':
                    try:
                        upstream=response_obj if response_obj is not None else json.loads(raw)
                        output=convert_response(target,source,upstream,model_id)
                        data=json.dumps(output,ensure_ascii=False,separators=(',',':')).encode()
                    except (ValueError,ConversionError) as e:
                        last_error='输出转换失败：'+str(e); attempt.update(status=200,error=last_error); break
                    if source==target:
                        # Exact same-format response only changes the upstream model name.
                        pass
                    usage=usage_numbers(upstream.get('usage') if isinstance(upstream,dict) else None)
                    h['content-type']='application/json'
                else:
                    data=raw; usage=tts_usage or {}
                    if 'json' in h.get('content-type','').lower():
                        try:
                            media_json=json.loads(raw)
                            if isinstance(media_json,dict) and not tts_usage: usage=usage_numbers(media_json.get('usage'))
                            if isinstance(media_json,dict) and 'model' in media_json:
                                media_json['model']=model_id
                                data=json.dumps(media_json,ensure_ascii=False,separators=(',',':')).encode()
                        except ValueError: pass
                log_payload['response_headers']=redact_headers(dict(resp.headers)); log_payload['response_body']=data.decode('utf-8','replace') if kind=='language' or 'json' in h.get('content-type','') else {'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'base64':base64.b64encode(data).decode()}
                attempt.update(status=200,result='succeeded')
                await asyncio.to_thread(save_log,start,u,model,path,200,route,attempt_count,log_payload,usage,'成功')
                close_uploads()
                return Response(data,status_code=200,headers=h)
            except (httpx.TimeoutException,asyncio.TimeoutError,TimeoutError,httpx.RequestError,ValueError) as e:
                last_error=str(e) or type(e).__name__; attempt.update(error=last_error[:2048])
                await client.aclose()
                if repeat+1<max_repeat: continue
                break
    await asyncio.to_thread(save_log,start,u,model,path,503,None,attempt_count,log_payload,{},'无可用渠道：'+last_error[:160])
    close_uploads()
    return api_error(503,f'{model_id}模型无可用渠道','service_unavailable')
