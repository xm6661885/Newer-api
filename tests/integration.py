import base64, json, os, threading, time, uuid
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
import httpx

BASE=os.environ.get('NEWER_API_TEST_BASE','')
TEST_CONFIG=os.environ.get('NEWER_API_TEST_CONFIG','')
TEST_DB=os.environ.get('NEWER_API_TEST_DB','')
if not BASE or not TEST_CONFIG or not TEST_DB or urlsplit(BASE).port==18988 or Path(TEST_CONFIG).resolve()==Path(__file__).resolve().parents[1]/'config.json' or Path(TEST_DB).resolve()==Path(__file__).resolve().parents[1]/'data'/'gateway.sqlite3':
    raise SystemExit('请设置隔离测试服务 NEWER_API_TEST_BASE、配置 NEWER_API_TEST_CONFIG 和数据库 NEWER_API_TEST_DB；禁止使用生产服务')
test_credentials=json.loads(Path(TEST_CONFIG).read_text())
MARK='xf-int-'+uuid.uuid4().hex[:8]
seen=[]
recover_hits=0
class Mock(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*a): pass
    def do_GET(self):
        data=json.dumps({'data':[{'id':'up-model','capabilities':{'vision':True,'tools':True}}]}).encode()
        self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def do_POST(self):
        global recover_hits
        n=int(self.headers.get('content-length','0')); raw=self.rfile.read(n); seen.append({'path':self.path,'headers':dict(self.headers),'body':raw})
        if self.headers.get('x-mock-fail')=='recover':
            recover_hits+=1
            if recover_hits==1:
                data=b'{"error":"temporary outage"}';self.send_response(503);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);return
        if self.headers.get('x-mock-fail')=='slow' and json.loads(raw).get('stream'):
            self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Connection','close');self.end_headers();self.wfile.flush();time.sleep(.8)
            try:self.wfile.write(b'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"slow"}}\n\n');self.wfile.flush()
            except BrokenPipeError:pass
            return
        if self.headers.get('x-mock-fail') and self.headers.get('x-mock-fail')!='recover':
            data=b'{"error":"temporary outage"}';self.send_response({'yes':503,'forbidden':403,'json200':200,'slow':503}[self.headers.get('x-mock-fail')]);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);return
        try: obj=json.loads(raw)
        except Exception: obj={}
        if self.path=='/v1/messages':
            if obj.get('stream'):
                if self.headers.get('x-test-thinking'):
                    events=[('message_start',{'type':'message_start','message':{'id':'msg_up','model':'up-model','usage':{'input_tokens':5}}}),('content_block_start',{'type':'content_block_start','index':0,'content_block':{'type':'thinking','thinking':''}}),('content_block_delta',{'type':'content_block_delta','index':0,'delta':{'type':'thinking_delta','thinking':'private'}}),('content_block_delta',{'type':'content_block_delta','index':0,'delta':{'type':'signature_delta','signature':'opaque'}}),('content_block_stop',{'type':'content_block_stop','index':0}),('content_block_start',{'type':'content_block_start','index':1,'content_block':{'type':'text','text':''}}),('content_block_delta',{'type':'content_block_delta','index':1,'delta':{'type':'text_delta','text':'hello'}}),('content_block_stop',{'type':'content_block_stop','index':1}),('message_delta',{'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':7}}),('message_stop',{'type':'message_stop'})]
                elif self.headers.get('x-test-tool-stream'):
                    events=[('message_start',{'type':'message_start','message':{'id':'msg_up','model':'up-model','usage':{'input_tokens':5}}}),('content_block_start',{'type':'content_block_start','index':0,'content_block':{'type':'tool_use','id':'toolu_1','name':'weather','input':{}}}),('content_block_delta',{'type':'content_block_delta','index':0,'delta':{'type':'input_json_delta','partial_json':'{"city":'}}),('content_block_delta',{'type':'content_block_delta','index':0,'delta':{'type':'input_json_delta','partial_json':'"Paris"}'}}),('content_block_stop',{'type':'content_block_stop','index':0}),('message_delta',{'type':'message_delta','delta':{'stop_reason':'tool_use'},'usage':{'output_tokens':2}}),('message_stop',{'type':'message_stop'})]
                else: events=[('message_start',{'type':'message_start','message':{'id':'msg_up','model':'up-model','usage':{'input_tokens':5}}}),('content_block_start',{'type':'content_block_start','index':0,'content_block':{'type':'text','text':''}}),('content_block_delta',{'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':'hello'}}),('content_block_stop',{'type':'content_block_stop','index':0}),('message_delta',{'type':'message_delta','delta':{'stop_reason':'end_turn'},'usage':{'output_tokens':2}}),('message_stop',{'type':'message_stop'})]
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Connection','close');self.end_headers()
                for typ,p in events:self.wfile.write((f'event: {typ}\ndata: '+json.dumps(p)+'\n\n').encode());self.wfile.flush();time.sleep(.01)
                return
            data={'id':'msg_up','type':'message','role':'assistant','model':'up-model','content':[{'type':'thinking','thinking':'private','signature':'opaque'},{'type':'text','text':'hello'}] if self.headers.get('x-test-thinking') else [{'type':'text','text':'hello'},{'type':'tool_use','id':'toolu_1','name':'weather','input':{'city':'Paris'}}],'stop_reason':'end_turn' if self.headers.get('x-test-thinking') else 'tool_use','usage':{'input_tokens':5,'output_tokens':8}}
        elif self.path=='/v1/chat/completions':
            if obj.get('stream'):
                if self.headers.get('x-test-tool-stream'):
                    chunks=[{'id':'up','object':'chat.completion.chunk','model':'up-model','choices':[{'index':0,'delta':{'role':'assistant'},'finish_reason':None}]},{'id':'up','object':'chat.completion.chunk','model':'up-model','choices':[{'index':0,'delta':{'tool_calls':[{'index':0,'id':'call_1','type':'function','function':{'name':'weather','arguments':'{"city":'}}]},'finish_reason':None}]},{'id':'up','object':'chat.completion.chunk','model':'up-model','choices':[{'index':0,'delta':{'tool_calls':[{'index':0,'function':{'arguments':'"Paris"}'}}]},'finish_reason':None}]},{'id':'up','object':'chat.completion.chunk','model':'up-model','choices':[{'index':0,'delta':{},'finish_reason':'tool_calls'}]}]
                else: chunks=[{'id':'up','object':'chat.completion.chunk','model':'up-model','choices':[{'index':0,'delta':{'role':'assistant'},'finish_reason':None}]},{'id':'up','object':'chat.completion.chunk','model':'up-model','choices':[{'index':0,'delta':{'content':'hi'},'finish_reason':None}]},{'id':'up','object':'chat.completion.chunk','model':'up-model','choices':[{'index':0,'delta':{},'finish_reason':'stop'}]}]
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Connection','close');self.end_headers()
                for p in chunks:self.wfile.write(('data: '+json.dumps(p)+'\n\n').encode());self.wfile.flush();time.sleep(.01)
                self.wfile.write(b'data: [DONE]\n\n');self.wfile.flush();return
            data={'id':'chat_up','object':'chat.completion','created':1,'model':'up-model','choices':[{'index':0,'message':{'role':'assistant','content':None,'tool_calls':[{'id':'call_1','type':'function','function':{'name':'weather','arguments':'{"city":"Paris"}'}}]},'finish_reason':'tool_calls'}],'usage':{'prompt_tokens':3,'completion_tokens':6,'total_tokens':9}}
        elif self.path=='/v1/responses':
            if obj.get('stream'):
                events=[{'type':'response.created','response':{'id':'resp_up','model':'up-model','output':[]}}, {'type':'response.output_item.added','output_index':0,'item':{'id':'fc_up','type':'function_call','call_id':'call_1','name':'weather','arguments':''}}, {'type':'response.function_call_arguments.delta','output_index':0,'item_id':'fc_up','delta':'{\"city\":'}, {'type':'response.function_call_arguments.delta','output_index':0,'item_id':'fc_up','delta':'\"Paris\"}'}, {'type':'response.completed','response':{'id':'resp_up','model':'up-model','output':[],'usage':{'input_tokens':4,'output_tokens':4,'total_tokens':8}}}]
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.send_header('Connection','close');self.end_headers()
                for ev in events:self.wfile.write((f'event: {ev["type"]}\ndata: '+json.dumps(ev)+'\n\n').encode());self.wfile.flush();time.sleep(.01)
                return
            data={'id':'resp_up','object':'response','model':'up-model','status':'completed','output':[{'type':'function_call','call_id':'call_1','name':'weather','arguments':'{\"city\":\"Paris\"}'}],'usage':{'input_tokens':4,'output_tokens':4,'total_tokens':8}}
        elif self.path in ('/v1/images/edits','/v1/audio/translations'): data={'created':1,'model':'up-model','data':[{'b64_json':'aGVsbG8='}]} if self.path.endswith('edits') else {'text':'translated','model':'up-model'}
        elif self.path=='/v1/images/generations': data={'created':1,'model':'up-model','data':[{'b64_json':'aGVsbG8='}]}
        elif self.path=='/v1/audio/speech':
            data=b'ID3FAKEAUDIO';self.send_response(200);self.send_header('Content-Type','audio/mpeg');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);return
        elif self.path=='/v1/audio/transcriptions': data={'text':'spoken words','model':'up-model'}
        else: data={'error':'unexpected '+self.path}
        data=json.dumps(data).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)

server=ThreadingHTTPServer(('127.0.0.1',0),Mock);threading.Thread(target=server.serve_forever,daemon=True).start()
admin=httpx.Client(base_url=BASE,timeout=30)
created={'channels':[],'models':[],'keys':[],'users':[],'announcements':[]}; asset_path=None; original_settings=None
def call(path,method='GET',data=None):
    r=admin.request(method,path,json=data);assert r.status_code<300,(path,r.status_code,r.text[:300]);return r.json()
try:
    health=admin.get('/health');assert health.status_code==200 and health.json().get('test_instance') is True,'目标服务不是隔离测试实例'
    call('/api/login','POST',{'username':test_credentials['admin_username'],'password':test_credentials['admin_password']})
    assert httpx.get(BASE+'/api/announcements').status_code==401
    announcement=call('/api/admin/announcements','POST',{'title':'维护通知','body':'第一行\n第二行','published':True})['id'];created['announcements'].append(announcement)
    assert any(x['id']==announcement for x in call('/api/announcements'))
    call(f'/api/admin/announcements/{announcement}','PUT',{'title':'维护通知更新','body':'正文','published':False})
    assert not any(x['id']==announcement for x in call('/api/announcements'))
    call(f'/api/admin/announcements/{announcement}','PUT',{'title':'维护通知更新','body':'正文','published':True})
    username='test_'+uuid.uuid4().hex[:12];password=uuid.uuid4().hex
    uid=call('/api/admin/users','POST',{'username':username,'password':password})['id'];created['users'].append(uid)
    with httpx.Client(base_url=BASE) as ordinary:
        r=ordinary.post('/api/login',json={'username':username,'password':password});assert r.status_code==200
        assert any(x['id']==announcement for x in ordinary.get('/api/announcements').json())
        assert ordinary.get('/api/admin/announcements').status_code==403
    original_settings=call('/api/admin/settings')
    sample=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/P9sAAAAASUVORK5CYII=')
    upload=admin.post('/api/admin/assets',files={'file':('tiny.png',sample,'image/png')});assert upload.status_code==200,upload.text;asset_path=upload.json()['url'];assert admin.get(asset_path).content==sample
    call(f'/api/admin/announcements/{announcement}','PUT',{'title':'维护通知更新','body':'正文','image_url':asset_path,'published':True})
    assert next(x for x in call('/api/announcements') if x['id']==announcement)['image_url']==asset_path
    call('/api/admin/settings','PUT',{'text_overrides':{'总览':'概览'}});assert call('/api/public')['text_overrides']['总览']=='概览'
    call('/api/admin/settings','PUT',{'text_overrides':original_settings['text_overrides'],'first_token_timeout':original_settings['first_token_timeout']})
    mock_url=f'http://127.0.0.1:{server.server_port}'
    fail=call('/api/admin/channels','POST',{'name':MARK+'-fail','format':'anthropic','base_url':mock_url,'api_key':'mock','extra_headers':{'x-mock-fail':'yes'}})['id'];created['channels'].append(fail)
    fail403=call('/api/admin/channels','POST',{'name':MARK+'-403','format':'anthropic','base_url':mock_url,'api_key':'mock','extra_headers':{'x-mock-fail':'forbidden'}})['id'];created['channels'].append(fail403)
    fail200=call('/api/admin/channels','POST',{'name':MARK+'-200','format':'anthropic','base_url':mock_url,'api_key':'mock','extra_headers':{'x-mock-fail':'json200'}})['id'];created['channels'].append(fail200)
    slow=call('/api/admin/channels','POST',{'name':MARK+'-slow','format':'anthropic','base_url':mock_url,'api_key':'mock','extra_headers':{'x-mock-fail':'slow'}})['id'];created['channels'].append(slow)
    ant=call('/api/admin/channels','POST',{'name':MARK+'-ant','format':'anthropic','base_url':mock_url,'api_key':'mock'})['id'];created['channels'].append(ant)
    chat=call('/api/admin/channels','POST',{'name':MARK+'-chat','format':'chat','base_url':mock_url,'api_key':'mock'})['id'];created['channels'].append(chat)
    recover=call('/api/admin/channels','POST',{'name':MARK+'-recover','format':'chat','base_url':mock_url,'api_key':'mock','extra_headers':{'x-mock-fail':'recover'}})['id'];created['channels'].append(recover)
    recovered_model=call('/api/admin/models','POST',{'public_id':MARK+'-recover','kind':'language'})['id'];created['models'].append(recovered_model)
    call(f'/api/admin/models/{recovered_model}/routes','POST',{'channel_id':recover,'upstream_model_id':'up-model'})
    call(f'/api/admin/models/{recovered_model}/routes','POST',{'channel_id':chat,'upstream_model_id':'up-model'})
    count=call(f'/api/admin/channels/{ant}/fetch-models','POST')['count'];assert count==1
    assert call(f'/api/admin/channels/{chat}/fetch-models','POST')['count']==1
    assert [x['kind'] for x in call(f'/api/admin/channels/{chat}/models')]==['language']
    call(f'/api/admin/channels/{chat}/models','POST',{'model_id':'extra-model','kind':'language'})
    pool=call('/api/admin/models','POST',{'public_id':MARK+'-pool','kind':'language'})['id'];created['models'].append(pool)
    call(f'/api/admin/models/{pool}/routes','POST',{'channel_id':ant,'upstream_model_id':'up-model'})
    assert call(f'/api/admin/models/{pool}/routes/import','POST',{'channel_id':chat})['count']==2
    assert call(f'/api/admin/models/{pool}/routes/import','POST',{'channel_id':chat})['count']==0
    pool_routes=next(m for m in call('/api/admin/models') if m['id']==pool)['routes']
    assert [(r['channel_id'],r['upstream_model_id'],r['priority']) for r in pool_routes]==[(ant,'up-model',0),(chat,'extra-model',1),(chat,'up-model',2)],pool_routes
    a=call('/api/admin/models','POST',{'public_id':MARK+'-a','kind':'language'})['id'];created['models'].append(a)
    call(f'/api/admin/models/{a}/routes','POST',{'channel_id':fail,'upstream_model_id':'up-model','priority':0})
    call(f'/api/admin/models/{a}/routes','POST',{'channel_id':fail403,'upstream_model_id':'up-model','priority':1})
    call(f'/api/admin/models/{a}/routes','POST',{'channel_id':fail200,'upstream_model_id':'up-model','priority':2})
    call(f'/api/admin/models/{a}/routes','POST',{'channel_id':slow,'upstream_model_id':'up-model','priority':3})
    call(f'/api/admin/models/{a}/routes','POST',{'channel_id':ant,'upstream_model_id':'up-model','priority':4})
    c=call('/api/admin/models','POST',{'public_id':MARK+'-c','kind':'language'})['id'];created['models'].append(c)
    call(f'/api/admin/models/{c}/routes','POST',{'channel_id':chat,'upstream_model_id':'up-model'})
    responses=call('/api/admin/channels','POST',{'name':MARK+'-responses','format':'responses','base_url':mock_url,'api_key':'mock'})['id'];created['channels'].append(responses)
    rr=call('/api/admin/models','POST',{'public_id':MARK+'-r','kind':'language'})['id'];created['models'].append(rr)
    call(f'/api/admin/models/{rr}/routes','POST',{'channel_id':responses,'upstream_model_id':'up-model'})
    for kind,path in [('image','i'),('tts','t'),('transcription','s')]:
        m=call('/api/admin/models','POST',{'public_id':MARK+'-'+path,'kind':kind})['id'];created['models'].append(m)
        for text_channel in (chat,responses,ant):
            r=admin.post(f'/api/admin/models/{m}/routes',json={'channel_id':text_channel,'upstream_model_id':'custom-media'});assert r.status_code==400,(kind,text_channel,r.text)
        media=call('/api/admin/channels','POST',{'name':MARK+'-'+kind,'format':kind,'base_url':mock_url,'api_key':'mock'})['id'];created['channels'].append(media)
        call(f'/api/admin/models/{m}/routes','POST',{'channel_id':media,'upstream_model_id':'up-model'})
    r=admin.post(f'/api/admin/channels/{chat}/models',json={'model_id':'dall-e-3','kind':'image'});assert r.status_code==400,r.text
    key=call('/api/me/keys','POST',{'name':MARK})['key']; created['keys'].append(call('/api/me/keys') [0]['id'])
    api=httpx.Client(base_url=BASE,headers={'Authorization':'Bearer '+key,'X-Test-Header':'must-forward'},timeout=30)
    for expected in (chat,recover):
        r=api.post('/v1/chat/completions',json={'model':MARK+'-recover','messages':[{'role':'user','content':'hello'}]});assert r.status_code==200,r.text
        found=next(x for x in call('/api/admin/logs') if x['model_id']==MARK+'-recover')
        assert found['channel_id']==expected and found['selected_channel_name']==(MARK+'-chat' if expected==chat else MARK+'-recover'),found
        assert found['route_trace'][0]['channel_name']==MARK+'-recover' and found['route_trace'][-1]['result']=='succeeded',found['route_trace']
        assert all(a.get('upstream_model')=='up-model' for a in found['route_trace']),found['route_trace']
    assert recover_hits==2
    tools=[{'type':'function','function':{'name':'weather','description':'lookup','parameters':{'type':'object','properties':{'city':{'type':'string'}}}}}]
    r=api.post('/v1/chat/completions',json={'model':MARK+'-a','messages':[{'role':'user','content':'weather?'}],'tools':tools});assert r.status_code==200,r.text; d=r.json();assert d['model']==MARK+'-a' and d['choices'][0]['message']['tool_calls'][0]['function']['name']=='weather'
    r=api.post('/v1/chat/completions',headers={'x-test-thinking':'yes'},json={'model':MARK+'-pool','messages':[{'role':'user','content':'hello'}]});assert r.status_code==200,r.text;assert r.json()['choices'][0]['message']['content']=='hello' and 'private' not in r.text
    assert any(any(k.lower()=='x-test-header' and v=='must-forward' for k,v in x['headers'].items()) for x in seen)
    assert not any(x['headers'].get('Authorization','').endswith(key) for x in seen)
    r=api.post('/v1/messages',json={'model':MARK+'-c','max_tokens':100,'messages':[{'role':'user','content':'weather?'}],'tools':[{'name':'weather','input_schema':{'type':'object','properties':{}}}]});assert r.status_code==200,r.text;assert r.json()['content'][0]['type']=='tool_use'
    r=api.post('/v1/responses',json={'model':MARK+'-a','input':'weather?','tools':[{'type':'function','name':'weather','parameters':{'type':'object','properties':{}}}]});assert r.status_code==200,r.text;assert any(x['type']=='function_call' for x in r.json()['output'])
    call('/api/admin/settings','PUT',{'first_token_timeout':0.2})
    r=api.post('/v1/chat/completions',json={'model':MARK+'-a','messages':[{'role':'user','content':'hello'}],'stream':True});assert r.status_code==200,r.text;assert 'hello' in r.text and 'slow' not in r.text and '[DONE]' in r.text and 'up-model' not in r.text
    r=api.post('/v1/messages',json={'model':MARK+'-c','max_tokens':100,'messages':[{'role':'user','content':'hello'}],'stream':True});assert r.status_code==200,r.text;assert 'hi' in r.text and 'message_stop' in r.text and 'up-model' not in r.text
    r=api.post('/v1/chat/completions',json={'model':MARK+'-c','messages':[{'role':'user','content':'hello'}],'stream':True});assert r.status_code==200,r.text;assert 'up-model' not in r.text
    r=api.post('/v1/chat/completions',json={'model':MARK+'-r','messages':[{'role':'user','content':'weather?'}],'tools':tools});assert r.status_code==200,r.text;assert r.json()['choices'][0]['message']['tool_calls'][0]['id']=='call_1'
    r=api.post('/v1/messages',json={'model':MARK+'-r','max_tokens':100,'messages':[{'role':'user','content':'weather?'}]});assert r.status_code==200,r.text;assert r.json()['content'][0]['type']=='tool_use'
    r=api.post('/v1/responses',json={'model':MARK+'-c','input':'weather?'});assert r.status_code==200,r.text;assert r.json()['output'][0]['type']=='function_call'
    r=api.post('/v1/responses',json={'model':MARK+'-r','input':'weather?'});assert r.status_code==200,r.text;assert r.json()['model']==MARK+'-r'
    r=api.post('/v1/chat/completions',headers={'x-test-tool-stream':'yes'},json={'model':MARK+'-a','messages':[{'role':'user','content':'weather?'}],'stream':True});assert r.status_code==200,r.text;assert 'tool_calls' in r.text and 'Paris' in r.text and 'up-model' not in r.text
    r=api.post('/v1/chat/completions',headers={'x-test-thinking':'yes'},json={'model':MARK+'-pool','messages':[{'role':'user','content':'hello'}],'stream':True});assert r.status_code==200,r.text;assert 'hello' in r.text and 'private' not in r.text and '[DONE]' in r.text
    r=api.post('/v1/messages',headers={'x-test-tool-stream':'yes'},json={'model':MARK+'-c','max_tokens':100,'messages':[{'role':'user','content':'weather?'}],'stream':True});assert r.status_code==200,r.text;assert 'input_json_delta' in r.text and 'Paris' in r.text
    r=api.post('/v1/chat/completions',json={'model':MARK+'-r','messages':[{'role':'user','content':'weather?'}],'stream':True});assert r.status_code==200,r.text;assert 'tool_calls' in r.text and 'Paris' in r.text and 'up-model' not in r.text
    r=api.post('/v1/responses',json={'model':MARK+'-a','input':'weather?','stream':True});assert r.status_code==200,r.text;assert 'response.completed' in r.text and 'hello' in r.text
    r=api.post('/v1/responses',json={'model':MARK+'-r','input':'weather?','stream':True});assert r.status_code==200,r.text;assert 'response.completed' in r.text and 'up-model' not in r.text
    r=api.post('/v1/images/generations',json={'model':MARK+'-i','prompt':'a cat'});assert r.status_code==200,r.text;assert r.json()['data'][0]['b64_json']=='aGVsbG8='
    r=api.post('/v1/images/edits',data={'model':MARK+'-i','prompt':'edit'},files={'image':('test.png',b'\x89PNG\r\n\x1a\n','image/png')});assert r.status_code==200,r.text;assert r.json()['model']==MARK+'-i'
    r=api.post('/v1/audio/speech',json={'model':MARK+'-t','input':'hi','voice':'alloy'});assert r.status_code==200 and r.content==b'ID3FAKEAUDIO'
    r=api.post('/v1/audio/transcriptions',data={'model':MARK+'-s'},files={'file':('test.wav',b'RIFFMOCK','audio/wav')});assert r.status_code==200,r.text;assert r.json()['text']=='spoken words'
    r=api.post('/v1/audio/translations',data={'model':MARK+'-s'},files={'file':('test.wav',b'RIFFMOCK','audio/wav')});assert r.status_code==200,r.text;assert r.json()['text']=='translated'
    logs=call('/api/admin/logs');assert any(x['model_id']==MARK+'-a' for x in logs)
    print('PASS: recovery across requests, route logs, announcements, headers, tools, streaming, media')
finally:
    for i in created['announcements']:
        try:call(f'/api/admin/announcements/{i}','DELETE')
        except Exception:pass
    for i in created['users']:
        try:call(f'/api/admin/users/{i}','DELETE')
        except Exception:pass
    for i in created['keys']:
        try:call(f'/api/me/keys/{i}','DELETE')
        except Exception:pass
    for i in created['models']:
        try:call(f'/api/admin/models/{i}','DELETE')
        except Exception:pass
    for i in created['channels']:
        try:call(f'/api/admin/channels/{i}','DELETE')
        except Exception:pass
    if original_settings:
        try:call('/api/admin/settings','PUT',{'text_overrides':original_settings['text_overrides'],'first_token_timeout':original_settings['first_token_timeout']})
        except Exception:pass
    if asset_path:
        try:os.unlink(str(Path(os.environ['NEWER_API_TEST_DB']).parent/'assets'/asset_path.rsplit('/',1)[-1]))
        except Exception:pass
    server.shutdown();admin.close()
