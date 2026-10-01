import json, secrets, time
from conversion import tool_input, usage_numbers

def sse(data,event=None):
    head=(f'event: {event}\n' if event else '')
    return (head+'data: '+json.dumps(data,ensure_ascii=False,separators=(',',':'))+'\n\n').encode()

class UpstreamEvents:
    def __init__(self,fmt):
        self.fmt=fmt; self.tools={}; self.finish=None; self.input_tokens=0; self.output_tokens=0; self.raw_usage={}
    @property
    def usage(self): return usage_numbers(self.raw_usage)
    def merge_usage(self,u):
        if isinstance(u,dict): self.raw_usage.update({k:v for k,v in u.items() if v is not None})
    def feed(self,event):
        typ=event.get('type','')
        if typ in ('error','response.failed') or event.get('error'): raise ValueError('上游流错误')
        out=[]
        if self.fmt=='chat':
            for choice in event.get('choices') or []:
                d=choice.get('delta') or {}
                if d.get('content'): out.append({'type':'text','text':d['content']})
                for t in d.get('tool_calls') or []:
                    i=t.get('index',0)
                    if i not in self.tools:
                        self.tools[i]={'id':t.get('id') or 'call_'+secrets.token_hex(8),'name':(t.get('function') or {}).get('name','')}
                        out.append({'type':'tool_start','index':i,**self.tools[i]})
                    f=t.get('function') or {}
                    if f.get('arguments'): out.append({'type':'tool_delta','index':i,'delta':f['arguments']})
                if choice.get('finish_reason'):
                    self.finish=choice['finish_reason']; out.append({'type':'finish','reason':self.finish})
            u=event.get('usage') or {}
            if u:
                self.merge_usage(u)
                self.input_tokens=u.get('prompt_tokens',0); self.output_tokens=u.get('completion_tokens',0)
                out.append({'type':'usage','input':self.input_tokens,'output':self.output_tokens})
        elif self.fmt=='anthropic':
            if typ=='message_start':
                u=(event.get('message') or {}).get('usage') or {}; self.input_tokens=u.get('input_tokens',0); self.merge_usage(u)
            elif typ=='content_block_start':
                p=event.get('content_block') or {}; i=event.get('index',0)
                if p.get('type')=='tool_use':
                    self.tools[i]={'id':p.get('id','toolu_'+secrets.token_hex(8)),'name':p.get('name','')}
                    out.append({'type':'tool_start','index':i,**self.tools[i]})
                    if p.get('input'): out.append({'type':'tool_delta','index':i,'delta':json.dumps(p['input'],ensure_ascii=False,separators=(',',':'))})
                elif p.get('type')=='text' and p.get('text'): out.append({'type':'text','text':p['text']})
                elif p.get('type') not in ('text','thinking','redacted_thinking'): raise ValueError(f'无法转换流式内容 {p.get("type")}')
            elif typ=='content_block_delta':
                d=event.get('delta') or {}; i=event.get('index',0)
                if d.get('type')=='text_delta': out.append({'type':'text','text':d.get('text','')})
                elif d.get('type')=='input_json_delta': out.append({'type':'tool_delta','index':i,'delta':d.get('partial_json','')})
            elif typ=='message_delta':
                u=event.get('usage') or {}; self.output_tokens=u.get('output_tokens',self.output_tokens); self.merge_usage(u)
                reason=(event.get('delta') or {}).get('stop_reason')
                if reason:
                    self.finish={'tool_use':'tool_calls','max_tokens':'length'}.get(reason,'stop')
                    out.append({'type':'finish','reason':self.finish})
                out.append({'type':'usage','input':self.input_tokens,'output':self.output_tokens})
            elif typ=='error': raise ValueError((event.get('error') or {}).get('message','上游流错误'))
        elif self.fmt=='responses':
            if typ=='response.output_text.delta': out.append({'type':'text','text':event.get('delta','')})
            elif typ=='response.output_item.added':
                item=event.get('item') or {}; i=event.get('output_index',0)
                if item.get('type')=='function_call':
                    self.tools[i]={'id':item.get('call_id') or item.get('id') or 'call_'+secrets.token_hex(8),'name':item.get('name','')}
                    out.append({'type':'tool_start','index':i,**self.tools[i]})
                    if item.get('arguments'): out.append({'type':'tool_delta','index':i,'delta':item['arguments']})
            elif typ=='response.function_call_arguments.delta': out.append({'type':'tool_delta','index':event.get('output_index',0),'delta':event.get('delta','')})
            elif typ in ('response.completed','response.incomplete'):
                resp=event.get('response') or {}; u=resp.get('usage') or {}; self.merge_usage(u)
                self.input_tokens=u.get('input_tokens',0); self.output_tokens=u.get('output_tokens',0)
                self.finish='tool_calls' if self.tools else ('length' if typ=='response.incomplete' else 'stop')
                out.extend([{'type':'finish','reason':self.finish},{'type':'usage','input':self.input_tokens,'output':self.output_tokens}])
            elif typ in ('response.failed','error'): raise ValueError('上游 Responses 流失败')
        return out

class DownstreamEvents:
    def __init__(self,fmt,model,include_usage=False):
        self.fmt=fmt; self.model=model; self.id= ('chatcmpl_' if fmt=='chat' else 'resp_' if fmt=='responses' else 'msg_')+secrets.token_hex(12)
        self.created=int(time.time()); self.include_usage=include_usage; self.started=False; self.ended=False
        self.tool_map={}; self.tool_args={}; self.text=''; self.finish='stop'; self.in_tokens=0; self.out_tokens=0
        self.block_index=0; self.active=None; self.output=[]; self.seq=0
        self.defer_anthropic=False; self.deferred=[]
    def _resp(self,status='in_progress'):
        return {'id':self.id,'object':'response','created_at':self.created,'status':status,'model':self.model,'output':list(self.output),'usage':{'input_tokens':self.in_tokens,'output_tokens':self.out_tokens,'total_tokens':self.in_tokens+self.out_tokens} if status in ('completed','incomplete') else None,'error':None,'incomplete_details':{'reason':'max_output_tokens'} if status=='incomplete' else None}
    def _event(self,typ,**kwargs):
        self.seq+=1; return sse({'type':typ,'sequence_number':self.seq,**kwargs},typ)
    def start(self):
        if self.started: return []
        self.started=True
        if self.fmt=='chat': return [sse({'id':self.id,'object':'chat.completion.chunk','created':self.created,'model':self.model,'choices':[{'index':0,'delta':{'role':'assistant','content':''},'finish_reason':None}]})]
        if self.fmt=='anthropic': return [sse({'type':'message_start','message':{'id':self.id,'type':'message','role':'assistant','model':self.model,'content':[],'stop_reason':None,'stop_sequence':None,'usage':{'input_tokens':0,'output_tokens':0}}},'message_start')]
        return [self._event('response.created',response=self._resp())]
    def _close_block(self):
        if self.active is None: return []
        kind,i=self.active; out=[]
        if self.fmt=='anthropic': out=[sse({'type':'content_block_stop','index':self.block_index-1},'content_block_stop')]
        elif self.fmt=='responses':
            if kind=='text':
                idx=self.text_item; item=self.output[idx]
                item['status']='completed'
                out=[self._event('response.output_text.done',item_id=item['id'],output_index=idx,content_index=0,text=item['content'][0]['text']),self._event('response.content_part.done',item_id=item['id'],output_index=idx,content_index=0,part=item['content'][0]),self._event('response.output_item.done',output_index=idx,item=item)]
            else:
                info=self.tool_map[i]; idx=info['output_index']; item=self.output[idx]
                item['status']='completed'
                out=[self._event('response.function_call_arguments.done',item_id=item['id'],output_index=idx,arguments=self.tool_args.get(i,''),name=item['name']),self._event('response.output_item.done',output_index=idx,item=item)]
        self.active=None; return out
    def _ensure_text(self):
        if self.active==('text',None): return []
        out=self._close_block(); self.active=('text',None)
        if self.fmt=='anthropic': out.append(sse({'type':'content_block_start','index':self.block_index,'content_block':{'type':'text','text':''}},'content_block_start')); self.block_index+=1
        elif self.fmt=='responses':
            idx=len(self.output); item={'type':'message','id':'msg_'+secrets.token_hex(12),'status':'in_progress','role':'assistant','content':[{'type':'output_text','text':'','annotations':[]}]}; self.output.append(item)
            self.text_item=idx
            out.extend([self._event('response.output_item.added',output_index=idx,item=item),self._event('response.content_part.added',item_id=item['id'],output_index=idx,content_index=0,part=item['content'][0])])
        return out
    def push(self,e):
        out=self.start(); t=e['type']
        if t=='usage': self.in_tokens=e.get('input',0); self.out_tokens=e.get('output',0); return out
        if t=='finish': self.finish=e.get('reason','stop'); return out
        if t=='text':
            if self.fmt=='anthropic' and self.defer_anthropic:
                chunk=e.get('text',''); self.text+=chunk
                if self.deferred and self.deferred[-1][0]=='text': self.deferred[-1]=('text',self.deferred[-1][1]+chunk)
                else: self.deferred.append(('text',chunk))
                return out
            out+=self._ensure_text(); chunk=e.get('text',''); self.text+=chunk
            if self.fmt=='chat': out.append(sse({'id':self.id,'object':'chat.completion.chunk','created':self.created,'model':self.model,'choices':[{'index':0,'delta':{'content':chunk},'finish_reason':None}]}))
            elif self.fmt=='anthropic': out.append(sse({'type':'content_block_delta','index':self.block_index-1,'delta':{'type':'text_delta','text':chunk}},'content_block_delta'))
            else:
                item=self.output[self.text_item]; item['content'][0]['text']+=chunk
                out.append(self._event('response.output_text.delta',item_id=item['id'],output_index=self.text_item,content_index=0,delta=chunk))
        elif t=='tool_start':
            if self.fmt=='anthropic':
                out+=self._close_block()
                self.defer_anthropic=True
                self.tool_map[e['index']]={'id':e['id'],'name':e['name']}
                self.tool_args[e['index']]=''
                self.deferred.append(('tool',e['index']))
                return out
            i=e['index']; info={'id':e['id'],'name':e['name'],'output_index':len(self.output)}; self.tool_map[i]=info; self.tool_args[i]=''
            out+=self._close_block(); self.active=('tool',i)
            if self.fmt=='chat': out.append(sse({'id':self.id,'object':'chat.completion.chunk','created':self.created,'model':self.model,'choices':[{'index':0,'delta':{'tool_calls':[{'index':i,'id':e['id'],'type':'function','function':{'name':e['name'],'arguments':''}}]},'finish_reason':None}]}))
            elif self.fmt=='anthropic': out.append(sse({'type':'content_block_start','index':self.block_index,'content_block':{'type':'tool_use','id':e['id'],'name':e['name'],'input':{}}},'content_block_start')); self.block_index+=1
            else:
                item={'type':'function_call','id':'fc_'+secrets.token_hex(12),'call_id':e['id'],'name':e['name'],'arguments':'','status':'in_progress'}; self.output.append(item)
                out.append(self._event('response.output_item.added',output_index=info['output_index'],item=item))
        elif t=='tool_delta':
            i=e['index']; chunk=e.get('delta',''); self.tool_args[i]=self.tool_args.get(i,'')+chunk
            if self.fmt=='anthropic': return out
            if self.fmt=='chat': out.append(sse({'id':self.id,'object':'chat.completion.chunk','created':self.created,'model':self.model,'choices':[{'index':0,'delta':{'tool_calls':[{'index':i,'function':{'arguments':chunk}}]},'finish_reason':None}]}))
            elif self.fmt=='anthropic': out.append(sse({'type':'content_block_delta','index':self.block_index-1,'delta':{'type':'input_json_delta','partial_json':chunk}},'content_block_delta'))
            else:
                idx=self.tool_map[i]['output_index']; self.output[idx]['arguments']+=chunk
                out.append(self._event('response.function_call_arguments.delta',item_id=self.output[idx]['id'],output_index=idx,delta=chunk))
        return out
    def end(self):
        if self.ended: return []
        self.ended=True; out=self.start()+self._close_block()
        if self.fmt=='chat':
            out.append(sse({'id':self.id,'object':'chat.completion.chunk','created':self.created,'model':self.model,'choices':[{'index':0,'delta':{},'finish_reason':self.finish}]}))
            if self.include_usage: out.append(sse({'id':self.id,'object':'chat.completion.chunk','created':self.created,'model':self.model,'choices':[],'usage':{'prompt_tokens':self.in_tokens,'completion_tokens':self.out_tokens,'total_tokens':self.in_tokens+self.out_tokens}}))
            out.append(b'data: [DONE]\n\n')
        elif self.fmt=='anthropic':
            for kind,value in self.deferred:
                index=self.block_index; self.block_index+=1
                if kind=='tool':
                    info=self.tool_map[value]; arguments=self.tool_args.get(value,'')
                    tool_input(arguments)
                    out.append(sse({'type':'content_block_start','index':index,'content_block':{'type':'tool_use','id':info['id'],'name':info['name'],'input':{}}},'content_block_start'))
                    if arguments: out.append(sse({'type':'content_block_delta','index':index,'delta':{'type':'input_json_delta','partial_json':arguments}},'content_block_delta'))
                else:
                    out.append(sse({'type':'content_block_start','index':index,'content_block':{'type':'text','text':''}},'content_block_start'))
                    if value: out.append(sse({'type':'content_block_delta','index':index,'delta':{'type':'text_delta','text':value}},'content_block_delta'))
                out.append(sse({'type':'content_block_stop','index':index},'content_block_stop'))
            out.extend([sse({'type':'message_delta','delta':{'stop_reason':'tool_use' if self.finish=='tool_calls' else ('max_tokens' if self.finish=='length' else 'end_turn'),'stop_sequence':None},'usage':{'input_tokens':self.in_tokens,'output_tokens':self.out_tokens}},'message_delta'),sse({'type':'message_stop'},'message_stop')])
        else:
            for item in self.output: item['status']='completed'
            status='incomplete' if self.finish=='length' else 'completed'
            out.append(self._event('response.'+status,response=self._resp(status)))
        return out

def parse_sse_buffer(buffer):
    """Return (complete event dicts, remainder). Handles CRLF and multiline data."""
    events=[]; buffer=buffer.replace(b'\r\n',b'\n')
    while b'\n\n' in buffer:
        frame,buffer=buffer.split(b'\n\n',1)
        data=b'\n'.join(line[5:].lstrip(b' ') for line in frame.split(b'\n') if line.startswith(b'data:'))
        if data and data!=b'[DONE]':
            try: events.append(json.loads(data))
            except ValueError: pass
    return events,buffer

def has_convertible_output(event):
    """Only start a converted stream after an event can produce downstream output."""
    typ=event.get('type')
    if event.get('choices'):
        return any((choice.get('delta') or {}).get('content') or (choice.get('delta') or {}).get('tool_calls') for choice in event['choices'])
    if typ=='content_block_start':
        block=event.get('content_block') or {}
        return block.get('type')=='tool_use' or (block.get('type')=='text' and bool(block.get('text')))
    if typ=='content_block_delta':
        delta=event.get('delta') or {}
        return delta.get('type')=='text_delta' and bool(delta.get('text'))
    if typ=='response.output_item.added': return (event.get('item') or {}).get('type')=='function_call'
    if typ=='response.output_text.delta': return bool(event.get('delta'))
    return False

def rewrite_sse_models(buffer, public_model):
    """Rewrite only protocol model fields while preserving unknown SSE events."""
    frames=[]; buffer=buffer.replace(b'\r\n',b'\n')
    while b'\n\n' in buffer:
        frame,buffer=buffer.split(b'\n\n',1)
        lines=frame.split(b'\n'); data=b'\n'.join(line[5:].lstrip(b' ') for line in lines if line.startswith(b'data:'))
        if data and data!=b'[DONE]':
            try:
                obj=json.loads(data)
                if isinstance(obj,dict):
                    if 'model' in obj: obj['model']=public_model
                    for k in ('message','response'):
                        if isinstance(obj.get(k),dict) and 'model' in obj[k]: obj[k]['model']=public_model
                    encoded=json.dumps(obj,ensure_ascii=False,separators=(',',':')).encode()
                    lines=[line for line in lines if not line.startswith(b'data:')]
                    lines.append(b'data: '+encoded)
            except ValueError: raise ValueError('上游 SSE 数据无效')
        frames.append(b'\n'.join(lines)+b'\n\n')
    return frames,buffer

def downstream_stream_error(fmt,public_model):
    error={'type':'api_error','message':'上游流中断'}
    if fmt=='anthropic': return [sse({'type':'error','error':error},'error')]
    if fmt=='responses': return [sse({'type':'response.failed','response':{'status':'failed','model':public_model,'error':error}},'response.failed')]
    return [sse({'error':error})]
