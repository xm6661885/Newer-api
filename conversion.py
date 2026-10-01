"""Conservative mappings among Chat Completions, Responses, and Messages.
Same-format traffic never uses this module. Unsupported cross-format structures raise
ConversionError instead of silently discarding client intent.
"""
import json, secrets, time

class ConversionError(ValueError): pass

def dumped(x): return x if isinstance(x,str) else json.dumps(x,ensure_ascii=False,separators=(',',':'))

def tool_input(x):
    """Anthropic tool inputs must be JSON objects; never replace malformed arguments."""
    if isinstance(x,dict): return x
    if not isinstance(x,str): raise ConversionError('工具参数必须是 JSON 对象')
    if not x: return {}
    try: value=json.loads(x)
    except ValueError: raise ConversionError('工具参数不是有效 JSON') from None
    if not isinstance(value,dict): raise ConversionError('工具参数必须是 JSON 对象')
    return value

def chat_usage(usage):
    numbers=usage_numbers(usage)
    result={'prompt_tokens':numbers['input'],'completion_tokens':numbers['output'],'total_tokens':numbers['input']+numbers['output']}
    details={}
    if numbers['cache_read']: details['cached_tokens']=numbers['cache_read']
    if numbers['cache_write']: details['cache_write_tokens']=numbers['cache_write']
    if details: result['prompt_tokens_details']=details
    return result

def usage_numbers(usage):
    """Normalize Chat/Responses/Messages/media usage to total input, cache read, cache write and output tokens."""
    def num(v): return int(v) if isinstance(v,(int,float)) and not isinstance(v,bool) and v>0 else 0
    u=usage if isinstance(usage,dict) else {}
    if 'prompt_tokens' in u or 'completion_tokens' in u:
        details=u.get('prompt_tokens_details') if isinstance(u.get('prompt_tokens_details'),dict) else {}
        total=num(u.get('prompt_tokens')); output=num(u.get('completion_tokens'))
        read=num(details.get('cached_tokens')) or num(u.get('prompt_cache_hit_tokens'))
        write=num(details.get('cache_write_tokens')) or num(details.get('cache_creation_tokens'))
    elif 'cache_read_input_tokens' in u or 'cache_creation_input_tokens' in u:
        # Anthropic input_tokens excludes cached and cache-creation tokens.
        read=num(u.get('cache_read_input_tokens')); write=num(u.get('cache_creation_input_tokens'))
        total=num(u.get('input_tokens'))+read+write; output=num(u.get('output_tokens'))
    else:
        details=u.get('input_tokens_details') if isinstance(u.get('input_tokens_details'),dict) else {}
        total=num(u.get('input_tokens')); output=num(u.get('output_tokens'))
        read=num(details.get('cached_tokens')); write=num(details.get('cache_write_tokens')) or num(details.get('cache_creation_tokens'))
    read=min(read,total); write=min(write,total-read)
    return {'input':total,'output':output,'cache_read':read,'cache_write':write}

def require_supported(body, allowed):
    ignored=[k for k,v in body.items() if k not in allowed and v is not None]
    if ignored: raise ConversionError('跨格式暂不支持参数：'+', '.join(ignored))

def chat_parts(content):
    if content is None: return []
    if isinstance(content,str): return [{'type':'text','text':content}]
    if not isinstance(content,list): raise ConversionError('无法转换此消息内容')
    result=[]
    for p in content:
        if not isinstance(p,dict): raise ConversionError('消息内容块无效')
        t=p.get('type')
        if t in ('text','input_text','output_text'): result.append({'type':'text','text':p.get('text','')})
        elif t in ('image_url','input_image'):
            url=p.get('image_url',p.get('url'))
            detail=p.get('detail')
            if isinstance(url,dict): detail=url.get('detail',detail); url=url.get('url')
            if not isinstance(url,str): raise ConversionError('图片 URL 无效')
            result.append({'type':'image','url':url,'detail':detail})
        else: raise ConversionError(f'跨格式暂不支持内容块 {t}')
    return result

def anthropic_parts(content):
    if isinstance(content,str): return [{'type':'text','text':content}]
    out=[]
    for p in content or []:
        t=p.get('type')
        if t=='text': out.append({'type':'text','text':p.get('text','')})
        elif t=='image':
            source=p.get('source') or {}
            if source.get('type')=='base64': url=f"data:{source.get('media_type','image/png')};base64,{source.get('data','')}"
            elif source.get('type')=='url': url=source.get('url')
            else: raise ConversionError('此 Anthropic 图片来源无法跨格式转换')
            out.append({'type':'image','url':url})
        elif t in ('tool_use','tool_result','thinking','redacted_thinking'): out.append(p)
        else: raise ConversionError(f'跨格式暂不支持 Anthropic 内容块 {t}')
    return out

def to_chat(source,body):
    if source=='chat': return dict(body)
    if source=='anthropic':
        require_supported(body,{'model','messages','system','max_tokens','temperature','top_p','stop_sequences','stream','tools','tool_choice'})
        out={k:body[k] for k in ('model','temperature','top_p','stop_sequences','stream','metadata') if k in body and k not in ('stop_sequences','metadata')}
        if 'stop_sequences' in body: out['stop']=body['stop_sequences']
        out['messages']=[]
        if body.get('system'):
            sys=body['system']
            if isinstance(sys,list):
                if any(p.get('type')!='text' for p in sys): raise ConversionError('系统消息包含无法转换的内容')
                sys='\n'.join(p.get('text','') for p in sys)
            out['messages'].append({'role':'system','content':sys})
        for m in body.get('messages',[]):
            role=m.get('role'); parts=anthropic_parts(m.get('content',[])); current=[]
            def flush():
                if current:
                    out['messages'].append({'role':role,'content':current[0]['text'] if len(current)==1 and current[0]['type']=='text' else list(current)})
                    current.clear()
            for p in parts:
                if p['type'] in ('text','image'):
                    current.append({'type':'text','text':p['text']} if p['type']=='text' else {'type':'image_url','image_url':{'url':p['url']}})
                elif p['type']=='tool_use':
                    flush(); out['messages'].append({'role':'assistant','content':None,'tool_calls':[{'id':p['id'],'type':'function','function':{'name':p['name'],'arguments':dumped(p.get('input',{}))}}]})
                elif p['type']=='tool_result':
                    flush(); val=p.get('content','')
                    if isinstance(val,list):
                        if any(q.get('type')!='text' for q in val): raise ConversionError('工具结果包含无法转换的内容')
                        val='\n'.join(q.get('text','') for q in val)
                    out['messages'].append({'role':'tool','tool_call_id':p['tool_use_id'],'content':val})
                else: raise ConversionError('思考内容只能在 Anthropic 同格式透传')
            flush()
        if 'max_tokens' in body: out['max_tokens']=body['max_tokens']
        if body.get('tools'):
            out['tools']=[{'type':'function','function':{'name':t['name'],'description':t.get('description',''),'parameters':t.get('input_schema',{'type':'object','properties':{}})}} for t in body['tools'] if t.get('type') in (None,'custom')]
            if len(out['tools'])!=len(body['tools']): raise ConversionError('服务端工具只能同格式透传')
        choice=body.get('tool_choice')
        if choice:
            if not isinstance(choice,dict): raise ConversionError('工具选择方式无法转换')
            if choice.get('type') not in ('auto','any','none','tool'): raise ConversionError('工具选择方式无法转换')
            out['tool_choice']={'auto':'auto','any':'required','none':'none','tool':lambda:{'type':'function','function':{'name':choice['name']}}}[choice['type']]
            if callable(out['tool_choice']): out['tool_choice']=out['tool_choice']()
        return out
    if source=='responses':
        require_supported(body,{'model','input','instructions','temperature','top_p','stream','metadata','max_output_tokens','tools','tool_choice'})
        if body.get('previous_response_id') or body.get('conversation') or body.get('prompt'):
            raise ConversionError('有状态 Responses 请求只能发送到 Responses 渠道')
        out={k:body[k] for k in ('model','temperature','top_p','stream','metadata') if k in body}
        if 'max_output_tokens' in body: out['max_tokens']=body['max_output_tokens']
        out['messages']=[]
        if body.get('instructions'): out['messages'].append({'role':'system','content':body['instructions']})
        items=body.get('input',[])
        if isinstance(items,str): items=[{'role':'user','content':items}]
        for item in items:
            t=item.get('type','message')
            if t in ('message','easy_input_message'):
                content=item.get('content',[])
                if isinstance(content,str): value=content
                else:
                    parts=chat_parts(content)
                    value=[{'type':'text','text':p['text']} if p['type']=='text' else {'type':'image_url','image_url':{'url':p['url']}} for p in parts]
                out['messages'].append({'role':item['role'],'content':value})
            elif t=='function_call': out['messages'].append({'role':'assistant','content':None,'tool_calls':[{'id':item['call_id'],'type':'function','function':{'name':item['name'],'arguments':item.get('arguments','{}')}}]})
            elif t=='function_call_output': out['messages'].append({'role':'tool','tool_call_id':item['call_id'],'content':dumped(item.get('output',''))})
            else: raise ConversionError(f'跨格式暂不支持 Responses 输入 {t}')
        if body.get('tools'):
            out['tools']=[]
            for t in body['tools']:
                if t.get('type')!='function': raise ConversionError('托管工具只能在 Responses 渠道使用')
                out['tools'].append({'type':'function','function':{'name':t['name'],'description':t.get('description',''),'parameters':t.get('parameters',{})}})
        if 'tool_choice' in body:
            c=body['tool_choice']; out['tool_choice']={'type':'function','function':{'name':c['name']}} if isinstance(c,dict) and c.get('type')=='function' else c
        return out
    raise ConversionError('未知接口格式')

def from_chat(target,body):
    if target=='chat': return dict(body)
    if target=='anthropic':
        require_supported(body,{'model','messages','max_tokens','max_completion_tokens','temperature','top_p','stop','stream','tools','tool_choice','stream_options'})
        # Common SDK transport options are not all semantically portable.
        if body.get('response_format') or body.get('modalities') or body.get('audio'):
            raise ConversionError('此 Chat 参数只能发送到 Chat 渠道')
        out={k:body[k] for k in ('model','temperature','top_p','stream','metadata') if k in body}
        out['max_tokens']=body.get('max_tokens',body.get('max_completion_tokens',4096))
        if 'stop' in body: out['stop_sequences']=body['stop'] if isinstance(body['stop'],list) else [body['stop']]
        system=[]; messages=[]
        for m in body.get('messages',[]):
            role=m.get('role')
            if role in ('system','developer'):
                if isinstance(m.get('content'),str): system.append(m['content'])
                else:
                    system_parts=chat_parts(m.get('content'))
                    if any(p['type']!='text' for p in system_parts): raise ConversionError('系统消息包含无法转换的图片')
                    system.extend(p['text'] for p in system_parts)
                continue
            if role=='tool':
                block={'type':'tool_result','tool_use_id':m['tool_call_id'],'content':m.get('content','')}
                if messages and messages[-1]['role']=='user' and isinstance(messages[-1]['content'],list) and all(q.get('type')=='tool_result' for q in messages[-1]['content']): messages[-1]['content'].append(block)
                else: messages.append({'role':'user','content':[block]})
                continue
            parts=chat_parts(m.get('content'))
            blocks=[]
            for p in parts:
                if p['type']=='text': blocks.append(p)
                else:
                    if p.get('detail') not in (None,'auto'): raise ConversionError('图片 detail 无法转换为 Anthropic')
                    url=p['url']
                    if url.startswith('data:'):
                        meta,data=url[5:].split(';base64,',1)
                        source={'type':'base64','media_type':meta,'data':data}
                    else: source={'type':'url','url':url}
                    blocks.append({'type':'image','source':source})
            for t in m.get('tool_calls') or []:
                if t.get('type')!='function': raise ConversionError('仅函数工具可跨格式转换')
                blocks.append({'type':'tool_use','id':t['id'],'name':t['function']['name'],'input':tool_input(t['function'].get('arguments','{}'))})
            messages.append({'role':role,'content':blocks})
        if system: out['system']='\n'.join(system)
        out['messages']=messages
        if body.get('tools'):
            out['tools']=[]
            for t in body['tools']:
                if t.get('type')!='function': raise ConversionError('仅函数工具可跨格式转换')
                f=t['function']; out['tools'].append({'name':f['name'],'description':f.get('description',''),'input_schema':f.get('parameters',{'type':'object','properties':{}})})
        choice=body.get('tool_choice')
        if choice:
            if isinstance(choice,str):
                if choice not in ('auto','required','none'): raise ConversionError('工具选择方式无法转换')
                out['tool_choice']={'auto':{'type':'auto'},'required':{'type':'any'},'none':{'type':'none'}}[choice]
            elif isinstance(choice,dict) and choice.get('type')=='function': out['tool_choice']={'type':'tool','name':choice['function']['name']}
            else: raise ConversionError('工具选择方式无法转换')
        return out
    if target=='responses':
        require_supported(body,{'model','messages','max_tokens','max_completion_tokens','temperature','top_p','stream','tools','tool_choice','metadata','stream_options','reasoning_effort'})
        if body.get('response_format') or body.get('audio') or body.get('modalities'):
            raise ConversionError('此 Chat 参数无法转换为 Responses')
        out={k:body[k] for k in ('model','temperature','top_p','stream','metadata') if k in body}
        if body.get('reasoning_effort'): out['reasoning']={'effort':body['reasoning_effort']}
        if 'max_tokens' in body or 'max_completion_tokens' in body: out['max_output_tokens']=body.get('max_completion_tokens',body.get('max_tokens'))
        out['input']=[]
        for m in body.get('messages',[]):
            role=m['role']
            if role=='tool': out['input'].append({'type':'function_call_output','call_id':m['tool_call_id'],'output':m.get('content','')}); continue
            parts=chat_parts(m.get('content'))
            content=[]
            for p in parts:
                content.append({'type':'input_text' if role!='assistant' else 'output_text','text':p['text']} if p['type']=='text' else {'type':'input_image','image_url':p['url'], **({'detail':p['detail']} if p.get('detail') else {})})
            if content: out['input'].append({'role':role,'content':content})
            for t in m.get('tool_calls') or []:
                if t.get('type')!='function': raise ConversionError('仅函数工具可跨格式转换')
                out['input'].append({'type':'function_call','call_id':t['id'],'name':t['function']['name'],'arguments':t['function'].get('arguments','{}')})
        if body.get('tools'):
            out['tools']=[]
            for t in body['tools']:
                if t.get('type')!='function': raise ConversionError('仅函数工具可跨格式转换')
                f=t['function']; out['tools'].append({'type':'function','name':f['name'],'description':f.get('description',''),'parameters':f.get('parameters',{'type':'object','properties':{}}),'strict':f.get('strict',False)})
        if 'tool_choice' in body:
            c=body['tool_choice']; out['tool_choice']={'type':'function','name':c['function']['name']} if isinstance(c,dict) and c.get('type')=='function' else c
        return out
    raise ConversionError('未知接口格式')

def convert_request(source,target,body):
    if source==target: return dict(body)
    return from_chat(target,to_chat(source,body))

def response_to_chat(source,data,public_model):
    if source=='chat':
        out=dict(data); out['model']=public_model; return out
    if source=='anthropic':
        content=[]; calls=[]
        for p in data.get('content',[]):
            if p.get('type')=='text': content.append(p.get('text',''))
            elif p.get('type')=='tool_use': calls.append({'id':p['id'],'type':'function','function':{'name':p['name'],'arguments':dumped(p.get('input',{}))}})
            # Signed or opaque thinking has no standard Chat field; keep the answer and tool calls.
            elif p.get('type') in ('thinking','redacted_thinking'): continue
            else: raise ConversionError(f'无法转换上游内容 {p.get("type")}')
        if not any(content) and not calls: raise ConversionError('Anthropic 响应没有可转换的正文或工具调用')
        reason=data.get('stop_reason')
        return {'id':data.get('id','chatcmpl_'+secrets.token_hex(12)),'object':'chat.completion','created':int(time.time()),'model':public_model,'choices':[{'index':0,'message':{'role':'assistant','content':''.join(content) or None,**({'tool_calls':calls} if calls else {})},'finish_reason':'length' if reason=='max_tokens' else ('tool_calls' if calls else 'stop')}],'usage':chat_usage(data.get('usage'))}
    if source=='responses':
        content=[]; calls=[]; refusal=None
        for item in data.get('output',[]):
            if item.get('type')=='message':
                for p in item.get('content',[]):
                    if p.get('type')=='output_text': content.append(p.get('text',''))
                    elif p.get('type')=='refusal': refusal=p.get('refusal','')
                    else: raise ConversionError(f'无法转换 Responses 内容块 {p.get("type")}')
            elif item.get('type')=='function_call': calls.append({'id':item['call_id'],'type':'function','function':{'name':item['name'],'arguments':item.get('arguments','{}')}})
            elif item.get('type')=='reasoning': continue
            else: raise ConversionError(f'无法转换 Responses 输出 {item.get("type")}')
        if not any(content) and not calls and refusal is None: raise ConversionError('Responses 响应没有可转换的正文或工具调用')
        incomplete=(data.get('incomplete_details') or {}).get('reason')
        return {'id':data.get('id','chatcmpl_'+secrets.token_hex(12)),'object':'chat.completion','created':int(time.time()),'model':public_model,'choices':[{'index':0,'message':{'role':'assistant','content':''.join(content) or None,**({'refusal':refusal} if refusal is not None else {}),**({'tool_calls':calls} if calls else {})},'finish_reason':'length' if incomplete else ('tool_calls' if calls else 'stop')}],'usage':chat_usage(data.get('usage'))}
    raise ConversionError('未知输出格式')

def chat_to_response(target,chat,public_model):
    if target=='chat': return chat
    msg=(chat.get('choices') or [{}])[0].get('message') or {}; usage=chat.get('usage') or {}
    if not msg.get('content') and not msg.get('tool_calls') and msg.get('refusal') is None:
        raise ConversionError('Chat 响应没有可转换的正文或工具调用')
    if target=='anthropic':
        if msg.get('refusal') is not None: raise ConversionError('无法将 Chat 拒绝内容转换为 Anthropic 内容块')
        if msg.get('content') is not None and not isinstance(msg['content'],str): raise ConversionError('无法转换 Chat 非文本输出内容')
        blocks=[]
        if msg.get('content'): blocks.append({'type':'text','text':msg['content']})
        for t in msg.get('tool_calls') or []: blocks.append({'type':'tool_use','id':t['id'],'name':t['function']['name'],'input':tool_input(t['function'].get('arguments','{}'))})
        reason=(chat.get('choices') or [{}])[0].get('finish_reason')
        numbers=usage_numbers(usage)
        anthropic_usage={'input_tokens':numbers['input']-numbers['cache_read']-numbers['cache_write'],'output_tokens':numbers['output']}
        if numbers['cache_read']: anthropic_usage['cache_read_input_tokens']=numbers['cache_read']
        if numbers['cache_write']: anthropic_usage['cache_creation_input_tokens']=numbers['cache_write']
        return {'id':chat.get('id','msg_'+secrets.token_hex(12)),'type':'message','role':'assistant','model':public_model,'content':blocks,'stop_reason':'max_tokens' if reason=='length' else ('tool_use' if reason=='tool_calls' else 'end_turn'),'stop_sequence':None,'usage':anthropic_usage}
    if target=='responses':
        if msg.get('content') is not None and not isinstance(msg['content'],str): raise ConversionError('无法转换 Chat 非文本输出内容')
        output=[]
        blocks=[]
        if msg.get('content'): blocks.append({'type':'output_text','text':msg['content'],'annotations':[]})
        if msg.get('refusal') is not None: blocks.append({'type':'refusal','refusal':msg['refusal']})
        if blocks: output.append({'type':'message','id':'msg_'+secrets.token_hex(12),'status':'completed','role':'assistant','content':blocks})
        for t in msg.get('tool_calls') or []: output.append({'type':'function_call','id':'fc_'+secrets.token_hex(12),'call_id':t['id'],'name':t['function']['name'],'arguments':t['function'].get('arguments','{}'),'status':'completed'})
        incomplete=(chat.get('choices') or [{}])[0].get('finish_reason')=='length'
        numbers=usage_numbers(usage)
        response_usage={'input_tokens':numbers['input'],'output_tokens':numbers['output'],'total_tokens':numbers['input']+numbers['output']}
        if numbers['cache_read']: response_usage['input_tokens_details']={'cached_tokens':numbers['cache_read']}
        return {'id':chat.get('id','resp_'+secrets.token_hex(12)),'object':'response','created_at':chat.get('created',int(time.time())),'status':'incomplete' if incomplete else 'completed','model':public_model,'output':output,'usage':response_usage,'error':None,'incomplete_details':{'reason':'max_output_tokens'} if incomplete else None}
    raise ConversionError('未知输出格式')

def convert_response(source,target,data,public_model):
    if source==target:
        out=dict(data); out['model']=public_model; return out
    return chat_to_response(target,response_to_chat(source,data,public_model),public_model)
