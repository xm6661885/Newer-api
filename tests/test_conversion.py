"""Cross-format behavior that cannot be represented by same-format passthrough."""
import unittest

from conversion import ConversionError, convert_request, convert_response
from streaming import DownstreamEvents, UpstreamEvents, has_convertible_output


class ConversionTests(unittest.TestCase):
    def test_anthropic_thinking_keeps_answer_and_cache_usage(self):
        upstream={
            'id':'msg_up','type':'message','content':[
                {'type':'thinking','thinking':'private','signature':'opaque'},
                {'type':'redacted_thinking','data':'opaque'},
                {'type':'text','text':'answer'},
            ],'stop_reason':'end_turn',
            'usage':{'input_tokens':3,'cache_read_input_tokens':4,'cache_creation_input_tokens':2,'output_tokens':6},
        }
        chat=convert_response('anthropic','chat',upstream,'public')
        self.assertEqual(chat['choices'][0]['message']['content'],'answer')
        self.assertEqual(chat['usage']['prompt_tokens'],9)
        self.assertEqual(chat['usage']['prompt_tokens_details']['cached_tokens'],4)
        self.assertEqual(chat['usage']['prompt_tokens_details']['cache_write_tokens'],2)
        response=convert_response('anthropic','responses',upstream,'public')
        self.assertEqual(response['output'][0]['content'][0]['text'],'answer')
        self.assertEqual(response['usage']['total_tokens'],15)
        self.assertEqual(response['model'],'public')

    def test_thinking_only_is_not_an_empty_success(self):
        upstream={'content':[{'type':'thinking','thinking':'private'}],'usage':{'input_tokens':1,'output_tokens':2}}
        for target in ('chat','responses'):
            with self.subTest(target=target),self.assertRaises(ConversionError):
                convert_response('anthropic',target,upstream,'public')
        with self.assertRaises(ConversionError):
            convert_response('responses','chat',{'output':[{'type':'reasoning','summary':[]}]},'public')

    def test_thinking_and_tool_call(self):
        upstream={'content':[{'type':'thinking','thinking':'private'}, {'type':'tool_use','id':'call_1','name':'weather','input':{'city':'Paris'}}], 'stop_reason':'tool_use'}
        chat=convert_response('anthropic','chat',upstream,'public')
        self.assertEqual(chat['choices'][0]['finish_reason'],'tool_calls')
        self.assertEqual(chat['choices'][0]['message']['tool_calls'][0]['function']['arguments'],'{"city":"Paris"}')

    def test_tool_arguments_are_never_replaced_with_empty_object(self):
        body={'model':'public','messages':[{'role':'assistant','content':None,'tool_calls':[{'id':'call_1','type':'function','function':{'name':'weather','arguments':'{bad'}}]}]}
        with self.assertRaises(ConversionError): convert_request('chat','anthropic',body)
        chat={'choices':[{'message':body['messages'][0],'finish_reason':'tool_calls'}]}
        with self.assertRaises(ConversionError): convert_response('chat','anthropic',chat,'public')

    def test_usage_from_chat_retains_cache_and_total(self):
        chat={'choices':[{'message':{'role':'assistant','content':'answer'},'finish_reason':'stop'}],
              'usage':{'prompt_tokens':9,'completion_tokens':6,'prompt_tokens_details':{'cached_tokens':4,'cache_write_tokens':2}}}
        ant=convert_response('chat','anthropic',chat,'public')
        self.assertEqual(ant['usage'],{'input_tokens':3,'output_tokens':6,'cache_read_input_tokens':4,'cache_creation_input_tokens':2})
        responses=convert_response('chat','responses',chat,'public')
        self.assertEqual(responses['usage']['total_tokens'],15)
        self.assertEqual(responses['usage']['input_tokens_details']['cached_tokens'],4)

    def test_text_tools_and_images_across_request_formats(self):
        chat={'model':'public','messages':[{'role':'user','content':[{'type':'text','text':'inspect'},{'type':'image_url','image_url':{'url':'data:image/png;base64,YQ=='}}]}]}
        ant=convert_request('chat','anthropic',chat)
        self.assertEqual(ant['messages'][0]['content'][1]['source']['data'],'YQ==')
        responses=convert_request('chat','responses',chat)
        self.assertEqual(responses['input'][0]['content'][1]['image_url'],'data:image/png;base64,YQ==')
        self.assertEqual(convert_request('anthropic','chat',ant)['messages'][0]['content'][1]['image_url']['url'],'data:image/png;base64,YQ==')
        self.assertEqual(convert_request('responses','chat',responses)['messages'][0]['content'][1]['image_url']['url'],'data:image/png;base64,YQ==')
        self.assertEqual(convert_request('anthropic','responses',ant)['input'][0]['content'][1]['type'],'input_image')
        self.assertEqual(convert_request('responses','anthropic',responses)['messages'][0]['content'][1]['type'],'image')
        chat['messages'][0]['content'][1]['image_url']['detail']='high'
        self.assertEqual(convert_request('chat','responses',chat)['input'][0]['content'][1]['detail'],'high')
        with self.assertRaises(ConversionError): convert_request('chat','anthropic',chat)

    def test_stream_thinking_waits_for_real_output(self):
        thinking={'type':'content_block_delta','index':0,'delta':{'type':'thinking_delta','thinking':'private'}}
        text={'type':'content_block_delta','index':1,'delta':{'type':'text_delta','text':'answer'}}
        self.assertFalse(has_convertible_output({'type':'content_block_start','content_block':{'type':'text','text':''}}))
        self.assertFalse(has_convertible_output(thinking))
        self.assertFalse(has_convertible_output({'type':'content_block_delta','delta':{'type':'signature_delta','signature':'opaque'}}))
        self.assertTrue(has_convertible_output(text))
        parser=UpstreamEvents('anthropic')
        self.assertEqual(parser.feed(thinking),[])
        emitter=DownstreamEvents('chat','public')
        wire=b''.join(part for item in parser.feed(text) for part in emitter.push(item))
        self.assertIn(b'answer',wire)
        self.assertNotIn(b'private',wire)
        self.assertIn(b'"model":"public"',wire)


if __name__=='__main__': unittest.main()
