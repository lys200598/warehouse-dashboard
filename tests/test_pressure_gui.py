from contextlib import redirect_stdout
import gzip
import io
import json
import math
from pathlib import Path
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler

from ops import quick_load as q
from ops.pressure_plan import normalize, requires_confirmation, FIELDS
from ops.pressure_gui import Manager, Server, reduce_records, atomic
from tests.test_quick_load import Handler
from http.server import ThreadingHTTPServer
from html.parser import HTMLParser


class AssetTests(unittest.TestCase):
    def test_dom_ids_unique_and_literal_selectors_exist(self):
        class Parser(HTMLParser):
            def handle_starttag(self,tag,attrs):
                value=dict(attrs).get('id')
                if value:ids.append(value)
        ids=[];Parser().feed((q.ROOT/'dist/pressure-lab/index.html').read_text())
        self.assertEqual(len(ids),len(set(ids)))
        import re
        js=(q.ROOT/'dist/pressure-lab/app.js').read_text()
        selectors=re.findall(r"\$\('([^']+)'\)",js)
        self.assertTrue(set(selectors)<=set(ids),set(selectors)-set(ids))


class PlanTests(unittest.TestCase):
    def test_presets_and_custom_budget(self):
        self.assertFalse(requires_confirmation(normalize('quick')))
        self.assertTrue(requires_confirmation(normalize('quick',{'clients':2})))
        plan=normalize('normal',{'clients':4,'processes':2,'duration':5})
        self.assertEqual(plan['processes'],2)

    def test_reject_invalid_and_unbounded(self):
        cases=[{'clients':True},{'duration':math.nan},{'processes':101},{'clients':1,'processes':2},
               {'command':'echo bad'},{'query_id':'write'},{'requests':0},
               {'mode':'open','rate':1000,'duration':300},
               {'mode':'periodic','clients':100,'processes':100,'interval':.1,'duration':300},
               {'clients':100,'requests':10000}]
        for value in cases:
            with self.subTest(value=value),self.assertRaises(ValueError):normalize('quick',value)
        with self.assertRaises(ValueError):normalize('quick',[])

    def test_mixed_case_weights_and_fixed_cutoff(self):
        plan=normalize('quick',{'query_id':'mixed','self_percent':75})
        cfg=q.load_config(q.ROOT/'config/pressure-test.json')
        rows=q.workload(cfg,'quick','fixture',11,plan)['cases']
        self.assertEqual([r['weight'] for r in rows],[75,25])
        self.assertEqual({r['body']['seq'] for r in rows},{11})
        self.assertTrue(all(r['kind']=='read' for r in rows))

    def test_identical_values_do_not_claim_custom(self):
        plan=normalize('quick');again=normalize('quick',{k:plan[k] for k in FIELDS})
        self.assertEqual(again['label'],plan['label'])


class GuiTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=q.ROOT/'tests',prefix='tmp-gui-')
        self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.manager=Manager(self.root/'jobs',q.ROOT/'config/pressure-test.json')
        self.addCleanup(self.manager.close)
        class Fixture(ThreadingHTTPServer):request_queue_size=256
        self.target=Fixture(('127.0.0.1',0),Handler)
        self.target.paths=[];self.target.requests=[]
        self.target.graph=True;self.target.bad_shape=False;self.target.fail_after_preflight=False
        threading.Thread(target=self.target.serve_forever,daemon=True).start()
        self.addCleanup(self.target.server_close);self.addCleanup(self.target.shutdown)
        self.url=f'http://127.0.0.1:{self.target.server_port}'
        self.gui=Server(('127.0.0.1',0),self.manager)
        threading.Thread(target=self.gui.serve_forever,daemon=True).start()
        self.addCleanup(self.gui.server_close);self.addCleanup(self.gui.shutdown)
        self.base=f'http://127.0.0.1:{self.gui.server_port}'
        self.opener=build_opener(ProxyHandler({}))

    def request(self,path,body=None,token=True,origin=None,host=None):
        headers={}
        if body is not None:headers.update({'Content-Type':'application/json'})
        if token:headers['X-Pressure-Token']=self.gui.token
        if origin:headers['Origin']=origin
        if host:headers['Host']=host
        request=Request(self.base+path,data=None if body is None else json.dumps(body).encode(),headers=headers)
        try:response=self.opener.open(request,timeout=30)
        except HTTPError as exc:response=exc
        with response:
            raw=response.read();status=response.status
        try:return status,json.loads(raw)
        except ValueError:return status,raw

    def wait(self,ident):
        deadline=time.monotonic()+35
        while time.monotonic()<deadline:
            row=self.manager.view(ident)
            if row['state'] not in ('running','cancelling'):return row
            time.sleep(.05)
        self.fail('owned GUI experiment did not finish')

    def test_bootstrap_static_assets_and_host_guard(self):
        status,value=self.request('/api/bootstrap')
        self.assertEqual(status,200);self.assertEqual(value['token'],self.gui.token)
        for path in ['/','/app.js','/style.css']:self.assertEqual(self.request(path)[0],200)
        self.assertEqual(self.request('/api/bootstrap',host='evil.example')[0],403)

    def test_restart_after_close_cannot_take_over_live_listener(self):
        port=self.gui.server_port
        with self.assertRaises(OSError):Server(('127.0.0.1',port),self.manager)
        self.request('/api/bootstrap')
        self.gui.shutdown();self.gui.server_close()
        restarted=Server(('127.0.0.1',port),self.manager)
        restarted.server_close()

    def test_csrf_origin_and_unknown_fields(self):
        self.assertEqual(self.request('/api/check',{'url':self.url},token=False)[0],403)
        self.assertEqual(self.request('/api/check',{'url':self.url},origin='http://evil.example')[0],403)
        self.assertEqual(self.request('/api/check',{'url':self.url,'extra':1})[0],400)
        self.assertEqual(self.request('/api/check',{'url':self.url})[0],200)
        self.assertEqual(self.target.requests,[])

    def test_high_load_and_credential_url_rejected(self):
        body={'url':self.url,'profile':'normal','confirmed':False}
        self.assertEqual(self.request('/api/jobs',body)[0],400)
        self.assertEqual(self.request('/api/jobs',{'url':'http://secret:secret@localhost:5'})[0],400)
        self.assertEqual(self.manager.jobs,{})

    def test_complete_job_charts_pagination_download(self):
        status,result=self.request('/api/jobs',{'url':self.url,'profile':'quick'})
        self.assertEqual(status,201);ident=result['id'];row=self.wait(ident)
        self.assertIn(row['state'],('complete','target_not_met'))
        self.assertEqual(row['summary']['outcomes'],{'ok':3})
        self.assertEqual(len(self.target.requests),4)
        self.assertFalse(row['summary']['million_prefix_qualified'])
        data=self.request(f'/api/jobs/{ident}/charts')[1]
        self.assertEqual(sum(data['histogram']),3)
        self.assertEqual(data['outcomes'],{'ok':3})
        requests=self.request(f'/api/jobs/{ident}/requests?offset=0&limit=2')[1]
        self.assertEqual(requests['total'],3);self.assertEqual(len(requests['rows']),2)
        self.assertEqual(self.request(f'/api/jobs/{ident}/download?file=report.md')[0],200)
        self.assertEqual(self.request(f'/api/jobs/{ident}/download?file=requests.csv')[0],200)
        self.assertEqual(self.request(f'/api/jobs/{ident}/download?file=experiment.json')[0],200)
        self.assertEqual(self.request(f'/api/jobs/{ident}/download?file=../../etc/passwd')[0],400)
        self.assertEqual(self.request(f'/api/jobs/{ident}/requests?limit=999999')[0],400)

    def test_custom_mixed_actual_parameters(self):
        body={'url':self.url,'profile':'quick','parameters':{'query_id':'mixed','requests':6,'duration':10},'confirmed':True}
        status,data=self.request('/api/jobs',body);self.assertEqual(status,201)
        row=self.wait(data['id']);self.assertEqual(row['summary']['plan']['requests'],6)
        self.assertEqual(row['summary']['outcomes'],{'ok':6})
        self.assertEqual(len(self.target.requests),8) # One preflight per kind.

    def test_gui_cancel_and_concurrent_admission(self):
        status,data=self.request('/api/jobs',{'url':self.url,'profile':'quick',
            'parameters':{'clients':2,'processes':2,'mode':'periodic','requests':0,'duration':20,'interval':1},'confirmed':True})
        self.assertEqual(status,201);ident=data['id']
        self.assertEqual(self.request('/api/jobs',{'url':self.url})[0],400)
        deadline=time.monotonic()+20
        while len(self.target.requests)<3 and time.monotonic()<deadline:time.sleep(.02)
        self.assertEqual(self.request(f'/api/jobs/{ident}/cancel',{})[0],200)
        self.assertEqual(self.request(f'/api/jobs/{ident}/cancel',{})[0],200)
        row=self.wait(ident);self.assertEqual(row['state'],'interrupted')
        self.assertFalse(row.get('summary',{}).get('all_requests_under_500ms',False))

    def test_restart_marks_old_running_as_orphan_not_success(self):
        ident='job-'+'a'*32;folder=self.manager.root/ident;folder.mkdir()
        atomic(folder/'job.json',{'id':ident,'state':'running','started_unix':0,'plan':normalize('quick')})
        restored=Manager(self.manager.root,self.manager.config)
        self.assertEqual(restored.jobs[ident]['state'],'orphaned')
        self.assertIsNone(restored.active)

    def test_reducer_keeps_failed_and_dropped_denominator(self):
        folder=self.root/'raw';(folder/'client').mkdir(parents=True)
        with gzip.open(folder/'client/client-0001.jsonl.gz','wt') as out:
            for outcome,value in [('ok',5),('http_error',600),('load_generator_busy',None)]:
                out.write(json.dumps({'outcome':outcome,'elapsed_ms':value,'start_ns':1000000000,
                                     'end_ns':2000000000 if value is not None else None})+'\n')
        result=reduce_records(folder)
        self.assertEqual(len(result['rows']),3);self.assertEqual(sum(result['histogram']),2)
        self.assertEqual(result['outcomes'],{'ok':1,'http_error':1,'load_generator_busy':1})
        self.assertEqual(result['histogram'][7],1)


if __name__=='__main__':unittest.main()
