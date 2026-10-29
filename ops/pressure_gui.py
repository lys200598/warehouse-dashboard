"""Loopback-only load experiment controller, history and offline chart reducer.

All measurements are delegated to the original harness via quick_load; this
server never controls the warehouse or writes Neo4j. One owned experiment at a
time, bounded input, CSRF/Host checks, no arbitrary command/file endpoints.
"""
from collections import Counter, OrderedDict
from datetime import datetime, timezone
import csv
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import json
import io
import math
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit, parse_qs

from . import quick_load as q
from .pressure_plan import normalize, requires_confirmation, PROFILES, QUERIES, FIELDS

ASSETS = q.ROOT/'dist/pressure-lab'
JOB_ID = re.compile(r'^job-[0-9a-f]{32}$')
DOWNLOADS = {'report.md','summary.json','analysis.json','workload.json','preflight.json',
             'requests.csv','experiment.json'}


def read_json(path, fallback=None):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return fallback


def atomic(path, data):
    pending = path.with_suffix('.pending')
    pending.write_text(json.dumps(data, ensure_ascii=False, allow_nan=False, indent=2), encoding='utf-8')
    os.replace(pending, path)


def reduce_records(folder):
    """Only sealed files after process exit. Keep all failures/drops in totals."""
    rows, buckets, histogram, outcomes = [], {}, [0]*10, Counter()
    bounds = [10,25,50,100,200,300,500,1000,5000]
    zero = None
    for file in sorted((folder/'client').glob('client-*.jsonl.gz')):
        for raw in q.harness.records(file):
            start, end = raw.get('start_ns'), raw.get('end_ns')
            if type(start) is int:
                zero = start if zero is None else min(zero, start)
            rows.append({k: raw.get(k) for k in ('request_id','client_id','query_class','status',
                'outcome','elapsed_ms','end_to_end_json_ms','queue_ms','service_ms','start_ns','end_ns')})
    for row in rows:
        outcomes[row['outcome']] += 1
        value = row['end_to_end_json_ms']
        if not isinstance(value,(int,float)) or not math.isfinite(value):
            value = row['elapsed_ms']
        row['chart_ms'] = value if type(value) in (int,float) and math.isfinite(value) else None
        if row['chart_ms'] is not None:
            histogram[next((i for i,b in enumerate(bounds) if value <= b),9)] += 1
        if zero is not None and type(row['end_ns']) is int:
            second = max(0,(row['end_ns']-zero)//1000000000)
            b = buckets.setdefault(second, {'second':second,'completed':0,'errors':0,'sum_ms':0.,'max_ms':0.,'timed':0})
            b['completed'] += 1
            b['errors'] += row['outcome'] != 'ok'
            if row['chart_ms'] is not None:
                b['sum_ms'] += value; b['max_ms']=max(b['max_ms'],value);b['timed'] += 1
    timeline = [{**b, 'mean_ms': b['sum_ms']/b['timed'] if b['timed'] else None}
                for _,b in sorted(buckets.items())]
    return {'rows':rows,'timeline':timeline,'histogram':histogram,'outcomes':dict(outcomes),
            'histogram_labels':['≤10','10–25','25–50','50–100','100–200','200–300','300–500','500–1000','1–5s','>5s'],
            'scope':'all saved records; drops without timing counted in outcomes, not latency bins'}


class Manager:
    def __init__(self, root, config, default_url=None):
        self.root = Path(root).resolve(); self.root.mkdir(parents=True, exist_ok=True)
        self.config = Path(config).resolve()
        self.cfg = q.load_config(self.config)
        self.default_url = default_url or self.cfg['base_url']
        q.harness.origin(self.default_url)
        self.lock = threading.RLock(); self.active = None; self.jobs = {}; self.cache = OrderedDict()
        self.closing = False
        for path in sorted(self.root.glob('job-*/job.json'),key=lambda p:p.stat().st_mtime,reverse=True)[:200]:
            if not JOB_ID.fullmatch(path.parent.name):
                continue
            row = read_json(path)
            if not isinstance(row,dict) or row.get('id') != path.parent.name:
                continue
            if row.get('state') in ('running','cancelling'):
                row.update(state='orphaned', error='GUI上次退出，未据PID猜测或重启旧任务；检查原件')
            self.jobs[row['id']] = row

    def directory(self, ident):
        if not JOB_ID.fullmatch(ident) or ident not in self.jobs:
            raise ValueError('未知实验编号')
        directory = (self.root/ident).resolve()
        if directory.parent != self.root:
            raise ValueError('实验路径越界')
        return directory

    def check(self, url):
        q.harness.origin(url); url = url.rstrip('/')
        health = q.fetch_json(url,self.cfg['health_path'])
        run, seq = q.point_from_health(health,self.cfg)
        catalog = q.pick(q.fetch_json(url,self.cfg['catalog_path']),self.cfg['catalog_queries_path'])
        if not isinstance(catalog,dict):
            raise ValueError('查询目录格式不兼容')
        return {'url':url,'run_id':run,'seq':seq,'queries':[x for x in QUERIES if x in catalog],
                'running':health.get('runtime',{}).get('running'),'scope':'health/catalog only; no load sent'}

    def start(self, body):
        if set(body)-{'url','profile','parameters','confirmed'}:
            raise ValueError('含未知启动字段')
        plan = normalize(body.get('profile','quick'),body.get('parameters'))
        url = body.get('url',self.default_url); q.harness.origin(url)
        if requires_confirmation(plan) and body.get('confirmed') is not True:
            raise ValueError('请勾选确认对自有测试服务施加负载')
        with self.lock:
            if self.closing or self.active is not None:
                raise ValueError('已有实验运行/控制台正在关闭；请等待它结束')
            ident = 'job-'+secrets.token_hex(16)
            directory = self.root/ident; directory.mkdir()
            parameters = {k:plan[k] for k in sorted(FIELDS)}
            # Only bounded numeric/enumerated parameters, never an arbitrary command.
            atomic(directory/'parameters.json', parameters)
            row = {'id':ident,'created_at':datetime.now(timezone.utc).isoformat(),
                   'started_unix':time.time(),'state':'running','url':url,'plan':plan,'cancel_requested':False}
            self.jobs[ident] = row; self.active = ident
            atomic(directory/'job.json',row)
            out = (directory/'console.log').open('xb')
            command = [sys.executable,str(q.ROOT/'pressure_test.py'),'--url',url,'--profile',plan['profile'],
                       '--plan-json',str(directory/'parameters.json'),'--output',str(directory/'output'),'--yes',
                       '--config',str(self.config)]
            try:
                process = subprocess.Popen(command,cwd=q.ROOT,stdout=out,stderr=subprocess.STDOUT,
                    env={**{k:v for k,v in os.environ.items() if not k.startswith('NEO4J_')},
                         'PYTHONUNBUFFERED':'1','PYTHONIOENCODING':'utf-8'},
                    start_new_session=os.name=='posix',
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name=='nt' else 0)
            except Exception as exc:
                out.close();self.active=None;row.update(state='failed',error=str(exc))
                atomic(directory/'job.json',row);raise
            row['_process'] = process
            threading.Thread(target=self._watch,args=(ident,process,out),daemon=True).start()
            return ident

    def _watch(self, ident, process, log):
        code = process.wait();log.close()
        with self.lock:
            row = self.jobs[ident];row.pop('_process',None)
            summary = read_json(self.directory(ident)/'output/summary.json', {})
            row.update(exit_code=code,ended_unix=time.time(),state='interrupted' if row['cancel_requested'] else
                       'complete' if code==0 else 'target_not_met' if code==2 else 'failed')
            if not summary or summary.get('status') != 'measured':
                row['error'] = summary.get('error','工具未生成完整测量摘要')
                if row['state']=='complete':row['state']='failed'
            atomic(self.directory(ident)/'job.json',row)
            self.active = None

    def cancel(self, ident):
        with self.lock:
            self.directory(ident);row=self.jobs[ident]
            process=row.get('_process')
            if process is None or process.poll() is not None or row['cancel_requested']:
                return
            row.update(state='cancelling',cancel_requested=True)
            # Signal the owned parent only. Its existing harness drains workers.
            process.send_signal(signal.SIGINT if os.name=='posix' else signal.CTRL_BREAK_EVENT)

    def view(self, ident):
        with self.lock:
            directory=self.directory(ident);row={k:v for k,v in self.jobs[ident].items() if k!='_process'}
        output=directory/'output'
        row['output_directory']=str(output)
        row['elapsed_seconds']=max(0,(row.get('ended_unix') or time.time())-row['started_unix'])
        row['summary']=read_json(output/'summary.json')
        row['stage']=('分析完成' if row['summary'] else '离线分析' if (output/'analyze.log').exists()
                      else '请求发起/等待客户端排空' if (output/'client').exists() else '连通性与输入检查')
        if row['state']=='orphaned':
            row['stage']='未确认完成';row['summary']=None
        log=directory/'console.log'
        if log.exists():
            with log.open('rb') as stream:
                stream.seek(max(0,log.stat().st_size-16000));row['console']=stream.read().decode('utf-8','replace')
        else:row['console']=''
        return row

    def charts(self, ident):
        with self.lock:
            directory=self.directory(ident)
            if self.jobs[ident]['state'] in ('running','cancelling'):
                return {'ready':False,'reason':'未解压正在写入的gzip；等待工具排空后生成全量图表'}
            if ident in self.cache:
                self.cache.move_to_end(ident);return self.cache[ident]
            if self.active is not None:
                return {'ready':False,'reason':'有实验正在测量，暂缓旧记录解压以减少对发起机的干扰'}
        try:
            reduced=reduce_records(directory/'output');reduced['ready']=True
        except (OSError,ValueError,EOFError) as exc:
            return {'ready':False,'reason':'原始记录未完整封口：'+str(exc)}
        with self.lock:
            self.cache[ident]=reduced
            while len(self.cache)>3:self.cache.popitem(last=False)
        return reduced

    def close(self):
        with self.lock:
            self.closing=True;ident=self.active
            process=self.jobs[ident].get('_process') if ident else None
        if process and process.poll() is None:
            self.cancel(ident)
            try:process.wait(45)
            except subprocess.TimeoutExpired:
                if os.name=='posix':os.killpg(process.pid,signal.SIGTERM)
                else:subprocess.run(['taskkill','/PID',str(process.pid),'/T','/F'],capture_output=True)
                process.wait(10)


class Server(ThreadingHTTPServer):
    daemon_threads=True
    # Permit a clean restart after our listener closes despite TCP TIME_WAIT;
    # without SO_REUSEPORT this still cannot take over a live listener.
    allow_reuse_address=True

    def __init__(self, address, manager):
        super().__init__(address, Handler)
        self.manager=manager;self.token=secrets.token_urlsafe(32)


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):
        pass

    def send(self,value,status=200,kind='application/json; charset=utf-8',attachment=None):
        data=json.dumps(value,ensure_ascii=False,allow_nan=False).encode() if kind.startswith('application/json') else value
        self.send_response(status);self.send_header('Content-Type',kind)
        self.send_header('Content-Length',str(len(data)));self.send_header('Cache-Control','no-store')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
        if attachment:self.send_header('Content-Disposition','attachment; filename="'+attachment+'"')
        self.end_headers();self.wfile.write(data)

    def host_ok(self):
        return self.headers.get('Host') in (f'127.0.0.1:{self.server.server_port}',f'localhost:{self.server.server_port}')

    def do_GET(self):
        if not self.host_ok():return self.send({'error':'Host不匹配'},403)
        try:
            path=urlsplit(self.path).path;manager=self.server.manager
            if path=='/api/bootstrap':
                return self.send({'token':self.server.token,'url':manager.default_url,
                    'presets':{name:normalize(name) for name in PROFILES},'queries':list(QUERIES),
                    'python':sys.version.split()[0],'platform':sys.platform,'cpu_count':os.cpu_count(),
                    'scope':'此进程所在设备发起请求；含其网络传输成本'})
            if path=='/api/jobs':
                with manager.lock:ids=sorted(manager.jobs,key=lambda k:manager.jobs[k]['started_unix'],reverse=True)[:100]
                return self.send({'jobs':[manager.view(i) for i in ids]})
            match=re.fullmatch(r'/api/jobs/(job-[0-9a-f]{32})(?:/(charts|requests|download))?',path)
            if match:
                ident,action=match.groups();params=parse_qs(urlsplit(self.path).query)
                if not action:return self.send(manager.view(ident))
                if action=='download':
                    name=params.get('file',[''])[0]
                    if name not in DOWNLOADS:raise ValueError('不允许下载这个文件')
                    if name=='requests.csv':
                        data=manager.charts(ident)
                        if not data['ready']:raise ValueError(data['reason'])
                        stream=io.StringIO(newline='')
                        writer=csv.DictWriter(stream,fieldnames=['request_id','client_id','query_class','status',
                            'outcome','chart_ms','elapsed_ms','queue_ms','service_ms'],extrasaction='ignore')
                        writer.writeheader();writer.writerows(data['rows'])
                        return self.send(stream.getvalue().encode('utf-8-sig'),kind='application/octet-stream',attachment=name)
                    directory=manager.directory(ident)
                    f=(directory/'job.json' if name=='experiment.json' else directory/'output'/name).resolve()
                    if not f.is_relative_to(directory):raise ValueError('下载路径越界')
                    return self.send(f.read_bytes(),kind='application/octet-stream',attachment=name)
                data=manager.charts(ident)
                if not data['ready']:return self.send(data)
                if action=='charts':return self.send({k:v for k,v in data.items() if k!='rows'})
                offset=int(params.get('offset',['0'])[0]);limit=int(params.get('limit',['30'])[0])
                if not 0<=offset<=100000 or not 1<=limit<=100:raise ValueError('分页参数无效')
                rows=data['rows'];order=params.get('order',['slow'])[0]
                if order=='slow':rows=sorted(rows,key=lambda x:x.get('chart_ms') or -1,reverse=True)
                if params.get('errors',['0'])[0]=='1':rows=[r for r in rows if r['outcome']!='ok']
                return self.send({'ready':True,'total':len(rows),'rows':rows[offset:offset+limit]})
            asset={'/':'index.html','/app.js':'app.js','/style.css':'style.css'}.get(path)
            if asset:
                kind={'index.html':'text/html','app.js':'text/javascript','style.css':'text/css'}[asset]
                return self.send((ASSETS/asset).read_bytes(),kind=kind+'; charset=utf-8')
            self.send({'error':'不存在的路径'},404)
        except (ValueError,OSError,KeyError) as exc:self.send({'error':str(exc)},400)

    def do_POST(self):
        origin=self.headers.get('Origin')
        if (not self.host_ok() or (origin and origin not in
             (f'http://127.0.0.1:{self.server.server_port}',f'http://localhost:{self.server.server_port}'))
                or not hmac.compare_digest(self.headers.get('X-Pressure-Token',''),self.server.token)):
            return self.send({'error':'请求来源或控制令牌无效'},403)
        try:
            if self.headers.get('Content-Type','').split(';')[0]!='application/json':raise ValueError('需要JSON请求')
            length=int(self.headers.get('Content-Length','0'))
            if not 0<length<=32768:raise ValueError('请求大小无效')
            def invalid(value):raise ValueError('不接受非有限数值')
            body=json.loads(self.rfile.read(length),parse_constant=invalid)
            if not isinstance(body,dict):raise ValueError('需要JSON对象')
            manager=self.server.manager;path=urlsplit(self.path).path
            if path=='/api/check':
                if set(body)!={'url'}:raise ValueError('只接受服务地址')
                return self.send(manager.check(body['url']))
            if path=='/api/jobs':return self.send({'id':manager.start(body)},201)
            match=re.fullmatch(r'/api/jobs/(job-[0-9a-f]{32})/cancel',path)
            if match:
                if body:raise ValueError('停止请求不接受附加参数')
                manager.cancel(match[1]);return self.send({'ok':True})
            self.send({'error':'不存在的操作'},404)
        except (ValueError,OSError,KeyError,TypeError) as exc:self.send({'error':str(exc)},400)


def serve(port,root,config,url=None):
    if not 1024<=port<=65535:raise ValueError('GUI端口应在1024—65535之间')
    manager=Manager(root,config,url)
    server=Server(('127.0.0.1',port),manager)
    print(f'压测控制台：http://127.0.0.1:{port}/\n记录目录：{manager.root}\nCtrl+C关闭控制台并停止其自有测试。',flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:manager.close();server.server_close()
    return 0
