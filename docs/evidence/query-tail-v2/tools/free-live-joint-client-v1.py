"""100 persistent client threads, full-response recording, no request oracle."""
from pathlib import Path
import concurrent.futures,gzip,http.client,json,queue,threading,time,hashlib,urllib.request

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');E=R/'live-joint-evidence-v1'
BASE='http://127.0.0.1:14189'
def get(path):return json.load(urllib.request.urlopen(BASE+path,timeout=60))
def control(action,value=None):
    body={'action':action,'expected_run_id':run}
    if value is not None:body['value']=value
    req=urllib.request.Request(BASE+'/api/control',data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    return json.load(urllib.request.urlopen(req,timeout=180))
initial=get('/api/snapshot');run=initial['runId'];assert initial['elapsed']==0 and initial['metadata']['motion_speed_mps']==.8
control('speed',120)
rows=queue.SimpleQueue();stop=threading.Event();started=time.perf_counter_ns();counts=[];history=[];launched=None
def sink():
    with gzip.open(E/'api-responses.jsonl.gz','xt',encoding='utf-8',compresslevel=1) as stream:
        while True:
            row=rows.get()
            if row is None:break
            stream.write(json.dumps(row,separators=(',',':'))+'\n')
writer=threading.Thread(target=sink,daemon=True);writer.start()
def worker(client):
    con=http.client.HTTPConnection('127.0.0.1',14189,timeout=90);number=0
    while not stop.is_set():
        number+=1;ident=f'live19-c{client:03}-r{number:06}'
        request={'query_id':'global' if number%50==0 else 'self','robot_id':client+1}
        before=time.perf_counter_ns();status=None;value=None;error=None;service=None;raw=None
        try:
            con.request('POST','/api/knowledge/query',body=json.dumps(request),headers={'Content-Type':'application/json','X-Probe-ID':ident})
            response=con.getresponse();status=response.status;service=response.getheader('X-Warehouse-Service-Ms')
            raw=response.read().decode('utf-8');value=json.loads(raw)
        except Exception as exc:
            error=type(exc).__name__+': '+str(exc);con.close();con=http.client.HTTPConnection('127.0.0.1',14189,timeout=90)
        after=time.perf_counter_ns()
        rows.put({'request_id':ident,'request':request,'response':value,'status':status,'error':error,
            'raw_response_on_error':raw if error or status!=200 else None,'client_start_ns':before,'client_end_ns':after,
            'client_ms':(after-before)/1e6,'service_header_ms':service})
    con.close();return number
try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as pool:
        futures=[pool.submit(worker,i) for i in range(100)]
        try:
            launched=time.perf_counter_ns();control('start')
            deadline=time.monotonic()+600
            while True:
                frame=get('/api/snapshot');history.append({'observed_ns':time.perf_counter_ns(),'run_id':frame['runId'],
                    'status':frame['status'],'elapsed':frame['elapsed'],'knowledge':frame['knowledge'],'metrics':frame['metrics']})
                if frame['status'] in ('complete','failed'):break
                if time.monotonic()>deadline:raise TimeoutError('owned live did not complete in bounded 600s')
                time.sleep(.25)
        finally:stop.set()
        counts=[f.result() for f in futures]
finally:
    stop.set();rows.put(None);writer.join(120)
    final=get('/api/snapshot')
    (E/'client-run.json').write_text(json.dumps({'run_id':run,'clients':100,'begun_ns':started,'launched_ns':launched,
        'ended_ns':time.perf_counter_ns(),'requests_per_client':counts,'observed_frames':history,
        'initial':initial,'final':final,'client_wrapper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},indent=2))
print(json.dumps({'run_id':run,'status':final['status'],'elapsed':final['elapsed'],'knowledge':final['knowledge'],'requests':sum(counts)}))
