"""Eight independent client processes, 100 unique one-in-flight robots total.

The request workload, timeouts, periodic global queries and controller match v5.
Additional timestamps observe original HTTP operations; no retry/barrier/delay.
"""
from pathlib import Path
import concurrent.futures,gzip,hashlib,http.client,json,multiprocessing,os,queue,shutil,threading,time

R=Path('/data/workspace/wz/warehouse-free-ports-20261029');E=R/'live-joint-evidence-v8'

class TracedConnection(http.client.HTTPConnection):
    def begin_trace(self):self.trace={'send_calls':[],'connect_start_ns':None,'connect_end_ns':None}
    def connect(self):
        self.trace['connect_start_ns']=time.perf_counter_ns()
        try:return super().connect()
        finally:self.trace['connect_end_ns']=time.perf_counter_ns()
    def send(self,data):
        start=time.perf_counter_ns()
        try:return super().send(data)
        finally:self.trace['send_calls'].append({'start_ns':start,'end_ns':time.perf_counter_ns(),
            'bytes':len(data) if isinstance(data,(str,bytes,bytearray,memoryview)) else None})

def request(con,method,path,body,ident=None):
    con.begin_trace();before=time.perf_counter_ns();status=None;value=None;error=None;raw=None;service=None;upstream=None
    phases={'request_start_ns':before};headers={'Content-Type':'application/json'}
    if ident:headers['X-Probe-ID']=ident
    try:
        con.request(method,path,body=None if body is None else json.dumps(body),headers=headers)
        phases['request_send_complete_ns']=time.perf_counter_ns()
        response=con.getresponse();phases['response_headers_complete_ns']=time.perf_counter_ns()
        status=response.status;service=response.getheader('X-Warehouse-Service-Ms');upstream=response.getheader('X-Warehouse-Upstream-Ms')
        phases['response_body_read_start_ns']=time.perf_counter_ns();data=response.read();phases['response_body_complete_ns']=time.perf_counter_ns()
        phases['response_bytes']=len(data);phases['json_parse_start_ns']=time.perf_counter_ns()
        raw=data.decode('utf-8');value=json.loads(raw);phases['json_parse_complete_ns']=time.perf_counter_ns()
    except Exception as exc:error=type(exc).__name__+': '+str(exc)
    after=time.perf_counter_ns();phases['client_end_ns']=after;phases.update(con.trace)
    calls=phases['send_calls']
    phases['headers_send_start_ns']=calls[0]['start_ns'] if calls else None
    phases['headers_send_complete_ns']=calls[0]['end_ns'] if calls else None
    phases['body_send_start_ns']=calls[1]['start_ns'] if len(calls)>1 else None
    phases['body_send_complete_ns']=calls[-1]['end_ns'] if len(calls)>1 else phases['headers_send_complete_ns']
    return {'request_id':ident,'request':body,'response':value,'status':status,'error':error,
            'raw_response_on_error':raw if error or status!=200 else None,'client_start_ns':before,'client_end_ns':after,
            'client_ms':(after-before)/1e6,'service_header_ms':service,'upstream_header_ms':upstream,'http_phases':phases,'client_pid':os.getpid()}

def client_process(index,clients,stop):
    rows=queue.SimpleQueue();begun=time.perf_counter_ns()
    def sink():
        with gzip.open(E/f'api-responses-client-{index}.jsonl.gz','xt',encoding='utf-8',compresslevel=1) as stream:
            while True:
                row=rows.get()
                if row is None:break
                stream.write(json.dumps(row,separators=(',',':'))+'\n')
    writer=threading.Thread(target=sink,daemon=True);writer.start()
    def worker(client):
        con=TracedConnection('127.0.0.1',14189,timeout=90);number=0
        while not stop.is_set():
            number+=1;ident=f'live19-c{client:03}-r{number:06}'
            body={'query_id':'global' if number%50==0 else 'self','robot_id':client+1}
            row=request(con,'POST','/api/knowledge/query',body,ident);rows.put(row)
            if row['error']:
                con.close();con=TracedConnection('127.0.0.1',14189,timeout=90)
        con.close();return number
    counts={}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(clients)) as pool:
            futures={client:pool.submit(worker,client) for client in clients}
            counts={client:future.result() for client,future in futures.items()}
    finally:
        rows.put(None);writer.join(120)
        (E/f'client-process-{index}.json').write_text(json.dumps({'index':index,'pid':os.getpid(),'clients':clients,
            'counts':counts,'begun_ns':begun,'ended_ns':time.perf_counter_ns()},indent=2))

def main():
    controller=[]
    def call(method,path,body=None,timeout=60):
        con=TracedConnection('127.0.0.1',14189,timeout=timeout)
        try:row=request(con,method,path,body);controller.append({'method':method,'path':path,'status':row['status'],
            'request':body,'http_phases':row['http_phases'],'error':row['error']})
        finally:con.close()
        if row['status']!=200 or row['error']:raise RuntimeError(str(row))
        return row['response']
    initial=call('GET','/api/snapshot');run=initial['runId'];assert initial['elapsed']==0 and initial['metadata']['motion_speed_mps']==.8
    def control(action,value=None):
        body={'action':action,'expected_run_id':run}
        if value is not None:body['value']=value
        return call('POST','/api/control',body,180)
    control('speed',120);ctx=multiprocessing.get_context('spawn');stop=ctx.Event();begun=time.perf_counter_ns();history=[];children=[];launched=None;failure=None
    try:
        for index in range(8):
            child=ctx.Process(target=client_process,args=(index,list(range(index,100,8)),stop));child.start();children.append(child)
        launched=time.perf_counter_ns();control('start');deadline=time.monotonic()+600
        while True:
            frame=call('GET','/api/snapshot');history.append({'observed_ns':time.perf_counter_ns(),'run_id':frame['runId'],
                'status':frame['status'],'elapsed':frame['elapsed'],'knowledge':frame['knowledge'],'metrics':frame['metrics']})
            if frame['status'] in ('complete','failed'):break
            if time.monotonic()>deadline:raise TimeoutError('owned live did not complete in bounded 600s')
            if any(child.exitcode is not None and child.exitcode!=0 for child in children):raise RuntimeError('load process exited nonzero')
            time.sleep(.25)
    except Exception as exc:failure=type(exc).__name__+': '+str(exc)
    finally:
        stop.set()
        for child in children:child.join(120)
        assert all(not child.is_alive() for child in children),'load process did not drain in 120s'
        final=call('GET','/api/snapshot');processes=[json.loads((E/f'client-process-{i}.json').read_text()) for i in range(8)]
        counts=[0]*100
        for process in processes:
            for client,count in process['counts'].items():counts[int(client)]=count
        # Concatenate gzip members without rewriting any original response line.
        with (E/'api-responses.jsonl.gz').open('xb') as combined:
            for index in range(8):
                with (E/f'api-responses-client-{index}.jsonl.gz').open('rb') as stream:shutil.copyfileobj(stream,combined)
        (E/'client-run.json').write_text(json.dumps({'run_id':run,'clients':100,'client_processes':8,'processes':processes,
            'begun_ns':begun,'launched_ns':launched,'ended_ns':time.perf_counter_ns(),'requests_per_client':counts,
            'observed_frames':history,'initial':initial,'final':final,'failure':failure,
            'process_exitcodes':[child.exitcode for child in children],
            'client_wrapper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},indent=2))
        (E/'controller-http.json').write_text(json.dumps(controller,indent=2))
    print(json.dumps({'run_id':run,'status':final['status'],'elapsed':final['elapsed'],'requests':sum(counts),'failure':failure,
        'process_exitcodes':[child.exitcode for child in children]}))
    return 0 if failure is None and all(child.exitcode==0 for child in children) else 2

if __name__=='__main__':raise SystemExit(main())
