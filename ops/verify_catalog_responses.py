"""Post-query source verification of saved warehouse catalog responses.

Expected values are reconstructed from the original journal/RealOracle. This
tool does not import QueryCatalogService, its Cypher, or its aggregation helpers.
It never connects to Neo4j and never runs within a measured query interval.
"""
import argparse
from collections import Counter, defaultdict
from contextlib import closing
from copy import deepcopy
from functools import lru_cache
import gzip
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import zlib

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def digest(value):return hashlib.sha256(canonical(value)).hexdigest()


def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def loads(raw):
    def pairs(values):
        d={}
        for k,v in values:
            if k in d:raise ValueError('duplicate JSON field: '+k)
            d[k]=v
        return d
    def invalid(value):raise ValueError('nonfinite JSON: '+value)
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)


def read_lines(path):
    opener=gzip.open if str(path).endswith('.gz') else open
    with opener(path,'rt',encoding='utf-8') as f:
        for line,raw in enumerate(f,1):
            if not raw.strip():continue
            try:yield line,loads(raw),None
            except Exception as exc:yield line,None,type(exc).__name__+': '+str(exc)


def require(value,message):
    if not value:raise ValueError(message)


def integer(value,low=1,high=None):
    require(type(value) is int and value>=low and (high is None or value<=high),'invalid integer')
    return value


def compare(actual,expected,path='$',errors=None):
    """Strict source fields; tolerate only explicitly calculated score rounding."""
    errors=[] if errors is None else errors
    if isinstance(expected,Approx):
        if type(actual) is not float or not math.isfinite(actual) or abs(actual-expected.value)>expected.tolerance:
            errors.append(path+': floating score mismatch')
    elif type(actual) is not type(expected):errors.append(path+': type mismatch '+type(actual).__name__+'/'+type(expected).__name__)
    elif isinstance(expected,dict):
        if set(actual)!=set(expected):errors.append(path+': fields differ missing='+str(sorted(set(expected)-set(actual)))+' extra='+str(sorted(set(actual)-set(expected))))
        for k in expected:
            if k in actual:compare(actual[k],expected[k],path+'.'+k,errors)
    elif isinstance(expected,list):
        if len(actual)!=len(expected):errors.append(path+': list length differs')
        for i,(a,b) in enumerate(zip(actual,expected)):compare(a,b,path+'['+str(i)+']',errors)
    elif actual!=expected:errors.append(path+': value mismatch actual='+repr(actual)[:180]+' expected='+repr(expected)[:180])
    return errors


class Approx:
    def __init__(self,value,tolerance=1e-12):self.value,self.tolerance=value,tolerance


class JournalTruth:
    def __init__(self,journal,oracle):
        self.journal=Path(journal).resolve();self.oracle=oracle
        self.db=sqlite3.connect(self.journal.as_uri()+'?mode=ro',uri=True)
        self.db.execute('BEGIN')
        heads=self.db.execute('SELECT run_id,seq FROM heads').fetchall()
        require(len(heads)==1,'one original run required')
        self.run_id,self.head=heads[0]
        identity=oracle.identity
        require(identity['run_id']==self.run_id,'oracle run differs from source')
        self.metadata=identity['metadata'];self.config=identity['config'];self.identity=identity
        first=self.batch(1)
        require(digest(first['metadata'])==digest(self.metadata) and digest(first['config'])==digest(self.config),'oracle immutable source identity differs')
        # Compare every indexed source checksum, not merely a metadata claim.
        indexed=oracle._db().execute('SELECT seq,checksum FROM batches ORDER BY seq').fetchall()
        original=self.db.execute('SELECT seq,checksum FROM commits WHERE run_id=? AND seq<=? ORDER BY seq',
            (self.run_id,oracle.prefix)).fetchall()
        require(indexed==original and len(indexed)==oracle.prefix,'oracle/source prefix checksum chain differs')

    @lru_cache(maxsize=32)
    def batch(self,seq):
        integer(seq,1,self.head)
        row=self.db.execute('SELECT checksum,batch FROM commits WHERE run_id=? AND seq=?',(self.run_id,seq)).fetchone()
        require(row is not None,'original source batch absent')
        raw=zlib.decompress(row[1]);body=loads(raw)
        require(hashlib.sha256(raw).hexdigest()==row[0] and digest(body)==row[0],'original source batch checksum mismatch')
        require(body['run_id']==self.run_id and type(body['seq']) is int and body['seq']==seq,'original batch identity differs')
        require(self.oracle.batch_checksum(seq)==row[0],'oracle selected batch differs from source')
        return body

    def state(self,seq,robot):
        state=self.oracle.state(seq,robot)
        raw=next((s for s in self.batch(seq)['robots'] if s['robot_id']==robot),None)
        require(raw is not None and digest(raw)==digest(state),'oracle state differs from original source payload')
        return state

    def task(self,seq,task):return self.oracle.task(seq,task)

    def all_tasks(self,seq):
        rows=self.oracle._db().execute('''SELECT a.payload FROM tasks a WHERE a.seq<=?
            AND a.seq=(SELECT max(b.seq) FROM tasks b WHERE b.task_id=a.task_id AND b.seq<=?)''',(seq,seq))
        return [loads(r[0]) for r in rows]

    def events(self,start,end,robot=None,task=None):
        rows=self.oracle._db().execute('SELECT seq,event_seq,payload FROM events WHERE seq>=? AND seq<=? ORDER BY event_seq',(start,end))
        result=[]
        for seq,number,payload in rows:
            event=loads(payload)
            if robot is not None and event.get('robot_id')!=robot:continue
            if task is not None and event.get('task_id')!=task:continue
            result.append({'seq':seq,'event_seq':number,'event':event})
        return result

    def goods(self,seq,goods,head):
        value=self.oracle.goods_history(seq,goods,limit=1000)
        value['graph_committed_seq']=head
        # SQLite REAL values lose JSON numeric type. The original event JSON
        # provides its exact typed effective time for each custody observation.
        for row in value['rows']:
            e=self.oracle._db().execute('SELECT payload FROM events WHERE event_seq=?',(row['event_seq'],)).fetchone()
            require(e is not None,'custody source event absent')
            row['sim_time']=loads(e[0])['sim_time']
        if value['rows']:
            last=value['rows'][-1]
            value['current_as_of']={k:last[k] for k in ('location_id','event_seq','sim_time')}
        return value

    def cooperation(self,seq,task,head):
        value=self.oracle.cooperation(seq,task,limit=1000);value['graph_committed_seq']=head
        return value

    def close(self):self.db.close()


def region(p,scene):
    closest=min(scene['racks'],key=lambda r:(math.hypot(p['x']-r['x']-r['w']/2,p['y']-r['y']-r['d']/2),r['id']))
    return closest['zone']


def feedback(events):
    """Independent event replay: preserve unknown window endpoints."""
    groups=defaultdict(list)
    for row in events:
        if row['event'].get('task_id'):groups[row['event']['task_id']].append(row['event'])
    result=[]
    for task in sorted(groups):
        released=None;assignment=None;pickup=None;observations=[]
        for event in groups[task]:
            kind=event['kind']
            if kind=='task_released' and released is None:released=event['sim_time']
            elif kind=='task_assigned':assignment=event['sim_time']
            elif kind=='picked':
                pickup=event['sim_time']
                if assignment is not None:observations.append({'robot_id':event.get('robot_id'),'assignment_to_pickup_seconds':pickup-assignment})
            elif kind in ('placed','shipped'):
                result.append({'task_id':task,'completion_kind':kind,'completed_at':event['sim_time'],'robot_id':event.get('robot_id'),
                    'release_to_completion_seconds':None if released is None else event['sim_time']-released,
                    'pickup_to_completion_seconds':None if pickup is None else event['sim_time']-pickup,
                    'assignment_pickup_observations':deepcopy(observations),
                    'release_observed_in_window':released is not None,'pickup_observed_in_window':pickup is not None})
    return result


FAMILIES=('idle','transport','handling','charging','returning','assistance','fault','unknown')
PHASES=[{'idle','standby','mission_queue','queued'},{'to_source','to_target','moving'},
    {'picking','placing','docking','undocking'},{'charging','charge_queue'},{'returning','to_charge'},
    {'to_handoff','handoff','handoff_receive','handoff_give'},{'fault_wait','faulted','recovery_wait','yielding','blocked'}]


def phase(stage):
    return next((FAMILIES[i] for i,s in enumerate(PHASES) if stage in s),'unknown')


def vector17(p,width,depth):
    x,y,b,load=p['x']/width,p['y']/depth,p['battery']/100,float(p['carrying'])
    values=[1.,x,1-x,y,1-y,b,1-b,load,1-load]+[float(phase(p['stage'])==s) for s in FAMILIES]
    norm=math.sqrt(sum(x*x for x in values))
    return [x/norm for x in values]


class ResponseVerifier:
    def __init__(self,truth):
        self.t=truth;self.created_at=None

    def head(self,value,seq):
        integer(value,seq,self.t.head)
        return value

    def point(self,actual,seq):
        stamp=actual.get('run_created_at')
        require(isinstance(stamp,str) and stamp,'run creation version absent')
        if self.created_at is None:self.created_at=stamp
        require(stamp==self.created_at,'run creation version changes across responses')
        return {'batch_checksum':self.t.oracle.batch_checksum(seq),'run_created_at':stamp,
            'graph_committed_seq':self.head(actual.get('graph_committed_seq'),seq),'sim_time':self.t.batch(seq)['sim_time']}

    def bay_states(self,seq):
        batch=self.t.batch(seq);result=[]
        for d in sorted(self.t.metadata['scene']['docks'],key=lambda d:d['id']):
            x,y=d['point']['x'],d['point']['y']
            bodies=[r for r in batch['robots'] if (r['x']-x)**2+(r['y']-y)**2<=1e-10]
            require(len(bodies)<=1,'source physical bay over capacity')
            charging=[r for r in bodies if r['stage']=='charging']
            result.append({'run_id':self.t.run_id,'seq':seq,'charger_id':d['id'],'physical_bay_id':d['id'],'capacity':1,
                'reserved_robot_ids':[r['robot_id'] for r in batch['robots'] if r.get('dock_id')==d['id']],
                'occupant_robot_id':bodies[0]['robot_id'] if bodies else None,'occupant_robot_ids':[r['robot_id'] for r in bodies],
                'charging_robot_id':charging[0]['robot_id'] if charging else None,'occupant_stage':bodies[0]['stage'] if bodies else None})
        return result

    def context(self,request,actual):
        seq=request['seq'];run=self.t.run_id;robot=request.get('robot_id',1)
        head=self.head(actual.get('graph_committed_seq'),seq)
        task=request['task_id'];current=self.t.task(seq,task)
        require(current is not None,'source task absent')
        goods=request.get('goods_id',current['goods_id']);require(goods==current['goods_id'],'source task/cargo mismatch')
        return {'run_id':run,'seq':seq,'graph_committed_seq':head,'batch_checksum':self.t.oracle.batch_checksum(seq),
            'task_id':task,'goods_id':goods,'focus_task':current,'request_cursors':{'goods_after':0,'cooperation_after':0},
            'goods_history':self.t.goods(seq,goods,head),'cooperation':self.t.cooperation(seq,task,head)}

    def business(self,request,actual):
        seq=request['seq'];robot=request.get('robot_id',1);run=self.t.run_id
        head=self.head(actual.get('graph_committed_seq'),seq);query=self.t.state(seq,robot)
        window=request.get('window_steps',60);embargo=request.get('embargo_steps',20);budget=request.get('max_candidates',10000);k=request.get('limit',10)
        start,end=max(1,seq-window),seq-embargo;require(end>=1,'business query lacks prior history')
        geometry=self.t.metadata['scene']['spec'];width,depth=geometry['width'],geometry['depth']
        qv=vector17(query,width,depth);rows=[]
        for at in range(start,end+1):
            for peer in range(1,101):
                if peer==robot:continue
                state=self.t.state(at,peer);v=vector17(state,width,depth)
                score=max(-1.,min(1.,math.fsum(a*b for a,b in zip(qv,v))))
                rows.append({'key':f'{run}:{at}:{peer}','run_id':run,'seq':at,'robot_id':peer,
                    **{key:state[key] for key in ('x','y','battery','carrying','stage')},
                    'source_payload_sha256':digest(state),'score':score,'phase_family':phase(state['stage'])})
        require(len(rows)<=budget,'source candidate domain exceeds declared budget')
        selected=sorted(rows,key=lambda r:(-r['score'],r['seq'],r['robot_id']))[:k]
        for row in selected:row['score']=Approx(row['score'])
        stages=actual.get('stage_ms');require(isinstance(stages,dict),'retrieval timing stages absent')
        require(set(stages)=={'header_and_query_features','candidate_graph_read','validated_exact_ranking'},'retrieval timing stage fields differ')
        for value in [*stages.values(),actual.get('service_ms')]:
            require(type(value) in (int,float) and math.isfinite(value) and value>=0,'invalid retrieval timing')
        return {'run_id':run,'seq':seq,'graph_committed_seq':head,'batch_checksum':self.t.oracle.batch_checksum(seq),
            'robot_id':robot,'query_source_payload_sha256':digest(query),'feature_version':'warehouse-business-state-v1',
            'method':'prefiltered-exact-cosine','dimensions':17,'feature_config':{'width':width,'depth':depth},
            'domain':{'start_seq':start,'end_seq':end,'window_steps':window,'embargo_steps':embargo,
                'exclude_query_robot':True,'max_candidates':budget,'candidate_count':len(rows),'complete_domain':True},
            'requested_k':k,'returned_k':len(selected),'rows':selected,
            'scope':'advisory structural-state similarity; not ownership authorization or predicted task outcome',
            'stage_ms':stages,'service_ms':actual['service_ms']}

    def expected(self,request,actual):
        q=request['query_id'];seq=integer(request['seq'],1,self.t.oracle.prefix);run=request['run_id'];require(run==self.t.run_id,'foreign requested source run')
        robot=request.get('robot_id',1);integer(robot,1,100)
        start=request.get('start_seq',max(1,seq-60));integer(start,1,seq)
        body=actual['result'];scene=self.t.metadata['scene'];batch=self.t.batch(seq)
        if q in ('self','readiness'):
            c=body['context'];head=self.head(c.get('graph_committed_seq'),seq);state=self.t.state(seq,robot)
            expected={'context':{'run_id':run,'seq':seq,'graph_committed_seq':head,'batch_checksum':self.t.oracle.batch_checksum(seq),
                'robots':[{'robot_id':robot,'robot':state,'robot_sha256':digest(state),'task':self.t.task(seq,state.get('task_id'))}]},
                'readiness':{'requested_point_present':True,'future_point_used':False,'known_committed_lag_steps':head-seq,
                    'ready_for_live_action':False,'note':'historical read; live coordinator must recheck current ownership/reservations'}}
        elif q in ('task','goods'):
            if q=='goods' and request.get('task_id') is None:
                goods=request['goods_id'];history=self.t.goods(seq,goods,seq)['rows']
                expected={'goods_id':goods,'history':history,'found':bool(history),'current_as_of':history[-1] if history else None,
                    'batch_checksum':self.t.oracle.batch_checksum(seq),'cooperation_scope':'supply task_id to request its cooperation context'}
            else:expected={'context':self.context(request,body['context'])}
        elif q=='similar_history':
            variant=request.get('variant','business-exact')
            require(variant=='business-exact','full-ann must use the separately frozen query_v2 oracle/quality verifier; not business certification')
            expected={'variant':variant,'context':self.business(request,body['context']),'control_role':'advisory; cannot authorize cargo transfer'}
        else:
            expected={'point':self.point(body['point'],seq)}
            robots=[self.t.state(seq,i) for i in range(1,101)];me=robots[robot-1]
            events=[]
            if q in ('assistance','progress','memory','local_awareness','global'):
                events=self.t.events(start,seq,robot if q=='memory' else None,request.get('task_id') if q=='progress' else None)
                require(len(events)<=request.get('event_limit',2000),'source event window exceeds response budget')
            if q=='energy':
                bays=self.bay_states(seq)
                expected.update(robot=me,thresholds={k:self.t.config.get(k) for k in
                    ('energy_profile','task_reserve_soc','emergency_reserve_soc','charge_request_soc','charge_target_soc')},
                    bays=bays,empty_physical_bays=sum(not b['occupant_robot_ids'] for b in bays),
                    planning_required=['route length and reservation','loaded/empty energy cost','assigned-bay policy'],
                    availability_scope='observed bodies/reservations at cutoff; no route or future charging admission claim')
            elif q=='assistance':
                peers=[]
                for peer in robots:
                    distance=math.sqrt((peer['x']-me['x'])**2+(peer['y']-me['y'])**2)
                    if (peer['robot_id']!=robot and peer['stage']=='idle' and peer['carrying'] is False
                            and peer.get('task_id') is None and peer['battery']>=request.get('min_soc',35.) and distance<=request.get('radius_m',200.)):
                        peers.append({'robot':peer,'distance_lower_bound_m':distance,'rack_zone':region(peer,scene)})
                peers.sort(key=lambda p:(p['distance_lower_bound_m'],p['robot']['robot_id']));limit=request.get('limit',10)
                for peer in peers:peer['distance_lower_bound_m']=Approx(peer['distance_lower_bound_m'],1e-10)
                expected.update(robot=me,candidates=peers[:limit],eligible_candidate_count=len(peers),candidate_list_truncated=len(peers)>limit,
                    events=events,feedback=feedback(events),planning_required=['energy admission including return reserve','route/traffic reservation','live unique ownership'],
                    candidate_scope='idle, empty, minimum SOC and Euclidean radius facts only; not feasible assignments')
            elif q=='progress':
                expected.update(events=events,kind_counts=dict(Counter(x['event']['kind'] for x in events)),feedback=feedback(events),
                    filter_task_id=request.get('task_id'),window={'start_seq':start,'end_seq':seq,'complete':True})
            elif q=='memory':
                require(seq-start+1<=request.get('history_limit',3601),'source memory exceeds response budget')
                states=[{'seq':i,'state':self.t.state(i,robot)} for i in range(start,seq+1)];first,last=states[0]['state'],states[-1]['state']
                xs=[h['state']['x'] for h in states];ys=[h['state']['y'] for h in states];span=math.sqrt((max(xs)-min(xs))**2+(max(ys)-min(ys))**2)
                expected.update(robot_id=robot,states=states,events=events,observations={'samples':len(states),
                    'stage_samples':dict(Counter(h['state']['stage'] for h in states)),'delta_x':last['x']-first['x'],'delta_y':last['y']-first['y'],
                    'battery_delta':last['battery']-first['battery'],'distance_delta':last['distance']-first['distance'],
                    'position_span_m':Approx(span,1e-10),'stationary_by_position':span<=1. if len(states)>1 else None,'sample_counts_are_continuous_durations':False})
            elif q in ('local_awareness','global'):
                tasks=self.t.all_tasks(seq);rack_zones={r['id']:r['zone'] for r in scene['racks']};counts=defaultdict(lambda:[0,0]);clock=batch['sim_time']
                for task in tasks:
                    zone=rack_zones.get(task['slot_id'].split('-')[0],'unmapped');key=(zone,task['status']);counts[key][0]+=1
                    counts[key][1]+=int(task['status']!='completed' and task['due_second']<clock)
                # Cypher's grouped collect order is not part of the contract;
                # compare the complete rows after canonical key ordering.
                actual_rows=body.get('task_counts',[])
                for entry in actual_rows:require(set(entry)=={'zone','status','tasks','overdue'},'task summary fields differ')
                actual_rows.sort(key=lambda r:(r['zone'],r['status']))
                zone=request.get('zone',region(me,scene)) if q=='local_awareness' else None
                summary=[{'zone':z,'status':s,'tasks':n,'overdue':late} for (z,s),(n,late) in sorted(counts.items()) if zone is None or z==zone]
                fleet=[r for r in robots if zone is None or region(r,scene)==zone];bays=self.bay_states(seq)
                expected.update(zone=zone,robot_region_semantics='nearest rack centre at observed coordinates',task_region_semantics='task target slot rack zone',
                    fleet_counts=dict(Counter(r['stage'] for r in fleet)),robots=fleet,task_counts=summary,
                    step={key:batch.get(key) for key in ('seq','tick','sim_time','completed','running')},
                    bay_counts={'physical_bays':100,'occupied':sum(bool(b['occupant_robot_ids']) for b in bays),'charging':sum(b['charging_robot_id'] is not None for b in bays)},
                    recent_event_kind_counts=dict(Counter(e['event']['kind'] for e in events)),
                    event_window_scope='global run window; event zone is not inferred from current task ownership',window={'start_seq':start,'end_seq':seq,'complete':True})
            else:raise ValueError('unsupported query_id '+str(q))
        return {'catalog_version':'warehouse-query-catalog-v1','query_id':q,'run_id':run,'seq':seq,'read_only':True,'used_by_live_control':False,'result':expected}

    def check(self,request,response):
        # Comparison may normalize unordered group rows; never mutate saved wire.
        actual=deepcopy(response)
        expected=self.expected(request,actual)
        return compare(actual,expected)


def verify_files(truth,requests,responses):
    errors=[];request_rows=[];response_rows={}
    for line,row,error in read_lines(responses):
        if error:errors.append({'response_line':line,'error':error});continue
        if not isinstance(row,dict):errors.append({'response_line':line,'error':'response line must be an object'});continue
        key=row.get('request_id')
        if not isinstance(key,str) or not key:errors.append({'response_line':line,'error':'invalid response request_id'});continue
        if key in response_rows:errors.append({'request_id':key,'error':'duplicate response'})
        else:response_rows[key]=row
    seen=set();counts=Counter();verifier=ResponseVerifier(truth);request_count=0
    for line,row,error in read_lines(requests):
        request_count+=1
        if error:errors.append({'request_line':line,'error':error});continue
        if not isinstance(row,dict):errors.append({'request_line':line,'error':'request line must be an object'});continue
        key=row.get('request_id');request_rows.append(row)
        try:
            require(isinstance(key,str) and key,'invalid request_id')
            require(key not in seen,'duplicate request_id');seen.add(key)
            require(type(row.get('ok')) is bool and row['ok'],'request failed or absent ok flag')
            begin=integer(row.get('start_ns'));end=integer(row.get('end_ns'),begin)
            request=row['request'];require(isinstance(request,dict),'request object absent')
            require(row.get('run_id')==request.get('run_id') and row.get('seq')==request.get('seq'),'outer/inner request identity mismatch')
            response=response_rows.pop(key,None);require(response is not None,'response missing')
            result=response['result'];require(digest(result)==row.get('response_sha256'),'saved full response fingerprint differs')
            mismatches=verifier.check(request,result)
            counts[request['query_id']]+=1
            if mismatches:errors.append({'request_id':key,'query_id':request['query_id'],'errors':mismatches})
        except Exception as exc:errors.append({'request_id':key,'request_line':line,'error':type(exc).__name__+': '+str(exc)[:3000]})
    for key in response_rows:errors.append({'request_id':key,'error':'orphan response without unique matching request'})
    return {'passed':bool(request_rows) and not errors,'request_count':request_count,'parsed_request_count':len(request_rows),'queries_by_id':dict(counts),
        'error_count':len(errors),'errors':errors,
        'scope':'posthoc complete catalog responses vs original journal/RealOracle; business-exact has independent scalar topK; legacy full-ann requires separate frozen verifier'}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for flag in ('journal','requests','responses','oracle-dir','output'):parser.add_argument('--'+flag,type=Path,required=True)
    args=parser.parse_args(argv)
    if args.output.exists():parser.error('preserve earlier verification; choose new output')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    oracle=truth=None
    before={key:None for key in ('journal','requests','responses')}
    try:
        before={key:file_sha(getattr(args,key)) for key in before}
        from backend.real_scale.oracle import RealOracle
        records=[r for _,r,e in read_lines(args.requests) if e is None and isinstance(r,dict)]
        with closing(sqlite3.connect(args.journal.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            heads=db.execute('SELECT seq FROM heads').fetchall()
        require(len(heads)==1,'one original source journal required')
        maximum=max((r['seq'] for r in records if type(r.get('seq')) is int and 1<=r['seq']<=heads[0][0]),default=1)
        args.oracle_dir.mkdir(parents=True,exist_ok=True);index=args.oracle_dir/'oracle.sqlite'
        oracle=RealOracle.open(index) if index.exists() else RealOracle(args.journal,prefix=maximum,index_path=index)
        require(oracle.prefix>=maximum,'oracle prefix does not cover queries')
        truth=JournalTruth(args.journal,oracle)
        report=verify_files(truth,args.requests,args.responses)
        report.update(source_run_id=truth.run_id,source_journal_head=truth.head,source_prefix_sha256=truth.identity['prefix_sha256'],
            source_journal_sha256=before['journal'],file_hashes={'requests':before['requests'],'responses':before['responses']},
            oracle_file_sha256=file_sha(index),oracle_prefix=oracle.prefix)
        after={key:file_sha(getattr(args,key)) for key in before}
        report['source_and_packets_unchanged']=before==after
        if before!=after:
            report['passed']=False;report['errors'].append({'error':'source or packet changed during verification'});report['error_count']+=1
    except Exception as exc:
        report={'passed':False,'request_count':0,'source_journal_sha256':before['journal'],
            'file_hashes':{'requests':before['requests'],'responses':before['responses']},
            'error_count':1,'errors':[{'error':type(exc).__name__+': '+str(exc)[:3000]}]}
    finally:
        if truth:truth.close()
        if oracle:oracle.close()
    with args.output.open('x',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2,allow_nan=False)
    print(json.dumps({key:report[key] for key in ('passed','request_count','error_count')},ensure_ascii=False))
    return 0 if report['passed'] else 2


if __name__=='__main__':raise SystemExit(main())
