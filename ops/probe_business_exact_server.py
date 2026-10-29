"""Experimental read-only server TopK vs released client exact; no default changes.

No APOC, schema writes, new indexes, hidden result caches or dependency installs.
Use a NEW output directory. The script is an ops prototype, not the weekly
runtime's selected backend. Complete service timing includes both Bolt reads,
budget checks, response validation and a complete JSON round trip.
"""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from backend.knowledge import digest
from backend.real_scale.store import RealStore,identity,integer
from backend.real_scale.oracle import RealOracle
from backend.retrieval_eval import business_vector,cosine,family,VERSION
from backend.retrieval_eval.service import HEADER,BusinessRetriever,CandidateBudgetExceeded


SERVER_QUERY = '''MATCH (n:WHAgentState {run_id:$run})
    USING INDEX n:WHAgentState(run_id,seq)
    WHERE n:WHRealState AND n.seq >= $start AND n.seq <= $end
      AND n.robot_id<>$robot
    WITH n LIMIT $budget
    WITH collect(n) AS candidates
    WITH candidates,size(candidates) AS candidate_count,
      size([n IN candidates WHERE n.x IS NULL OR n.y IS NULL OR n.battery IS NULL
        OR n.carrying IS NULL OR n.stage IS NULL OR n.real_payload_sha256 IS NULL
        OR NOT(n.x>=0 AND n.x<=$width AND n.y>=0 AND n.y<=$depth
               AND n.battery>=0 AND n.battery<=100)]) AS invalid_count
    CALL (candidates,candidate_count,invalid_count) {
      WITH candidates,candidate_count,invalid_count
      WHERE candidate_count<=$maximum AND invalid_count=0
      UNWIND candidates AS n
      WITH n,CASE
        WHEN n.stage IN ['idle','standby','mission_queue','queued'] THEN 0
        WHEN n.stage IN ['to_source','to_target','moving'] THEN 1
        WHEN n.stage IN ['picking','placing','docking','undocking'] THEN 2
        WHEN n.stage IN ['charging','charge_queue'] THEN 3
        WHEN n.stage IN ['returning','to_charge'] THEN 4
        WHEN n.stage IN ['to_handoff','handoff','handoff_receive','handoff_give'] THEN 5
        WHEN n.stage IN ['fault_wait','faulted','recovery_wait','yielding','blocked'] THEN 6
        ELSE 7 END AS phase
      WITH n,phase,[1.0,n.x/$width,1.0-n.x/$width,n.y/$depth,1.0-n.y/$depth,
        n.battery/100.0,1.0-n.battery/100.0,
        CASE WHEN n.carrying THEN 1.0 ELSE 0.0 END,
        CASE WHEN n.carrying THEN 0.0 ELSE 1.0 END]
        +[j IN range(0,7) | CASE WHEN j=phase THEN 1.0 ELSE 0.0 END] AS raw
      WITH n,phase,raw,reduce(norm=0.0,v IN raw | norm+v*v) AS norm2,
        reduce(dot=0.0,j IN range(0,16) | dot+raw[j]*$query_vector[j]) AS dot
      WITH n,phase,dot/sqrt(norm2) AS score
      ORDER BY score DESC,n.seq,n.robot_id LIMIT $k
      RETURN collect({key:n.key,run_id:n.run_id,seq:n.seq,robot_id:n.robot_id,
        x:n.x,y:n.y,battery:n.battery,carrying:n.carrying,stage:n.stage,
        source_payload_sha256:n.real_payload_sha256,score:score,phase:phase}) AS rows
    }
    RETURN candidate_count,invalid_count,rows'''


def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for part in iter(lambda:f.read(1024*1024),b''):h.update(part)
    return h.hexdigest()


def json_roundtrip(value):
    return json.loads(json.dumps(value,ensure_ascii=False,allow_nan=False))


def server_search(mirror,run,seq,robot,*,k=5,window=60,embargo=20,maximum=10000):
    identity(run);integer(seq,'cutoff');integer(robot,'robot',1,100)
    integer(k,'k',1,100);integer(window,'window',1,10000)
    integer(embargo,'embargo',1,10000);integer(maximum,'maximum',1,100000)
    if embargo>window or seq<=embargo:raise ValueError('no eligible historical window')
    begun=time.perf_counter()
    headers=mirror.query(HEADER,{'run':run,'seq':seq,'robot':robot,
        'step_key':f'{run}:{seq}','state_key':f'{run}:{seq}:{robot}'},timeout=60)
    if len(headers)!=1:raise ValueError('one source header required')
    header=headers[0];query=json.loads(header['query_payload'])
    if (type(header['head']) is not int or header['head']<seq or type(query.get('robot_id')) is not int
        or query.get('robot_id')!=robot or digest(query)!=header['query_sha256']
        or not isinstance(header.get('checksum'),str) or not re.fullmatch('[0-9a-f]{64}',header['checksum'])):
        raise ValueError('invalid source query identity/head/SHA')
    geometry=json.loads(header['metadata'])['scene']['spec'];width,depth=geometry['width'],geometry['depth']
    vector=business_vector(query,width,depth)
    start,end=max(1,seq-window),seq-embargo
    header_done=time.perf_counter()
    values=mirror.query(SERVER_QUERY,{'run':run,'robot':robot,'start':start,'end':end,
        'budget':maximum+1,'maximum':maximum,'width':float(width),'depth':float(depth),
        'query_vector':vector,'k':k},timeout=60)
    graph_done=time.perf_counter()
    if len(values)!=1:raise ValueError('one bounded retrieval result required')
    value=values[0]
    if type(value['candidate_count']) is not int or not 0<=value['candidate_count']<=maximum+1:
        raise ValueError('bad bounded candidate count')
    if value['candidate_count']>maximum:raise CandidateBudgetExceeded('candidate budget exceeded')
    if value['invalid_count']!=0:raise ValueError('invalid graph candidate features')
    rows=value['rows'];seen=set()
    if len(rows)!=min(k,value['candidate_count']):raise ValueError('silently incomplete TopK')
    for row in rows:
        if (row['run_id']!=run or type(row['seq']) is not int or not start<=row['seq']<=end
            or type(row['robot_id']) is not int or not 1<=row['robot_id']<=100 or row['robot_id']==robot
            or row['key']!=f"{run}:{row['seq']}:{row['robot_id']}" or row['key'] in seen):
            raise ValueError('foreign/future/self/duplicate row')
        if not isinstance(row.get('source_payload_sha256'),str) or not re.fullmatch('[0-9a-f]{64}',row['source_payload_sha256']):
            raise ValueError('source payload SHA absent')
        expected=cosine(vector,business_vector(row,width,depth))
        if not math.isfinite(row['score']) or abs(row['score']-expected)>1e-12:
            raise ValueError('server cosine differs from independent normalized scalar')
        seen.add(row['key'])
    result={'run_id':run,'seq':seq,'robot_id':robot,'graph_committed_seq':header['head'],
        'batch_checksum':header['checksum'],'query_source_payload_sha256':header['query_sha256'],
        'feature_version':VERSION,'method':'experimental-server-prefiltered-exact-cosine',
        'domain':{'start_seq':start,'end_seq':end,'candidate_count':value['candidate_count'],
                  'max_candidates':maximum,'complete_domain':True,'exclude_query_robot':True},
        'rows':rows,'header_ms':(header_done-begun)*1000,'graph_ms':(graph_done-header_done)*1000}
    return json_roundtrip(result)


def verify(result,oracle):
    """Independent source domain and scalar ranking, not production server math."""
    run,seq,robot=result['run_id'],result['seq'],result['robot_id']
    query=oracle.state(seq,robot);q=business_vector(query)
    domain=result['domain'];source=[]
    for s,r,payload,sha in oracle._db().execute(
            'SELECT seq,robot,payload,sha256 FROM states WHERE seq>=? AND seq<=? AND robot<>?',
            (domain['start_seq'],domain['end_seq'],robot)):
        state=json.loads(payload)
        source.append({'key':f'{run}:{s}:{r}','seq':s,'robot_id':r,'state':state,'sha':sha,
                       'score':cosine(q,business_vector(state))})
    if len(source)!=domain['candidate_count']:raise AssertionError('domain count differs from source')
    truth=sorted(source,key=lambda r:(-r['score'],r['seq'],r['robot_id']))[:5]
    actual=result['rows'];bykey={r['key']:r for r in source}
    if len(actual)!=len(truth):raise AssertionError('wrong actual result size')
    for row in actual:
        expected=bykey[row['key']]
        if row['source_payload_sha256']!=expected['sha']:raise AssertionError('wrong source SHA')
        if any(row[field]!=expected['state'][field] for field in ('x','y','battery','carrying','stage')):
            raise AssertionError('wrong source physical fields')
        if abs(row['score']-expected['score'])>1e-12:raise AssertionError('wrong exact score')
    strict=[r['key'] for r in actual]==[r['key'] for r in truth]
    if not strict:
        threshold=truth[-1]['score']
        if any(bykey[r['key']]['score']<threshold-1e-12 for r in actual):
            raise AssertionError('TopK has a non-tied wrong neighbor')
        if any(abs(actual[j]['score']-truth[j]['score'])>1e-12 for j in range(len(truth))):
            raise AssertionError('TopK positions have non-equivalent scores')
    return {'passed':True,'strict_source_key_order':strict,
        'topk_equivalent_within_1e_12':True,'candidate_count':len(source)}


def summary(samples):
    values=sorted(r['elapsed_ms'] for r in samples)
    return {'requests':len(values),'mean_ms':sum(values)/len(values),
        'p99_ms':values[math.ceil(.99*len(values))-1],'max_ms':max(values),
        'over_500ms':sum(v>500 for v in values)}


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--oracle-index',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    args=p.parse_args(argv);args.output.mkdir(parents=True,exist_ok=False)
    store=RealStore.from_env(timeout=60);oracle=RealOracle.open(args.oracle_index)
    sha_before=file_sha(args.oracle_index);head_before=store.head(oracle.run_id)
    samples=[];failures=[];checks=[]
    # Fixed in advance: original four source points plus full-prefix and
    # low-SOC examples. Alternating AB/BA mitigates simple ordering effects.
    cases=[(350,9),(576,88),(6000,66),(10000,100),(22,1),(456,58),
           (1130,79),(2239,56),(3348,34),(6004,3),(7783,45),(8892,22)]
    try:
        baseline=BusinessRetriever(store.mirror,timeout=60)
        for case,(seq,robot) in enumerate(cases,1):
            for method in (('client','server') if case%2 else ('server','client')):
                begun=time.perf_counter()
                try:
                    result=server_search(store.mirror,oracle.run_id,seq,robot) if method=='server' else json_roundtrip(baseline.search(oracle.run_id,seq,robot))
                    elapsed=(time.perf_counter()-begun)*1000
                    sample={'case':case,'method':method,'seq':seq,'robot_id':robot,
                        'elapsed_ms':elapsed,'result':result}
                    samples.append(sample)
                    checks.append({'case':case,'method':method,**verify(result,oracle)})
                except Exception as exc:
                    failures.append({'case':case,'method':method,'seq':seq,'robot_id':robot,
                        'elapsed_ms':(time.perf_counter()-begun)*1000,
                        'error':type(exc).__name__+': '+str(exc)})
            print(json.dumps({'case':case,'seq':seq,'failures':len(failures)}),flush=True)
        budget_rejected=False
        try:server_search(store.mirror,oracle.run_id,6000,66,maximum=1)
        except CandidateBudgetExceeded:budget_rejected=True
        except Exception as exc:failures.append({'scope':'budget_probe','error':type(exc).__name__+': '+str(exc)})
        head_after=store.head(oracle.run_id)
        result={'passed':not failures and len(samples)==24 and budget_rejected
                    and head_before==head_after and file_sha(args.oracle_index)==sha_before,
            'scope':'experimental ops-only12paired serial requests; cold requests retained; no default or weekly-runtime change',
            'source_identity':oracle.identity,'oracle_sha_before':sha_before,
            'oracle_sha_after':file_sha(args.oracle_index),'graph_head_before':head_before,'graph_head_after':head_after,
            'script_sha256':file_sha(__file__),'query_sha256':hashlib.sha256(SERVER_QUERY.encode()).hexdigest(),
            'budget_rejection_passed':budget_rejected,'failures':failures,'samples':samples,'source_checks':checks,
            'summary':{name:summary([r for r in samples if r['method']==name])
                for name in ('client','server') if any(r['method']==name for r in samples)},
            'limitations':['12samples/method are diagnostics, not tail-latency/stability certification',
                'math.reduce vs fsum ties explicitly compared within1e-12',
                'queries include all header/candidate/JSON work, independent source validation is afterward']}
        (args.output/'report.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
        print(json.dumps({'passed':result['passed'],'failures':failures,'summary':result['summary']}))
        return 0 if result['passed'] else 2
    finally:store.close();oracle.close()


if __name__=='__main__':raise SystemExit(main())
