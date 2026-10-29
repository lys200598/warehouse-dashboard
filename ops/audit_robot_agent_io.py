"""Read-only complete robot HTTP evidence vs canonical journal and real graph.

The oracle is built AFTER the workload ends. Cold requests, errors and every
response field remain in the denominator. No input or graph repair is done.
"""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time

VERSION='warehouse-agent-source-audit-v2'
PATHS={'/api/agent/observation':'write','/api/agent/query':'query'}


def require(value,message):
    if not value:raise ValueError(message)


def packet_kind(row):
    require(row.get('path') in PATHS,'unknown agent endpoint')
    require(row.get('http',{}).get('path')==row['path'],'HTTP/packet endpoint differs')
    require(row['http'].get('request_id')==row.get('request_id'),'HTTP/packet request ID differs')
    return PATHS[row['path']]


def packet_integrity(row,run,pids,digest):
    rid=row['robot_id'];body=row['request'];res=row['response'];http=row['http']
    kind=packet_kind(row)
    require(type(rid) is int and rid in pids,'unknown robot')
    require(row['pid']==pids[rid] and http.get('pid')==pids[rid]
        and http.get('robot_id')==rid and body.get('robot_id')==rid and body.get('run_id')==run,'process/source binding')
    require(row.get('request_sha256')==digest(body) and row.get('response_sha256')==digest(res),'packet hash')
    require(row.get('response_body_sha256')==digest(res),'transport body fingerprint differs from full JSON')
    require(http.get('status')==200 and not http.get('error') and http.get('body_complete') is True,'HTTP failure')
    require(type(res) is dict and res.get('ok') is True and res.get('run_id')==run
        and res.get('robot_id')==rid and res.get('backend')=='neo4j','response identity/backend')
    clock=[http.get(name) for name in ('start_ns','headers_ns','body_end_ns','parse_end_ns','end_ns')]
    require(all(type(t) is int and t>0 for t in clock) and clock==sorted(clock),'invalid client timing order')
    duration=http.get('duration_ms')
    require(type(duration) in (int,float) and math.isfinite(duration) and duration>=0
        and abs(duration-(clock[-1]-clock[0])/1e6)<1e-6,'client duration differs from raw timestamps')
    require(row.get('observer_after_http') is True,'observer timing boundary absent')
    if kind=='write':
        require(set(body)=={'run_id','robot_id','seq','sim_time','committed_seq','state','events'},'write request fields')
        require(set(res)=={'ok','run_id','robot_id','seq','payload_sha256','backend','metrics'},'write response fields')
        require(set(res['metrics'])=={'queue_and_commit_ms'},'write metric fields')
    else:
        require(set(body)<= {'run_id','robot_id','at_seq','limit'} and 'at_seq' in body,'query request fields')
        require(set(res)=={'ok','backend','run_id','robot_id','at_seq','context','use','action_applied','metrics'},'query response fields')
        require(set(res['context'])=={'observations','canonical_context'},'query context fields')
        require(set(res['metrics'])=={'query_queue_ms','backend_query_ms','service_ms'},'query metric fields')
        require(res.get('at_seq')==body['at_seq'],'query requested fence differs')
    require(all(type(v) in (int,float) and math.isfinite(v) and v>=0 for v in res['metrics'].values()),'invalid service timings')
    return kind


def trace_verdict(api,seen,status):
    ids=[r.get('request_id') for r in api]
    failures=[r for r in api if r.get('status')!=200 or r.get('body_send_completed') is not True
        or r.get('path') not in PATHS or type(r.get('start_ns')) is not int
        or type(r.get('end_ns')) is not int or r.get('end_ns',0)<r.get('start_ns',0)]
    complete=bool(status and status.get('complete') is True and status.get('drained') is True
        and status.get('dropped')==0 and not status.get('transport_rejections',0)
        and not status.get('prior_trace_incomplete',False) and set(ids)==seen
        and len(ids)==len(set(ids)) and not failures)
    return complete,failures


def client_packet_coverage(clients,packets):
    require(set(clients)==set(packets),'full response/client trace request coverage differs')
    for key in clients:
        require(clients[key]==packets[key]['http'],'saved packet HTTP differs from independent client trace')


def check_canonical_context(verifier,request,wrapped):
    differences=verifier.check(request,wrapped)
    require(not differences,'canonical context differences: '+str(differences[:2]))


def expected_local_events(state,previous,sim_time):
    events=[]
    if previous is None:events.append({'kind':'observation_started','sim_time':sim_time})
    elif previous.get('stage')!=state.get('stage'):
        events.append({'kind':'stage_changed','sim_time':sim_time,'from_stage':previous.get('stage'),'to_stage':state.get('stage')})
    if previous is not None and previous.get('task_id')!=state.get('task_id'):
        events.append({'kind':'task_changed','sim_time':sim_time,'task_id':state.get('task_id')})
    low=float(state.get('battery',state.get('soc',100)))<35
    was_low=previous is not None and float(previous.get('battery',previous.get('soc',100)))<35
    if low and not was_low:events.append({'kind':'low_energy_observed','sim_time':sim_time})
    return events


def closed_verdict(closed):
    return bool(closed and closed.get('closed') is True and closed.get('agents')==100
        and closed.get('failed') is False and closed.get('backend')=='neo4j'
        and closed.get('live_processes')==0 and closed.get('http_service_alive') is False
        and closed.get('writer_alive') is False and closed.get('all_owned_processes_stopped') is True)


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def stats(values):
    a=sorted(values)
    return {'n':len(a),'mean_ms':sum(a)/len(a) if a else None,
        'p99_ms':a[math.ceil(.99*len(a))-1] if a else None,
        'max_ms':max(a) if a else None,'over_500ms':sum(x>500 for x in a)}


def peak(rows):
    changes=[v for r in rows for v in ((r['start_ns'],1),(r['end_ns'],-1))]
    active=best=0
    for _,n in sorted(changes):active+=n;best=max(active,best)
    return best


def audit(run_dir,output,*,graph=True):
    if not __debug__:raise RuntimeError('auditor must run without Python -O')
    from backend.knowledge import digest
    from backend.real_scale.oracle import RealOracle
    from ops.verify_catalog_responses import JournalTruth,ResponseVerifier,loads,compare
    run_dir,output=Path(run_dir),Path(output)
    if output.exists():raise FileExistsError('audit output exists; preserve previous result')
    output.parent.mkdir(parents=True,exist_ok=True)
    root=run_dir/'agents'
    result=loads((run_dir/'results.json').read_text())
    run=result['run_id']
    original=run_dir/'journal.sqlite'
    files=[original,run_dir/'results.json',*root.glob('*.json'),*root.glob('*.jsonl'),*root.glob('*.gz'),root/'agent-journal.sqlite']
    before={str(p):sha(p) for p in files if p.is_file()}
    errors=[];groups=defaultdict(list);owners=Counter();seen=set();writes=Counter();queries=Counter()
    clients={};packets={};written_points=set();client_record_count=0
    processes=loads((root/'agent-processes.json').read_text())
    pids={r['robot_id']:r['pid'] for r in processes['agents']}
    closed=json.loads((root/'agent-closed-status.json').read_text()) if (root/'agent-closed-status.json').exists() else None
    oracle=RealOracle(original,index_path=output.with_suffix('.oracle.sqlite'))
    truth=JournalTruth(original,oracle);verifier=ResponseVerifier(truth)
    db=sqlite3.connect((root/'agent-journal.sqlite').resolve().as_uri()+'?mode=ro',uri=True)
    graph_counts=None
    try:
        require(truth.run_id==run and processes.get('run_id')==run,'source journal/process run differs from results')
        require(processes.get('backend')=='neo4j','process backend is not Neo4j')
        require(len(processes['agents'])==100 and set(pids)==set(range(1,101))
            and len(set(pids.values()))==100 and all(type(pid) is int and pid>0 for pid in pids.values()),'registered robot/process identities differ')
        require(digest(result['config'])==digest(truth.config),'results/source config differs')
        require(result['code_identity']==truth.metadata['execution_code_sha256'],'results/source execution identity differs')
        require(set(result.get('input_identity',{}))=={'scene_sha256','tasks_sha256','initial_inventory_sha256','manifest_sha256'},'input identity fields differ')
        for key,value in result.get('input_identity',{}).items():
            require(value==truth.metadata.get(key),'results/source input identity differs: '+key)
        identity_row=db.execute("SELECT value FROM metadata WHERE key='identity'").fetchone()
        require(identity_row is not None,'agent journal identity absent')
        agent_identity=loads(identity_row[0])
        require(agent_identity['run_id']==run and agent_identity['robots']==list(range(1,101))
            and agent_identity['backend']=='neo4j','agent journal source identity differs')
        client_paths=sorted(root.glob('agent-????-http.jsonl'))
        require(len(client_paths)==100,'one original client HTTP trace per robot required')
        for path in client_paths:
            file_robot=int(path.name.split('-')[1])
            for number,line in enumerate(path.read_text().splitlines(),1):
                client_record_count+=1
                try:
                    row=loads(line);key=row['request_id']
                    require(key not in clients,'duplicate client request ID')
                    require(row['robot_id']==file_robot and row['pid']==pids[file_robot],'client trace process/file identity')
                    require(row['path'] in PATHS,'unknown original client endpoint')
                    clients[key]=row
                except Exception as exc:
                    errors.append({'client_file':path.name,'line':number,'error':type(exc).__name__+': '+str(exc)[:300]})
        response_paths=sorted(root.glob('agent-*-responses.jsonl.gz'))
        require(len(response_paths)==100,'one full response gzip per robot required')
        for path in response_paths:
            with gzip.open(path,'rt',encoding='utf-8') as stream:
                for number,line in enumerate(stream,1):
                    key=None
                    try:
                        row=loads(line);rid=row['robot_id'];key=row['request_id']
                        body,res,http=row['request'],row['response'],row['http']
                        owners[rid]+=1
                        assert key not in seen,'duplicate request ID'
                        seen.add(key)
                        packets[key]=row
                        require(int(path.name.split('-')[1])==rid,'response file robot identity')
                        kind=packet_kind(row)
                        # All valid numeric HTTP durations, including failed
                        # responses, enter totals before correctness validation.
                        if type(http.get('duration_ms')) in (int,float) and math.isfinite(http['duration_ms']):
                            groups[kind].append(http['duration_ms'])
                            point=(body.get('committed_seq') if kind=='write' else
                                res.get('context',{}).get('observations',[{}])[0].get('committed_seq') if isinstance(res,dict) else None)
                            phase='cold' if point==1 else 'later' if type(point) is int and point>1 else 'unknown'
                            groups[kind+'_'+phase].append(http['duration_ms'])
                        kind=packet_integrity(row,run,pids,digest)
                        if kind=='write':
                            assert res['seq']==body['seq'] and res['payload_sha256']==digest(body),'write ack'
                            assert not compare(body['state'],truth.state(body['committed_seq'],rid)),'canonical observation'
                            assert body['sim_time']==truth.batch(body['committed_seq'])['sim_time'],'canonical observation time'
                            ledger=db.execute('SELECT sha,payload,mirrored FROM observations WHERE robot=? AND seq=?',(rid,body['seq'])).fetchone()
                            assert ledger and ledger[0]==digest(body) and ledger[2]==1 and not compare(loads(ledger[1]),body),'write differs from mirrored journal'
                            written_points.add((rid,body['seq']))
                            writes[rid]+=1
                        else:
                            at=body['at_seq'];context=res['context']
                            raw=db.execute('SELECT payload FROM observations WHERE robot=? AND seq<=? AND mirrored=1 ORDER BY seq DESC LIMIT ?',
                                (rid,at,body.get('limit',8))).fetchall()
                            expected=[loads(r[0]) for r in raw]
                            assert expected and not compare(context['observations'],expected),'own history differs from durable journal'
                            cutoff=expected[0]['committed_seq']
                            # Reuse the complete source verifier for canonical self/task
                            # payload, not a checksum-only assertion.
                            wrapped={'catalog_version':'warehouse-query-catalog-v1','query_id':'self','run_id':run,'seq':cutoff,
                                'read_only':True,'used_by_live_control':False,'result':{'context':context['canonical_context'],
                                'readiness':{'requested_point_present':True,'future_point_used':False,
                                    'known_committed_lag_steps':context['canonical_context']['graph_committed_seq']-cutoff,
                                    'ready_for_live_action':False,'note':'historical read; live coordinator must recheck current ownership/reservations'}}}
                            check_canonical_context(verifier,{'query_id':'self','run_id':run,'seq':cutoff,'robot_id':rid},wrapped)
                            assert res['action_applied'] is False and res['use']=='advisory_available','use scope'
                            queries[rid]+=1
                    except Exception as exc:
                        errors.append({'response_file':path.name,'line':number,'request_id':key,'error':type(exc).__name__+': '+str(exc)[:300]})
        client_packet_coverage(clients,packets)
        ledger_points=set()
        for rid in sorted(pids):
            ledger=list(db.execute('SELECT seq,sha,payload,mirrored FROM observations WHERE robot=? ORDER BY seq',(rid,)))
            require(ledger and [r[0] for r in ledger]==list(range(1,len(ledger)+1)),'agent journal sequence gap')
            previous_time=previous_source=-1
            previous_state=None
            for seq,payload_sha,payload,mirrored in ledger:
                body=loads(payload);ledger_points.add((rid,seq))
                require(mirrored==1 and digest(body)==payload_sha and body['run_id']==run and body['robot_id']==rid
                    and body['seq']==seq,'durable agent observation identity/hash/pending differs')
                require(body['sim_time']>=previous_time and body['committed_seq']>=previous_source,'agent source time/seq moved backwards')
                require(body['sim_time']==truth.batch(body['committed_seq'])['sim_time'] and
                    not compare(body['state'],truth.state(body['committed_seq'],rid)),'durable observation differs from canonical source')
                require(not compare(body['events'],expected_local_events(body['state'],previous_state,body['sim_time'])),
                        'agent local events differ from observed state transitions')
                previous_time,previous_source=body['sim_time'],body['committed_seq']
                previous_state=body['state']
        require(written_points==ledger_points,'successful HTTP writes do not cover every durable agent observation')
        if graph:
            from neo4j import GraphDatabase,READ_ACCESS
            driver=GraphDatabase.driver(os.environ['NEO4J_URI'],auth=(os.environ.get('NEO4J_USER','neo4j'),os.environ['NEO4J_PASSWORD']),max_connection_pool_size=1)
            try:
                with driver.session(database=os.environ.get('NEO4J_DATABASE','neo4j'),default_access_mode=READ_ACCESS) as session:
                    source=session.run('MATCH (r:WHRun {run_id:$run}) RETURN r.metadata_json AS metadata,r.config_json AS config,r.committed_seq AS seq',run=run).single()
                    require(source and source['seq']==truth.head and digest(loads(source['metadata']))==digest(truth.metadata)
                        and digest(loads(source['config']))==digest(truth.config),'graph/source run identity or complete head differs')
                    rows=session.run('MATCH (o:WHAgentObservation {run_id:$run}) RETURN o.robot_id AS robot,o.agent_seq AS seq,o.payload_json AS payload,o.payload_sha256 AS sha',run=run)
                    n=0
                    for row in rows:
                        expected=db.execute('SELECT sha,payload FROM observations WHERE robot=? AND seq=? AND mirrored=1',(row['robot'],row['seq'])).fetchone()
                        assert expected and row['sha']==expected[0] and row['payload']==expected[1],'graph/durable observation differs'
                        n+=1
                    expected=db.execute('SELECT count(*) FROM observations WHERE mirrored=1').fetchone()[0]
                    assert n==expected,'graph observation cardinality'
                    events=session.run('''MATCH (e:WHAgentRuntimeEvent {run_id:$run})
                        OPTIONAL MATCH (e)-[:FROM_OBSERVATION]->(o:WHAgentObservation)
                        RETURN e.key AS key,e.robot_id AS robot,e.agent_seq AS seq,e.kind AS kind,
                          e.sim_time AS sim_time,e.payload_json AS payload,collect(o.key) AS observations''',run=run)
                    event_count=0
                    for row in events:
                        raw=db.execute('SELECT payload FROM observations WHERE robot=? AND seq=? AND mirrored=1',(row['robot'],row['seq'])).fetchone()
                        require(raw is not None,'graph event has no own durable observation')
                        body=loads(raw[0]);index=int(row['key'].rsplit(':',1)[1])
                        require(0<=index<len(body['events']),'graph event index outside own observation')
                        require(row['key']==f"{run}:{row['robot']}:{row['seq']}:{index}" and
                            row['observations']==[f"{run}:{row['robot']}:{row['seq']}"] and
                            row['kind']==body['events'][index]['kind'] and row['sim_time']==body['sim_time'] and
                            not compare(loads(row['payload']),body['events'][index]),'graph event payload/link differs')
                        event_count+=1
                    expected_events=sum(len(loads(r[0])['events']) for r in db.execute('SELECT payload FROM observations WHERE mirrored=1'))
                    require(event_count==expected_events,'graph own-event cardinality')
                    graph_counts={'observations':n,'journal_mirrored_observations':expected,
                                  'events':event_count,'journal_events':expected_events,'same_source_run_head':source['seq']}
            finally:driver.close()
    except Exception as exc:errors.append({'stage':'audit','error':type(exc).__name__+': '+str(exc)[:300]})
    finally:db.close();truth.close();oracle.close()
    server=[loads(s) for s in (root/'agent-http.jsonl').read_text().splitlines()]
    api=[r for r in server if r.get('kind')=='api']
    trace_path=root/'agent-http.jsonl.status.json'
    trace_status=loads(trace_path.read_text()) if trace_path.exists() else None
    complete_trace,server_failures=trace_verdict(api,seen,trace_status)
    if any(r.get('kind')=='agent_error' for r in server):
        errors.append({'stage':'server','error':'server recorded agent exception'})
    # Statistical denominators come from original client logs, not just the
    # subset with intact/full/valid response packets. Missing responses and
    # 503s remain timed samples; unavailable source labels stay "unknown".
    groups=defaultdict(list)
    for key,client in clients.items():
        kind=PATHS.get(client.get('path'),'unknown_endpoint')
        packet=packets.get(key,{})
        body=packet.get('request',{});res=packet.get('response')
        try:
            point=(body.get('committed_seq') if kind=='write' else
                res.get('context',{}).get('observations',[{}])[0].get('committed_seq') if isinstance(res,dict) else None)
        except (KeyError,IndexError,TypeError,AttributeError):point=None
        phase='cold' if point==1 else 'later' if type(point) is int and point>1 else 'unknown'
        duration=client.get('duration_ms')
        if type(duration) in (int,float) and math.isfinite(duration) and duration>=0:
            groups[kind].append(duration);groups[kind+'_'+phase].append(duration)
        else:errors.append({'request_id':key,'error':'original client timing absent/invalid'})
        if client.get('status')!=200 or client.get('error') or client.get('body_complete') is not True:
            errors.append({'request_id':key,'error':'original client HTTP failure'})
    after={p:sha(p) for p in before}
    closed_ok=closed_verdict(closed)
    report={'version':VERSION,'run_id':run,'errors':errors,'requests':client_record_count,
        'full_response_records':sum(owners.values()),'request_id_denominator':len(set(clients)|set(packets)),
        'unique_processes':len(set(pids.values())),'robots_with_writes':len(writes),'robots_with_queries':len(queries),
        'requests_per_robot':dict(owners),'complete_JSON_source_verified':not errors,'actual_server_api_peak':peak(api),
        'server_trace_complete':complete_trace,'server_failures':server_failures,'statistics':{k:stats(v) for k,v in groups.items()},
        'trace_status':trace_status,'graph_checked':graph,'audit_tool_sha256':sha(__file__),
        'phase_label_definition':{'cold':'canonical source commit 1; includes every first-batch request',
            'later':'canonical commit >1; chronological later, not proof of fully warm caches','unknown':'source commit unavailable due to error'},
        'graph':graph_counts,'source_artifacts_unchanged':before==after,'artifacts_sha256':before,
        'closed':closed,'closed_ok':closed_ok,'million_prefix_qualified':False,
        'use':'robot self/history/task knowledge available; canonical scheduler separately consumes historical handoff authorization',
        'all_requests_under500ms':bool(groups) and not errors and all(v<=500 for values in groups.values() for v in values)}
    report['passed']=bool(result['status']=='complete' and len(pids)==100 and len(set(pids.values()))==100
        and len(writes)==100 and len(queries)==100 and not errors and complete_trace and before==after and closed_ok
        and (not graph or graph_counts))
    output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return report


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--no-graph',action='store_true')
    a=p.parse_args(argv);r=audit(a.run,a.output,graph=not a.no_graph)
    print(json.dumps({k:r[k] for k in ('passed','requests','unique_processes','robots_with_queries','actual_server_api_peak','all_requests_under500ms')}))
    return 0 if r['passed'] else 2


if __name__=='__main__':raise SystemExit(main())
