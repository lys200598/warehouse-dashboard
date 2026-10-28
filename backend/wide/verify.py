"""Read-only real-Neo4j correctness and 100-agent query smoke verification.

This is a small-run Bolt API smoke test, not the million-scale, multimodal or
specified edge-device qualification. It never copies or prints credentials.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
import zlib

from ..checkpoint import read_checkpoint
from ..knowledge import canonical, digest
from .graph import WideMirror, audit_wide
from .queries import WideQueries


MAX_SMOKE_EVENTS = 200000
HISTORY_FIELDS = ('event_seq','seq','sim_time','kind','location_id','event_key')


def _custody_event(ledger, event, tasks):
    """Independent cargo transition oracle using persisted business events."""
    if 'goods_id' not in event or 'location_id' not in event:
        return
    goods, location, kind = event['goods_id'], event['location_id'], event['kind']
    before = ledger.get(goods)
    task = tasks.get(event.get('task_id'))
    if kind == 'initial_stock':
        if before is not None:
            raise AssertionError('initial cargo identity reused')
    elif kind == 'received':
        if before is not None or task is None or location != task['source_id']:
            raise AssertionError('invalid received cargo dependency')
    elif kind == 'picked':
        if task is None or before != task['source_id'] or location != f"ROBOT-{event['robot_id']}":
            raise AssertionError('pick does not transfer from its unique source')
    elif kind in {'placed','shipped'}:
        target = 'SHIPPED' if kind == 'shipped' else task['target_id'] if task else None
        if before != f"ROBOT-{event['robot_id']}" or location != target:
            raise AssertionError('delivery does not transfer from its unique carrier')
    elif kind == 'cargo_handoff':
        donor = event.get('donor_robot_id',event.get('donor_id'))
        receiver = event.get('receiver_robot_id',event.get('receiver_id'))
        if donor == receiver or before != f'ROBOT-{donor}' or location != f'ROBOT-{receiver}':
            raise AssertionError('handoff does not transfer unique donor custody')
    else:
        raise AssertionError('unclassified cargo transition: '+str(kind))
    ledger[goods] = location


def load_source(output):
    """Validate immutable checkpoint identity and replay every durable commit."""
    output = Path(output).resolve()
    path = output/'checkpoint.json'
    envelope = json.loads(path.read_text(encoding='utf-8'))
    if envelope.get('format') != 'warehouse-checkpoint-v2':
        raise ValueError('this verifier requires a SHA256-enveloped checkpoint')
    checkpoint = read_checkpoint(path)
    if checkpoint.get('running'):
        raise ValueError('verify a stopped/finalized run, not a changing live checkpoint')
    run = checkpoint['run_id']
    journal = output/'journal.sqlite'
    db = sqlite3.connect(f'file:{journal.as_posix()}?mode=ro',uri=True)
    try:
        head = db.execute('SELECT seq,checksum,checkpoint,mirrored_seq FROM heads WHERE run_id=?',(run,)).fetchone()
        if head is None:
            raise AssertionError('checkpoint has no durable journal head')
        raw = zlib.decompress(head[2])
        if hashlib.sha256(raw).hexdigest() != head[1] or digest(json.loads(raw)) != digest(checkpoint):
            raise AssertionError('journal head and checkpoint identity differ')
        if head[0] != checkpoint['knowledge_seq']:
            raise AssertionError('journal/checkpoint committed sequence mismatch')
        batches, events, ledger, tasks, history, handoffs = {}, [], {}, {}, {}, []
        task_versions = {}
        previous_seq, previous_event_seq = 0, 0
        checksum = None
        for seq,checksum,blob in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq',(run,)):
            raw = zlib.decompress(blob)
            if hashlib.sha256(raw).hexdigest() != checksum:
                raise AssertionError('journal commit checksum mismatch')
            batch = json.loads(raw)
            if seq != previous_seq+1 or batch['seq'] != seq or batch['run_id'] != run:
                raise AssertionError('journal sequence gap or wrong run')
            previous_seq = seq
            for task in batch['tasks']:
                tasks[task['task_id']] = deepcopy(task)
                task_versions.setdefault(task['task_id'],[]).append((seq,deepcopy(task)))
            for event in batch['events']:
                if event['event_seq'] != previous_event_seq+1 or event['sim_time'] > batch['sim_time']:
                    raise AssertionError('business event archive gap or future event')
                previous_event_seq = event['event_seq']
                events.append({**event,'commit_seq':seq})
                if len(events)>MAX_SMOKE_EVENTS:
                    raise ValueError('input exceeds bounded small-run smoke size; use the formal benchmark harness')
                _custody_event(ledger,event,tasks)
                if 'goods_id' in event and 'location_id' in event:
                    history.setdefault(event['goods_id'],[]).append({
                        'event_seq':event['event_seq'],'seq':seq,'sim_time':event['sim_time'],
                        'kind':event['kind'],'location_id':event['location_id'],'event_key':event['event_id']})
                if event['kind']=='cargo_handoff':
                    handoffs.append({**event,'commit_seq':seq})
            batches[seq] = {'sim_time':batch['sim_time'],
                'robots':{r['robot_id']:deepcopy(r) for r in batch['robots']}}
        if previous_seq != checkpoint['knowledge_seq'] or previous_event_seq != checkpoint['event_sequence']:
            raise AssertionError('archive is not complete through the checkpoint')
        if ledger != checkpoint['goods'] or sum(v!='SHIPPED' for v in ledger.values()) != checkpoint['inside']:
            raise AssertionError('independent event replay disagrees with final cargo ledger')
        final_robots = {row['state']['robot_id']:row['state'] for row in checkpoint['robots']}
        if set(final_robots) != set(range(1,101)) or final_robots != batches[previous_seq]['robots']:
            raise AssertionError('final 100-robot journal snapshot disagrees with checkpoint')
        if tasks != checkpoint['tasks']:
            raise AssertionError('final task delta replay disagrees with checkpoint')
        for owner,robot in final_robots.items():
            held = [g for g,location in ledger.items() if location==f'ROBOT-{owner}']
            expected = [robot['cargo_id']] if robot['carrying'] else []
            if held != expected:
                raise AssertionError('final carrier state disagrees with unique event custody')
        if not handoffs:
            raise ValueError('this knowledge smoke requires at least one real cargo_handoff')
        candidates = []
        for event in handoffs:
            past = max(1,event['commit_seq']-1)
            older = [r for r in history[event['goods_id']] if r['seq']<=past]
            last = older[-1]['location_id'] if older else None
            if last != ledger[event['goods_id']]:
                candidates.append((bool(older),event,past))
        if not candidates:
            raise ValueError('no handoff cargo provides a distinct historical snapshot in this run')
        _,selected,historical_seq = max(candidates,key=lambda c:(c[0],-c[1]['event_seq']))
        # Store version deltas, not a full ever-growing task map at every
        # commit. Materialize only the two snapshots used by this smoke.
        for cutoff in {historical_seq,previous_seq}:
            snapshot = {}
            for task_id,versions in task_versions.items():
                older = [task for seq,task in versions if seq<=cutoff]
                if older:
                    snapshot[task_id] = deepcopy(older[-1])
            batches[cutoff]['tasks'] = snapshot
        return {'checkpoint':checkpoint,'run_id':run,'batches':batches,'events':events,
            'ledger':ledger,'history':history,'handoffs':handoffs,'selected':selected,
            'historical_seq':historical_seq,'last_checksum':checksum,
            'identity':{'checkpoint_sha256':envelope['sha256'],'journal_commits_verified':previous_seq,
                'business_events_verified':previous_event_seq,'journal_mirrored_seq':head[3],
                'final_custody_sha256':digest(ledger),'checkpoint_path':str(path),'journal_path':str(journal)}}
    finally:
        db.close()


def _all_goods(api,run,goods,seq):
    after, rows = 0, []
    while True:
        reply = api.goods_history(run,goods,seq=seq,limit=200,after_event_seq=after)
        if reply['seq'] != seq:
            raise AssertionError('query changed its requested historical cutoff')
        rows.extend(reply['rows'])
        if not reply['has_more']:
            return reply,rows
        cursor = reply['next_after_event_seq']
        if cursor is None or cursor <= after:
            raise AssertionError('history pagination failed to advance')
        after = cursor


def verify_correctness(mirror,source):
    cp, run = source['checkpoint'],source['run_id']
    final_seq = cp['knowledge_seq']
    point = mirror.committed_point(run)
    if not point or point['seq'] != final_seq or point['checksum'] != source['last_checksum']:
        raise AssertionError('graph watermark/checksum differs from finalized journal')
    graph_rows = mirror.query('''MATCH (g:WHCargo {run_id:$run}) USING INDEX g:WHCargo(run_id)
        WHERE g.current_location_id IS NOT NULL RETURN g.cargo_id AS goods_id,g.current_location_id AS location_id''',{'run':run})
    graph_ledger = {r['goods_id']:r['location_id'] for r in graph_rows}
    if len(graph_rows)!=len(graph_ledger) or graph_ledger!=source['ledger']:
        raise AssertionError('graph final cargo holders differ from independent journal replay')
    api = WideQueries(mirror)
    selected = source['selected']
    goods = selected['goods_id']
    checks = []
    for seq in [source['historical_seq'],final_seq]:
        reply,actual = _all_goods(api,run,goods,seq)
        expected = [r for r in source['history'][goods] if r['seq']<=seq]
        if [{k:r[k] for k in HISTORY_FIELDS} for r in actual] != expected:
            raise AssertionError('temporal cargo chain differs from source archive')
        expected_current = ({k:expected[-1][k] for k in ['location_id','event_seq','sim_time']} if expected else None)
        if reply['current_as_of'] != expected_current:
            raise AssertionError('past cargo query leaked a current/future holder')
        checks.append({'seq':seq,'current_as_of':expected_current,'rows':actual,'matches_source':True})
    task_id = selected['task_id']
    after, cooperation = 0, []
    while True:
        reply = api.cooperation_history(run,task_id=task_id,seq=final_seq,limit=100,after_event_seq=after)
        cooperation.extend(reply['rows'])
        if not reply['has_more']:
            break
        cursor = reply['next_after_event_seq']
        if cursor is None or cursor<=after:
            raise AssertionError('cooperation pagination failed to advance')
        after = cursor
    expected_handoffs = [e for e in source['handoffs'] if e['task_id']==task_id]
    actual_handoffs = [r['event'] for r in cooperation if r['event']['kind']=='cargo_handoff']
    if len(actual_handoffs)!=len(expected_handoffs):
        raise AssertionError('cooperation history lost or duplicated a handoff')
    for expected,actual in zip(expected_handoffs,actual_handoffs):
        for key in ['event_id','task_id','goods_id','location_id','reason']:
            if actual.get(key)!=expected.get(key):
                raise AssertionError('cooperation event payload differs from source')
        for role in ['donor','receiver']:
            if actual.get(role+'_robot_id',actual.get(role+'_id')) != expected.get(role+'_robot_id',expected.get(role+'_id')):
                raise AssertionError('cooperation participant differs from source')
    history_seq = source['historical_seq']
    owner = selected.get('donor_robot_id',selected.get('donor_id'))
    historical_robot = api.robot_context(run,owner,seq=history_seq)
    _verify_robot(historical_robot,source['batches'][history_seq],owner,history_seq)
    sim = type('FinalizedSource',(),{'run_id':run,'cursor':cp['cursor'],'inside':cp['inside']})()
    counts = audit_wide(mirror,sim,expected_handoffs=len(source['handoffs']))
    return {'journal_replay_matches_checkpoint':True,'graph_final_custody_matches_replay':True,
            'graph_final_custody_count':len(graph_ledger),'graph_final_custody_sha256':digest(graph_ledger),
            'selected_handoff':selected,'goods_queries':checks,'historical_robot':historical_robot,
            'historical_and_final_holder_differ':checks[0]['current_as_of']!=checks[1]['current_as_of'],
            'task_cooperation_events':cooperation,'cooperation_matches_source':True,'graph_audit':counts}


def _verify_robot(reply,batch,owner,seq):
    if reply['seq']!=seq or reply['robot']!=batch['robots'][owner]:
        raise AssertionError('robot snapshot differs from the committed source state')
    task_id = batch['robots'][owner].get('task_id')
    expected_task = batch['tasks'].get(task_id) if task_id else None
    if reply['task']!=expected_task:
        raise AssertionError('robot task context is not its historical task snapshot')


def _stats(values):
    values = sorted(values)
    if not values:
        return {'count':0,'mean_ms':None,'p95_ms':None,'p99_ms':None,'max_ms':None}
    return {'count':len(values),'mean_ms':sum(values)/len(values),
            'p95_ms':values[max(0,math.ceil(.95*len(values))-1)],
            'p99_ms':values[max(0,math.ceil(.99*len(values))-1)],'max_ms':values[-1]}


def concurrent_round(mirror,source,number):
    """100 ready workers launch together; timing includes both query API calls."""
    run = source['run_id']
    seq = source['checkpoint']['knowledge_seq']
    batch = source['batches'][seq]
    barrier = threading.Barrier(101,timeout=60)
    envelope_start = time.perf_counter()

    def worker(owner):
        started = None
        try:
            barrier.wait()
            started = time.perf_counter()
            reply = WideQueries(mirror).robot_context(run,owner,seq=seq)
            ended = time.perf_counter()
            _verify_robot(reply,batch,owner,seq)
            return {'robot_id':owner,'duration_ms':(ended-started)*1000,'ok':True,
                    'error_type':None,'_started':started,'_ended':ended}
        except Exception as exc:
            ended = time.perf_counter()
            return {'robot_id':owner,'duration_ms':(ended-started)*1000 if started is not None else None,
                    'ok':False,'error_type':type(exc).__name__,'_started':started,'_ended':ended}

    with ThreadPoolExecutor(max_workers=100) as pool:
        futures = [pool.submit(worker,owner) for owner in range(1,101)]
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass  # Worker records make incomplete concurrency an explicit failure.
        records = [future.result() for future in futures]
    envelope_seconds = time.perf_counter()-envelope_start
    starts = [r['_started'] for r in records if r['_started'] is not None]
    ends = [r['_ended'] for r in records if r['_started'] is not None]
    request_span = max(ends)-min(starts) if starts else None
    for record in records:
        record.pop('_started');record.pop('_ended')
    errors = [r for r in records if not r['ok']]
    return {'round':number,'requested_clients':100,'completed_requests':len(records),'errors':errors,
            'all_correct':not errors,'all_request_latency':_stats([r['duration_ms'] for r in records if r['duration_ms'] is not None]),
            'successful_request_latency':_stats([r['duration_ms'] for r in records if r['ok']]),
            'request_span_seconds':request_span,'envelope_seconds':envelope_seconds,
            'successful_throughput_qps':sum(r['ok'] for r in records)/request_span if request_span else 0.,
            'envelope_throughput_qps':sum(r['ok'] for r in records)/envelope_seconds,'records':records}


def verify(output,report=None):
    output = Path(output).resolve()
    report = Path(report).resolve() if report else output/'knowledge-smoke.json'
    if report.exists():
        raise FileExistsError('preserve prior smoke evidence; choose --report with a new filename')
    result = {'schema':'warehouse-wide-knowledge-smoke-v1','status':'failed',
        'scope':{'transport':'Python WideQueries -> Neo4j Bolt; not HTTP/browser',
            'dataset':'this finalized small run only','clients':100,'rounds':3,
            'latency':'full robot_context API, including cutoff query, state query and JSON parsing; barrier excluded',
            'warmup':'correctness checks precede measurements; no measurement round discarded'},
        'qualification':{'million_scale_evaluated':False,'specified_edge_hardware_evaluated':False,
            'vector_temporal_complex_queries_evaluated':False,'formal_500ms_requirement_passed':False,
            'full_task2_qualification_passed':False}}
    mirror = None
    started = time.perf_counter()
    stage = 'source_integrity'
    try:
        source = load_source(output)
        result['run_id'] = source['run_id']
        result['source'] = source['identity']
        stage = 'graph_connection'
        mirror = WideMirror.from_env()
        stage = 'temporal_correctness'
        result['correctness'] = verify_correctness(mirror,source)
        stage = '100_agent_concurrency'
        rounds = [concurrent_round(mirror,source,n) for n in range(1,4)]
        result['concurrency'] = {'rounds':rounds,'total_requests':300,
            'all_correct':all(r['all_correct'] for r in rounds),
            'total_errors':sum(len(r['errors']) for r in rounds)}
        if not result['concurrency']['all_correct']:
            raise AssertionError('one or more concurrent robot contexts failed correctness or transport')
        result['status'] = 'complete'
    except Exception as exc:
        result['error_type'] = type(exc).__name__
        result['failure_stage'] = stage
        if isinstance(getattr(exc,'code',None),str):
            result['neo4j_code'] = exc.code
        # Do not serialize arbitrary driver exception strings (connection/auth
        # information may be embedded). Our local contract errors are safe.
        if isinstance(exc,(AssertionError,ValueError)):
            result['error'] = str(exc)
    finally:
        if mirror is not None:
            mirror.close()
        result['wall_seconds'] = time.perf_counter()-started
        report.parent.mkdir(parents=True,exist_ok=True)
        report.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    print(json.dumps({'status':result['status'],'run_id':result.get('run_id'),'report':str(report),
        'error_type':result.get('error_type'),'total_errors':result.get('concurrency',{}).get('total_errors')},ensure_ascii=False))
    return 0 if result['status']=='complete' else 2


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',required=True)
    parser.add_argument('--report')
    args = parser.parse_args()
    raise SystemExit(verify(args.output,args.report))
