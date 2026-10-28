"""Read-only source reconstruction of knowledge-authorized physical handoffs.

The journal, final checkpoint and complete results are inputs. A new/reused
RealOracle is an independent indexed view of the ORIGINAL committed source;
no graph contact, scheduler mutation or acceptance-by-counts takes place here.
"""
import argparse
from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import tempfile
import zlib


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def file_sha(path):
    checksum = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b''):
            checksum.update(chunk)
    return checksum.hexdigest()


def integer(value, name, low=1, high=None):
    if type(value) is not int or value < low or high is not None and value > high:
        raise ValueError('invalid '+name)
    return value


def source_snapshot(path):
    """Read one consistent source prefix; keep events and compact batch proof."""
    db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True)
    try:
        db.execute('BEGIN')
        heads = db.execute('SELECT run_id,seq,checksum,checkpoint,mirrored_seq FROM heads').fetchall()
        if len(heads) != 1:
            raise ValueError('one original run per audit journal required')
        run, prefix, head_sha, head_blob, mirrored = heads[0]
        integer(prefix, 'source head')
        raw = zlib.decompress(head_blob)
        if hashlib.sha256(raw).hexdigest() != head_sha:
            raise ValueError('source head checksum mismatch')
        checkpoint = json.loads(raw)
        if checkpoint['run_id'] != run or type(checkpoint['knowledge_seq']) is not int or checkpoint['knowledge_seq'] != prefix:
            raise ValueError('source checkpoint identity mismatch')
        batches, events, task_hashes = {}, [], {}
        rolling, previous_event = hashlib.sha256(), 0
        for seq, checksum, blob in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq', (run,)):
            integer(seq, 'source sequence')
            if seq != len(batches)+1 or seq > prefix:
                raise ValueError('source commit sequence gap')
            raw = zlib.decompress(blob)
            batch = json.loads(raw)
            if hashlib.sha256(raw).hexdigest() != checksum or digest(batch) != checksum:
                raise ValueError('source batch checksum mismatch')
            if batch['run_id'] != run or type(batch['seq']) is not int or batch['seq'] != seq:
                raise ValueError('source batch identity mismatch')
            integer(batch['sim_time'], 'source simulation time', 0)
            if digest(batch['metadata']) != digest(checkpoint['run_metadata']) or digest(batch['config']) != digest(checkpoint['config']):
                raise ValueError('source metadata/configuration changed')
            batches[seq] = {'checksum': checksum, 'sim_time': batch['sim_time']}
            rolling.update(f'{seq}:{checksum}\n'.encode('utf-8'))
            for task in batch['tasks']:
                task_hashes.setdefault(task['task_id'], []).append((seq, digest(task)))
            for event in batch['events']:
                number = integer(event.get('event_seq'), 'source event sequence')
                if number != previous_event+1 or event.get('event_id') != f'{run}:event:{number}':
                    raise ValueError('source event sequence gap or foreign identity')
                if type(event.get('sim_time')) is not int or not 0 <= event['sim_time'] <= batch['sim_time']:
                    raise ValueError('source event has future/invalid effective time')
                previous_event = number
                events.append({'commit_seq': seq, 'event': event})
        if len(batches) != prefix or previous_event != checkpoint['event_sequence']:
            raise ValueError('incomplete source prefix or event archive')
        return {'run_id': run, 'prefix': prefix, 'head_sha256': head_sha, 'mirrored_seq': mirrored,
                'prefix_sha256': rolling.hexdigest(), 'checkpoint': checkpoint,
                'batches': batches, 'events': events, 'task_hashes': task_hashes}
    finally:
        db.close()


def _raw_event(oracle, event_seq):
    row = oracle._db().execute('SELECT payload FROM events WHERE event_seq=?', (event_seq,)).fetchone()
    if row is None:
        raise ValueError('source history event missing')
    return json.loads(row[0])


def reconstruct_context(oracle, seq, task_id, goods_id, decision_time, batches):
    """Reproduce exact QueryService+provider context, including JSON types.

    SQLite stores goods.sim_time in REAL columns. The graph originally stores
    the source event's numeric type. Restore from original JSON before hashing;
    blindly converting all floats or accepting False==0 would hide corruption.
    """
    integer(seq, 'decision source cutoff', 1, oracle.prefix)
    integer(decision_time, 'decision effective time', 0)
    task = oracle.task(seq, task_id)
    if task is None or task.get('goods_id') != goods_id:
        raise ValueError('decision refers to foreign/missing source task/cargo')
    goods = deepcopy(oracle.goods_history(seq, goods_id))
    cooperation = deepcopy(oracle.cooperation(seq, task_id))
    for history in (goods, cooperation):
        history['graph_committed_seq'] = seq
    for row in goods['rows']:
        original = _raw_event(oracle, row['event_seq'])
        if original.get('goods_id') != goods_id or original.get('location_id') != row['location_id']:
            raise ValueError('custody row differs from source event')
        row['sim_time'] = original['sim_time']
    if goods['current_as_of'] is not None:
        original = _raw_event(oracle, goods['current_as_of']['event_seq'])
        if original.get('goods_id') != goods_id:
            raise ValueError('current cargo provenance differs from source')
        goods['current_as_of']['sim_time'] = original['sim_time']
    source_time = batches[seq]['sim_time']
    return {'run_id': oracle.run_id, 'seq': seq, 'graph_committed_seq': seq,
            'batch_checksum': oracle.batch_checksum(seq), 'task_id': task_id, 'goods_id': goods_id,
            'focus_task': task, 'request_cursors': {'goods_after': 0, 'cooperation_after': 0},
            'goods_history': goods, 'cooperation': cooperation,
            'source_sim_time': source_time, 'observation_lag_seconds': decision_time-source_time}


def audit_events(source, oracle, *, max_lag_seconds=30, authorize_fn=None):
    """Every decision rebuilt; one authorization permits exactly one action."""
    if authorize_fn is None:
        from backend.knowledge_runtime.policy import authorize as authorize_fn
    integer(max_lag_seconds, 'maximum source lag', 0)
    errors, decisions, plans, transfers = [], [], [], []
    reservations, grants, actual_cargo = [], [], []
    seen, previous_event, previous_watermark = set(), 0, 0
    def require(condition, label):
        if not condition:
            raise ValueError(label)
    for entry in source['events']:
        event, commit = entry['event'], entry['commit_seq']
        number = event.get('event_seq')
        try:
            integer(number, 'event number')
            require(number not in seen and number > previous_event, 'duplicate/out-of-order event')
            previous_event = number
            seen.add(number)
            require(event.get('event_id') == f"{source['run_id']}:event:{number}", 'foreign event identity')
            kind = event.get('kind')
            if kind == 'handoff_reserved':
                reservations.append(event)
            elif kind == 'knowledge_decision':
                seq = integer(event.get('knowledge_seq'), 'knowledge cutoff', 1, oracle.prefix)
                require(seq < commit, 'decision cites uncommitted/future source batch')
                require(seq >= previous_watermark, 'decision source watermark moved backwards')
                previous_watermark = seq
                require(event.get('mode') == 'required', 'foreign knowledge decision mode')
                donor = integer(event.get('robot_id'), 'decision donor', 1, 100)
                transfer = event.get('decision') == 'authorize_transfer'
                require(event.get('decision') in ('authorize_transfer', 'authorize_assistance_planning'), 'unknown authorization action')
                receiver = event.get('receiver_robot_id')
                if transfer:
                    integer(receiver, 'decision receiver', 1, 100)
                    require(receiver != donor, 'same donor/receiver')
                else:
                    require(receiver is None, 'planning grant already selects receiver')
                task = oracle.task(seq, event['task_id'])
                require(task is not None, 'missing source decision task')
                goods = task['goods_id']
                context = reconstruct_context(oracle, seq, event['task_id'], goods,
                                              event['sim_time'], source['batches'])
                require(0 <= context['observation_lag_seconds'] <= max_lag_seconds, 'future/stale decision knowledge')
                require(event.get('knowledge_batch_checksum') == source['batches'][seq]['checksum'] == context['batch_checksum'],
                        'decision source checksum differs')
                versions = source.get('task_hashes', {}).get(event['task_id'], [])
                if versions:
                    expected_task_sha = next((sha for at, sha in reversed(versions) if at <= seq), None)
                    require(expected_task_sha == digest(task), 'oracle task differs from source delta')
                state = oracle.state(seq, donor)
                require(state.get('carrying') is True and state.get('task_id') == event['task_id']
                        and state.get('cargo_id') == goods, 'source donor is not the committed carrier')
                decision = authorize_fn(context, run=source['run_id'], donor=donor,
                                         task=task, goods=goods, receiver=receiver)
                require(event['decision'] == decision['action'], 'authorization replay changed action')
                require(event.get('context_sha256') == digest(context) == decision['context_sha256'], 'complete decision context fingerprint differs')
                require(digest(event.get('history_event_seqs')) == digest(decision['history_event_seqs']), 'decision evidence differs from source')
                require(all(type(x) is int and 0 < x < number for x in decision['history_event_seqs']), 'future/foreign decision evidence')
                grant = {'event': event, 'decision': decision, 'goods_id': goods,
                         'commit_seq': commit, 'consumed': False}
                decisions.append(grant)
                grants.append(grant)
            elif kind == 'knowledge_action_applied':
                seq = integer(event.get('knowledge_seq'), 'applied source cutoff', 1, oracle.prefix)
                require(seq < commit, 'applied action cites future source')
                action = event.get('action')
                if action == 'handoff_plan_reserved':
                    candidates = [g for g in grants if not g['consumed']
                        and g['event']['decision'] == 'authorize_assistance_planning'
                        and g['event']['task_id'] == event.get('task_id')
                        and g['event']['robot_id'] == event.get('robot_id')
                        and g['event']['knowledge_seq'] == seq]
                    require(len(candidates) == 1, 'missing/ambiguous/reused planning authorization')
                    grant = candidates[0]
                    receiver = integer(event.get('receiver_robot_id'), 'planned receiver', 1, 100)
                    matches = [r for r in reservations if grant['event']['event_seq'] < r['event_seq'] < number
                        and r.get('task_id') == event.get('task_id') and r.get('robot_id') == event.get('robot_id')
                        and r.get('receiver_robot_id') == receiver]
                    require(len(matches) == 1, 'plan action lacks one actual matching reservation')
                    require(grant['event']['sim_time'] == matches[0]['sim_time'] == event['sim_time'], 'plan action effective time differs')
                    grant['consumed'] = True
                    plans.append({'decision_event_seq': grant['event']['event_seq'],
                        'reservation_event_seq': matches[0]['event_seq'], 'applied_event_seq': number,
                        'knowledge_seq': seq, 'task_id': event['task_id'], 'goods_id': grant['goods_id'],
                        'donor_robot_id': event['robot_id'], 'receiver_robot_id': receiver})
                elif action == 'cargo_transferred':
                    donor = integer(event.get('donor_robot_id'), 'applied donor', 1, 100)
                    receiver = integer(event.get('receiver_robot_id'), 'applied receiver', 1, 100)
                    require(event.get('robot_id') == receiver, 'applied actor is not receiver')
                    matches = [h for h in actual_cargo if not h['applied']
                        and h['event']['task_id'] == event.get('task_id')
                        and h['event']['goods_id'] == event.get('goods_id')
                        and h['donor'] == donor and h['receiver'] == receiver
                        and h['grant']['event']['knowledge_seq'] == seq]
                    require(len(matches) == 1, 'missing/ambiguous/reused physical custody event')
                    handoff = matches[0]
                    require(handoff['event']['event_seq'] < number and handoff['event']['sim_time'] == event['sim_time'], 'applied action predates actual custody transfer')
                    handoff['applied'] = True
                    transfers.append({'decision_event_seq': handoff['grant']['event']['event_seq'],
                        'cargo_handoff_event_seq': handoff['event']['event_seq'], 'applied_event_seq': number,
                        'knowledge_seq': seq, 'task_id': event['task_id'], 'goods_id': event['goods_id'],
                        'donor_robot_id': donor, 'receiver_robot_id': receiver})
                else:
                    raise ValueError('unknown knowledge-applied action')
            elif kind == 'cargo_handoff':
                donor = integer(event.get('donor_robot_id', event.get('donor_id')), 'actual donor', 1, 100)
                receiver = integer(event.get('receiver_robot_id', event.get('receiver_id')), 'actual receiver', 1, 100)
                for name, expected in [('donor', donor), ('receiver', receiver)]:
                    require(all(type(event[k]) is int and event[k] == expected
                                for k in (name+'_id', name+'_robot_id') if k in event), 'conflicting actual participant aliases')
                require(donor != receiver and event.get('robot_id') == receiver
                        and event.get('location_id') == f'ROBOT-{receiver}', 'actual transfer identity/custody differs')
                candidates = [g for g in grants if not g['consumed']
                    and g['event']['decision'] == 'authorize_transfer'
                    and g['event']['task_id'] == event.get('task_id') and g['goods_id'] == event.get('goods_id')
                    and g['event']['robot_id'] == donor and g['event']['receiver_robot_id'] == receiver]
                require(len(candidates) == 1, 'actual cargo transfer lacks one unused knowledge authorization')
                grant = candidates[0]
                require(grant['event']['event_seq'] < number and grant['event']['sim_time']+1 == event['sim_time'], 'transfer authorization was not the preceding physical step')
                plan = [p for p in plans if p['task_id'] == event.get('task_id') and p['goods_id'] == event.get('goods_id')
                    and p['donor_robot_id'] == donor and p['receiver_robot_id'] == receiver
                    and p['applied_event_seq'] < grant['event']['event_seq']
                    and p.get('cargo_handoff_event_seq') is None]
                require(len(plan) == 1, 'actual transfer lacks one prior authorized plan')
                plan[0]['cargo_handoff_event_seq'] = number
                grant['consumed'] = True
                actual_cargo.append({'event': event, 'grant': grant, 'donor': donor,
                                     'receiver': receiver, 'applied': False})
        except Exception as exc:
            errors.append({'event_seq': number, 'commit_seq': commit,
                           'kind': event.get('kind'), 'error': type(exc).__name__+': '+str(exc)[:1500]})
    unused = [g['event']['event_seq'] for g in decisions if not g['consumed']]
    unapplied = [h['event']['event_seq'] for h in actual_cargo if not h['applied']]
    handoffs = sum(e['event'].get('kind') == 'cargo_handoff' for e in source['events'])
    applied_count = sum(e['event'].get('kind') == 'knowledge_action_applied' for e in source['events'])
    counts = {'decisions': len(decisions), 'planning_decisions': sum(g['event']['decision']=='authorize_assistance_planning' for g in decisions),
              'transfer_decisions': sum(g['event']['decision']=='authorize_transfer' for g in decisions),
              'actual_cargo_handoffs': handoffs, 'authorized_plans_applied': len(plans),
              'authorized_transfers_applied': len(transfers), 'logged_applied_actions': applied_count}
    passed = not errors and not unused and not unapplied and handoffs == len(transfers) and applied_count == len(plans)+len(transfers)
    return {'passed': passed, 'counts': counts, 'errors': errors,
            'unused_authorization_event_seqs': unused, 'unapplied_handoff_event_seqs': unapplied,
            'plans': plans, 'transfers': transfers,
            'decisions': [{'event_seq': g['event']['event_seq'], 'commit_seq': g['commit_seq'],
                **g['decision'], 'source_sim_time': source['batches'][g['decision']['seq']]['sim_time'],
                'decision_sim_time': g['event']['sim_time']} for g in decisions]}


def audit(journal, results_path, oracle_dir, *, max_lag_seconds=30):
    from backend.knowledge_runtime import VERSION
    from backend.real_scale.writer import real_identity
    from backend.real_scale.oracle import RealOracle
    journal, results_path = Path(journal).resolve(), Path(results_path).resolve()
    before = {'journal': file_sha(journal), 'results': file_sha(results_path)}
    source = source_snapshot(journal)
    results = json.loads(results_path.read_text(encoding='utf-8'))
    oracle_dir = Path(oracle_dir).resolve()
    oracle_dir.mkdir(parents=True, exist_ok=True)
    index = oracle_dir/'oracle.sqlite'
    oracle = RealOracle.open(index) if index.exists() else RealOracle(journal, index_path=index, run_id=source['run_id'])
    checks = {}
    def check(name, condition):
        checks[name] = bool(condition)
    try:
        identity, checkpoint = oracle.identity, source['checkpoint']
        check('oracle_current_source_prefix', identity['run_id'] == source['run_id']
              and identity['prefix'] == source['prefix'] and identity['prefix_sha256'] == source['prefix_sha256']
              and identity['head_checkpoint_sha256'] == source['head_sha256'])
        check('oracle_source_metadata_config', digest(identity['metadata']) == digest(checkpoint['run_metadata'])
              and digest(identity['config']) == digest(checkpoint['config']))
        expected_events = source['events']
        oracle_events = oracle._db().execute('SELECT seq,payload FROM events ORDER BY event_seq')
        matched = 0
        for entry, indexed in zip(expected_events, oracle_events):
            if entry['commit_seq'] != indexed[0] or digest(entry['event']) != digest(json.loads(indexed[1])):
                break
            matched += 1
        indexed_count = oracle._db().execute('SELECT count(*) FROM events').fetchone()[0]
        check('every_oracle_event_matches_original_source', matched == len(expected_events) == identity['events'] == indexed_count)
        runtime = checkpoint['knowledge_runtime']
        check('required_live_history_mode', runtime['identity']['mode'] == 'required'
              and runtime['identity']['version'] == VERSION
              and digest(runtime['identity']) == digest(checkpoint['run_metadata']['knowledge_runtime']))
        cadence = checkpoint['real_scale']['identity']['commit_seconds']
        check('selected_runtime_source_matches', runtime['identity']['source'] == real_identity(cadence)['execution_sha256'])
        check('results_execution_fingerprint', results['code_identity'] == digest(runtime['identity'])
              == checkpoint['run_metadata']['execution_code_sha256'])
        check('results_same_committed_run', results['run_id'] == source['run_id']
              and results['elapsed'] == checkpoint['elapsed'] and results['status'] == 'complete'
              and results['error'] is None and digest(results['config']) == digest(checkpoint['config']))
        check('results_runtime_identity_and_counters', digest(results['scheduler']['knowledge_runtime']) == digest(runtime))
        check('results_input_provenance', all(results['input_identity'].get(key) == checkpoint['run_metadata'].get(key)
              for key in ('scene_sha256', 'tasks_sha256', 'initial_inventory_sha256', 'manifest_sha256')))
        check('journal_mirrored_at_final_head', source['mirrored_seq'] == source['prefix']
              and results['knowledge']['durable_seq'] == results['knowledge']['mirrored_seq'] == source['prefix']
              and results['knowledge']['last_error'] is None)
        chain = audit_events(source, oracle, max_lag_seconds=max_lag_seconds)
        check('every_decision_and_action_source_replayed', chain['passed'])
        counts, stats = chain['counts'], runtime['stats']
        check('counters_match_reconstructed_control_events', stats['queries'] == counts['decisions']+stats['rejected']
              and stats['approved_plans'] == counts['planning_decisions']
              and stats['approved_transfers'] == counts['actual_cargo_handoffs'] == counts['authorized_transfers_applied']
              and results['metrics']['handoffs'] == counts['actual_cargo_handoffs'])
        check('all_counter_types_valid', all(type(stats.get(key)) is int and stats[key] >= 0
              for key in ('queries','approved_plans','approved_transfers','rejected','helper_order_changes'))
              and isinstance(stats['query_seconds'], (int, float)) and not isinstance(stats['query_seconds'], bool)
              and math.isfinite(stats['query_seconds']) and stats['query_seconds'] >= 0)
        check('physical_handoff_was_actually_exercised', counts['actual_cargo_handoffs'] > 0)
        after = {'journal': file_sha(journal), 'results': file_sha(results_path)}
        check('source_inputs_unchanged', before == after)
        watermarks = [d['seq'] for d in chain['decisions']]
        lags = [d['decision_sim_time']-d['source_sim_time'] for d in chain['decisions']]
        historical = sorted({owner for d in chain['decisions'] for owner in d['historical_donor_robot_ids']})
        backend = Path(sys.modules['backend'].__file__).parent
        loaded = {f'backend/knowledge_runtime/{name}': file_sha(backend/'knowledge_runtime'/name)
                  for name in ('__init__.py','policy.py','provider.py','sim.py','runner.py')}
        return {'passed': all(checks.values()), 'checks': checks,
            'failed_checks': [name for name, value in checks.items() if not value],
            'scope': 'every live required-mode decision rebuilt from original source; grants/reservations/physical cargo/actions causally matched; no graph-network or latency certification',
            'run_id': source['run_id'], 'prefix': source['prefix'], 'source_prefix_sha256': source['prefix_sha256'],
            'source_files_before': before, 'source_files_after': after, 'oracle_identity': identity,
            'oracle_file_sha256': file_sha(index), 'selected_runtime_files': loaded,
            'reported_runtime_stats': stats, 'source_context_lag_budget_sim_seconds': max_lag_seconds,
            'knowledge_watermarks': {'minimum_seq': min(watermarks, default=None),
                'maximum_seq': max(watermarks, default=None), 'distinct_sequences': sorted(set(watermarks)),
                'minimum_observation_lag_sim_seconds': min(lags, default=None),
                'maximum_observation_lag_sim_seconds': max(lags, default=None)},
            'participants': {'actual_donor_robot_ids': sorted({t['donor_robot_id'] for t in chain['transfers']}),
                'actual_receiver_robot_ids': sorted({t['receiver_robot_id'] for t in chain['transfers']}),
                'historical_donor_robot_ids_used': historical},
            'control_chain': chain}
    finally:
        oracle.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', required=True, type=Path)
    parser.add_argument('--results', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--oracle-dir', type=Path)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--max-lag-seconds', type=int, default=30)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error('preserve previous audit; choose a new output file')
    if args.max_lag_seconds < 0:
        parser.error('nonnegative source lag required')
    selected = args.source_root
    if selected is None:
        candidates = [Path(__file__).resolve().parents[1]/'warehouse-dashboard', Path(__file__).resolve().parents[1]]
        selected = next((p for p in candidates if (p/'backend/knowledge_runtime').is_dir()), None)
    if selected is None or not (selected/'backend/knowledge_runtime/policy.py').is_file():
        parser.error('select the frozen producing source with --source-root')
    sys.path.insert(0, str(selected.resolve()))
    loaded_backend = sys.modules.get('backend')
    if loaded_backend is not None and Path(loaded_backend.__file__).resolve().parent != (selected/'backend').resolve():
        parser.error('another backend is already loaded; run this audit in a fresh selected-source process')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = tempfile.TemporaryDirectory(prefix='knowledge-runtime-oracle-', dir=args.output.parent) if args.oracle_dir is None else None
    try:
        try:
            result = audit(args.journal, args.results, args.oracle_dir or Path(temporary.name),
                           max_lag_seconds=args.max_lag_seconds)
        except Exception as exc:
            result = {'passed': False, 'scope': 'fail-closed source/control audit',
                      'error': type(exc).__name__+': '+str(exc)[:3000]}
        with args.output.open('x', encoding='utf-8') as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        print(canonical({'passed': result['passed'], 'failed_checks': result.get('failed_checks'),
                         'error': result.get('error')}))
        return 0 if result['passed'] else 2
    finally:
        if temporary is not None:
            temporary.cleanup()


if __name__ == '__main__':
    raise SystemExit(main())
