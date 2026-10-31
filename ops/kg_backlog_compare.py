"""Bounded, real memory+async graph backlog comparison; never manages services.

Run from an isolated/frozen repository after other owned experiments finish.
Requires an existing Ray 27380, writer 14591 and synthetic Neo4j 17695.
Only --help / compilation are safe preparation checks; running creates test data.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import time
import traceback
from urllib.parse import urlparse
import uuid


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    temporary.replace(path)


def distribution(values):
    ordered = sorted(values)
    return {'n': len(ordered), 'mean_ms': statistics.fmean(ordered) if ordered else None,
            'p99_ms': ordered[math.ceil(len(ordered)*.99)-1] if ordered else None,
            'max_ms': ordered[-1] if ordered else None,
            'over_1000ms': sum(v > 1000 for v in ordered),
            'over_5000ms': sum(v > 5000 for v in ordered)}


def source_hashes(root):
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for area in ('backend', 'tests', 'ops') for p in sorted((root/area).rglob('*.py'))}


def normalize_business(value, run_id):
    # Preserve every business field, timestamp, list order and numeric value.
    if isinstance(value, dict):
        return {k: normalize_business(v, run_id) for k,v in value.items()}
    if isinstance(value, list):
        return [normalize_business(v, run_id) for v in value]
    return value.replace(run_id, '<RUN>') if isinstance(value, str) else value


def read_jsonl(path):
    raw = path.read_bytes()
    assert not raw or raw.endswith(b'\n'), f'incomplete log after flush: {path}'
    return [json.loads(line) for line in raw.splitlines()]


def run_group(args, root, backlog, run_id, folder, expected):
    from tests import test_dispatch_golden as golden
    from tests import test_robot_process_kg as kg
    from backend.robot_actor.ray_actor import RayRobotProcessSplit
    from backend.wheeled.sim import WheeledSimulation
    from ops.kg_long_run import graph_config, run_exists

    folder.mkdir(exist_ok=False)
    result = {'status': 'running', 'run_id': run_id, 'backlog': backlog,
              'requested_ticks': args.ticks, 'kg_state_source': 'memory', 'robot_graph_async': True,
              'robot_context_interval': 10, 'history_interval': 4,
              'timing_note': 'Main-thread phase write_ms is not worker cost. End-to-end throughput includes final drain, excludes audit and cleanup.'}
    save(folder/'result.json', result)
    instances, physical = [], []
    began = time.perf_counter()
    first_step = last_step_end = None
    failure = None
    actual = None
    kg.URL = args.global_writer_url  # existing helper reads its module-level URL

    class Engine(RayRobotProcessSplit, WheeledSimulation):
        ray_address = args.ray_address
        ray_actor_cpus = .02

        def __init__(self, scene, dataset, ignored_run_id, config):
            super().__init__(scene, dataset, run_id, config, robot_graph=True,
                graph_state_interval=4, kg_state_source='memory',
                global_writer_url=args.global_writer_url, robot_graph_async=True,
                robot_graph_backlog=backlog, robot_context_interval=10)
            instances.append(self)
            self.graph_timing_log_path = str(folder/'robot-graph-timing.jsonl')
            self.context_log_path = str(folder/'context.jsonl')
            self._robot_pool()  # Ray.init may replace SIGTERM; restore drain/cleanup after it.
            signal.signal(signal.SIGTERM, args._terminate_handler)

        def step(self):
            nonlocal first_step, last_step_end
            start = time.perf_counter()
            if first_step is None:
                first_step = start
            super().step()
            last_step_end = time.perf_counter()
            physical.append({'tick': self.tick, **self.kg_timing,
                             'step_call_ms': (last_step_end-start)*1000})
            if self.tick % 50 == 0:
                save(folder/'progress.json', {'tick': self.tick,
                    'wall_seconds': last_step_end-began, 'queue': self.graph_backpressure_status(),
                    'graph': self.frame().get('robotGraph', {})})

    try:
        cfg = graph_config()
        assert not run_exists(cfg, run_id), 'refuse pre-existing run ID'
        start = time.perf_counter()
        actual, sim = golden.record_relay(sim_class=Engine, with_sim=True, max_ticks=args.ticks)
        physical_end = time.perf_counter()
        save(folder/'dispatch.json', actual)
        save(folder/'physical-timing.json', physical)
        assert actual == expected, 'dispatch/handoff/completion facts differ from memory reference'
        assert sim.tick == args.ticks, 'scenario ended before requested ticks; not a full requested trial'
        result['queue_at_physical_end'] = sim.graph_backpressure_status()
        result['graph_at_physical_end'] = sim.frame().get('robotGraph', {})
        drain_start = time.perf_counter()
        sim.flush_robot_graph(timeout=args.flush_timeout)
        if sim._step_client is not None:
            sim._step_client.flush(args.flush_timeout)
        drained = time.perf_counter()
        result['timing_seconds'] = {
            'simulation_construction_and_physical_loop': physical_end-start,
            'physical_loop_elapsed': last_step_end-first_step,
            'physical_step_calls_sum': sum(r['step_call_ms'] for r in physical)/1000,
            'tail_drain': drained-drain_start,
            'run_including_tail_drain': drained-start,
            'physical_loop_including_tail_drain': drained-first_step,
            'simulation_seconds_per_wall_second_including_drain': sim.tick/(drained-start)}
        result['queue_after_flush'] = sim.graph_backpressure_status()
        result['graph_after_flush'] = sim.frame().get('robotGraph', {})
        assert result['queue_after_flush']['pending'] == 0
        assert result['queue_after_flush']['max_pending'] <= backlog
        assert result['queue_after_flush']['confirmed_tick'] == args.ticks
        assert result['graph_after_flush']['errors'] == 0
        assert result['graph_after_flush']['contextErrors'] == 0
        result['graph_audit'] = kg.audit(sim)
        assert result['graph_audit']['step']['count'] == args.ticks+1
        if args.ticks >= 400:
            assert actual['handoffs'], '400-step relay did not exercise handoff'
            assert any(r['receiver_id'] == 6 for r in actual['handoffs'])
            assert any(r['receiver'] == 6 and r['owner'] == 6 for r in result['graph_audit']['owners'])
        pool = sim._robot_pool()
        assert len(pool.actors) == 100
        result['ray_cluster_resources'] = pool.ray.cluster_resources()
        context_batches = read_jsonl(folder/'context.jsonl')
        assert [batch['tick'] for batch in context_batches] == list(range(0,args.ticks+1,10))
        contexts = [row for batch in context_batches for row in batch['robots']]
        assert not [r for r in contexts if r.get('error')]
        by_tick = {}
        for row in contexts:
            by_tick.setdefault(row['tick'], []).append(row)
        assert sorted(by_tick) == list(range(0,args.ticks+1,10)), 'missing context round'
        for tick, rows in by_tick.items():
            assert sorted(r['robot_id'] for r in rows) == sorted(sim.robots), f'context coverage tick {tick}'
            assert len({r['pid'] for r in rows}) == 100, f'not 100 actor processes at {tick}'
        result['context_audit'] = {'rounds':len(by_tick), 'samples':len(contexts),
            'independent_actor_pids':len({r['pid'] for r in contexts}),
            'errors':0, 'total_ms':distribution([r['total_ms'] for r in contexts])}
        rows = read_jsonl(folder/'robot-graph-timing.jsonl')
        assert [r['tick'] for r in rows] == list(range(args.ticks+1)), 'missing worker timing'
        assert not [r for r in rows if r.get('error')]
        result['worker_timings'] = {}
        for name, selected in (('all_including_tick0',rows),
                ('excluding_tick0',[r for r in rows if r['tick'] > 0]),
                ('history_after_warmup',[r for r in rows if r['tick'] > 20 and r['tick'] % 4 == 0]),
                ('all_after_warmup',[r for r in rows if r['tick'] > 20])):
            result['worker_timings'][name] = {phase: distribution([r[phase] for r in selected])
                for phase in ('write_ms','wait_step_ms','context_ms')}
        result['status'] = 'passed_pending_cleanup'
    except BaseException as exc:
        failure = exc
        result.update(status='failed', error=repr(exc), traceback=traceback.format_exc())
    finally:
        save(folder/'physical-timing.json', physical)
        # Existing helper closes/drains before DELETE and verifies zero nodes;
        # on uncertain in-flight work it refuses DELETE and preserves the run.
        for sim in instances:
            cleanup_start = time.perf_counter()
            try:
                kg.cleanup(sim)
                result['cleanup'] = {'status':'complete','remaining_nodes':0,
                                     'wall_seconds':time.perf_counter()-cleanup_start}
            except BaseException as exc:
                result['cleanup'] = {'status':'failed_or_blocked_inflight', 'error':repr(exc),
                    'resolution':'inspect existing run/queue; no automatic retry or service stop',
                    'wall_seconds':time.perf_counter()-cleanup_start}
                result['status'] = 'failed'
                if failure is None:
                    failure = exc
        if result['status'] == 'passed_pending_cleanup':
            result['status'] = 'passed'
        result['whole_group_including_audit_cleanup_seconds'] = time.perf_counter()-began
        save(folder/'result.json', result)
    if failure is not None:
        raise failure
    return result, actual


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--ticks',type=int,default=400)
    parser.add_argument('--backlogs',type=int,nargs='+',default=[8,120])
    parser.add_argument('--run-prefix',default='wz-test-backlog')
    parser.add_argument('--ray-address',default='127.0.0.1:27380')
    parser.add_argument('--global-writer-url',default='http://127.0.0.1:14591')
    parser.add_argument('--flush-timeout',type=float,default=300.)
    args=parser.parse_args()
    if not 1 <= args.ticks <= 4000:
        parser.error('--ticks must be within 1..4000 (400 exercises the relay)')
    if any(not 1 <= b <= 128 for b in args.backlogs) or len(set(args.backlogs)) != len(args.backlogs):
        parser.error('--backlogs must be distinct integers within 1..128')
    if not re.fullmatch(r'wz-test-[A-Za-z0-9_-]+',args.run_prefix):
        parser.error('--run-prefix must be a safe wz-test- identifier')
    if not math.isfinite(args.flush_timeout) or args.flush_timeout <= 0:
        parser.error('--flush-timeout must be finite and positive')
    if args.ray_address != '127.0.0.1:27380' or args.global_writer_url.rstrip('/') != 'http://127.0.0.1:14591':
        parser.error('only owned Ray27380 and writer14591 are accepted')
    root=Path.cwd().resolve()
    if not (root/'tests/test_dispatch_golden.py').is_file():
        parser.error('run from the frozen repository root')
    sys.path.insert(0,str(root))
    os.environ['RAY_AUTH_MODE']='disabled'
    from ops.kg_long_run import graph_config
    uri=urlparse(graph_config()['uri'])
    if uri.hostname != '127.0.0.1' or uri.port != 17695:
        parser.error('this isolated comparison requires Neo4j127.0.0.1:17695')
    args.output=args.output.resolve()
    args.output.mkdir(parents=True,exist_ok=False)
    sources=source_hashes(root)
    state={'status':'running','groups':[], 'expected_source_base':'8fa9bbf',
           'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
           'source_changes':subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=root,text=True).strip(),
           'sources':sources, 'ticks':args.ticks,'backlogs':args.backlogs,
           'qualification':'Bounded relay comparison only; not an all-day or rare-tail guarantee',
           'throughput_comparison':'Compare run_including_tail_drain, not physical-loop-only time'}
    save(args.output/'comparison.json',state)
    previous_handler=signal.getsignal(signal.SIGTERM)
    def terminated(signum, frame):
        raise KeyboardInterrupt('SIGTERM: drain/close before cleanup')
    signal.signal(signal.SIGTERM,terminated)
    args._terminate_handler=terminated
    try:
        from tests import test_dispatch_golden as golden
        expected=golden.record_relay(max_ticks=args.ticks)
        save(args.output/'memory-reference.json',expected)
        suffix=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
        normalized=[]
        for backlog in args.backlogs:
            assert source_hashes(root)==sources, 'source changed between groups'
            rid=f'{args.run_prefix}-{suffix}-b{backlog}'
            result, actual=run_group(args,root,backlog,rid,args.output/f'backlog-{backlog}',expected)
            state['groups'].append(result)
            normalized.append(normalize_business(actual,rid))
            save(args.output/'comparison.json',state)
        assert all(value==normalized[0] for value in normalized), 'backlog variants differ'
        assert source_hashes(root)==sources, 'source changed during comparison'
        state.update(status='passed',equivalent_business_facts=True)
    except BaseException as exc:
        state.update(status='failed',error=repr(exc),traceback=traceback.format_exc())
        raise
    finally:
        signal.signal(signal.SIGTERM,previous_handler)
        save(args.output/'comparison.json',state)
    print(json.dumps({'status':state['status'],'output':str(args.output)},ensure_ascii=False))


if __name__=='__main__':
    main()
