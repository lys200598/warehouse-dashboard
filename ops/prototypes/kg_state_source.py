"""Prototype: robot actors write their own graph records every step, a global writer marks Step(t),
dispatch can read robot state from the graph (KG_STATE_SOURCE=neo4j), and 100 agents fetch their
decision context concurrently while the run keeps writing.

Research tool, not product code. It subclasses the wheeled simulation without changing it. Labels
follow the C schema (Run-less: run_id property; Robot, AgentState, RuntimeEvent, Step). Writes go
to NEO4J_URI. Remove prototype data with --cleanup.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from backend.wheeled.sim import WheeledConfig, WheeledSimulation  # noqa: E402

SCENE = ROOT / 'data/layouts/expansion-candidates/medium/layout.json'
CONFIG = ROOT / 'config/wheeled-medium-100.json'
DATABASE = os.environ.get('NEO4J_DATABASE', 'neo4j')
SCHEMA = (
    'CREATE CONSTRAINT kgproto_robot_id IF NOT EXISTS FOR (n:Robot) REQUIRE n.id IS UNIQUE',
    'CREATE CONSTRAINT kgproto_state_id IF NOT EXISTS FOR (n:AgentState) REQUIRE n.id IS UNIQUE',
    'CREATE CONSTRAINT kgproto_event_id IF NOT EXISTS FOR (n:RuntimeEvent) REQUIRE n.id IS UNIQUE',
    'CREATE CONSTRAINT kgproto_step_id IF NOT EXISTS FOR (n:Step) REQUIRE n.id IS UNIQUE',
    'CREATE CONSTRAINT kgproto_run_id IF NOT EXISTS FOR (n:Run) REQUIRE n.id IS UNIQUE',
    'CREATE INDEX kgproto_robot_run IF NOT EXISTS FOR (n:Robot) ON (n.run_id)',
    'CREATE INDEX kgproto_state_robot_tick IF NOT EXISTS FOR (n:AgentState) ON (n.run_id, n.robot_id, n.tick)',
    'CREATE INDEX kgproto_event_robot_tick IF NOT EXISTS FOR (n:RuntimeEvent) ON (n.run_id, n.robot_id, n.tick)',
    'CREATE INDEX kgproto_step_run_tick IF NOT EXISTS FOR (n:Step) ON (n.run_id, n.tick)',
)
# One transaction per robot per step: current state on its Robot node, optional history, its events.
WRITE_ROBOT = '''MERGE (r:Robot {id: $rid})
SET r.run_id = $run, r.robot_id = $robot_id, r.last_written_tick = $tick, r.payload = $payload,
    r.x = $x, r.y = $y, r.battery = $battery, r.stage = $stage, r.task_id = $task_id
FOREACH (_ IN CASE WHEN $history THEN [1] ELSE [] END |
  MERGE (s:AgentState {id: $sid})
  SET s.run_id = $run, s.robot_id = $robot_id, s.tick = $tick, s.x = $x, s.y = $y,
      s.battery = $battery, s.stage = $stage, s.task_id = $task_id
  MERGE (s)-[:OF_ROBOT]->(r))
WITH r UNWIND $events AS e
MERGE (ev:RuntimeEvent {id: e.id})
SET ev.run_id = $run, ev.robot_id = $robot_id, ev.tick = $tick, ev.type = e.type, ev.task_id = e.task_id,
    ev.payload = e.payload
MERGE (ev)-[:ABOUT_ROBOT]->(r)'''
# The global writer also keeps Run.latest_step, so "latest complete step" is a key lookup, not a scan.
WRITE_STEP = '''MERGE (run:Run {id: $run}) SET run.latest_step = $tick
WITH run MERGE (st:Step {id: $id}) SET st.run_id = $run, st.tick = $tick, st.robots = $robots,
    st.working = $working, st.charging = $charging, st.idle = $idle, st.low_battery = $low, st.completed = $completed'''
READ_STATE = '''MATCH (run:Run {id: $run}) WITH run.latest_step AS t
MATCH (r:Robot {run_id: $run}) RETURN t, r.robot_id AS robot_id, r.last_written_tick AS tick, r.payload AS payload'''
# Decision-context pack for one agent (LaDe i-series style), all read the latest complete Step.
CONTEXT = {
    'latest_step': 'MATCH (run:Run {id: $run}) MATCH (st:Step {id: $run + ":step:" + toString(run.latest_step)}) RETURN st.tick AS t, st.working AS working, st.low_battery AS low',
    'self_state': 'MATCH (r:Robot {id: $rid}) RETURN r.x AS x, r.y AS y, r.battery AS battery, r.stage AS stage, r.task_id AS task_id',
    'recent_states': 'MATCH (s:AgentState {run_id: $run, robot_id: $robot_id}) WHERE s.tick <= $t AND s.tick > $t - 400 RETURN s.tick AS tick, s.stage AS stage, s.battery AS battery ORDER BY s.tick DESC LIMIT 20',
    'recent_events': 'MATCH (e:RuntimeEvent {run_id: $run, robot_id: $robot_id}) WHERE e.tick <= $t AND e.tick > $t - 600 RETURN e.tick AS tick, e.type AS type, e.task_id AS task_id ORDER BY e.tick DESC LIMIT 20',
    'nearby_robots': 'MATCH (me:Robot {id: $rid}) MATCH (o:Robot {run_id: $run}) WHERE o.id <> me.id AND abs(o.x - me.x) < 15 AND abs(o.y - me.y) < 15 RETURN o.robot_id AS id, o.stage AS stage, o.battery AS battery',
    'help_candidates': 'MATCH (me:Robot {id: $rid}) MATCH (o:Robot {run_id: $run}) WHERE o.id <> me.id AND o.stage = "idle" AND o.battery > 60 RETURN o.robot_id AS id, sqrt((o.x-me.x)^2 + (o.y-me.y)^2) AS d ORDER BY d LIMIT 5',
}


def driver():
    from neo4j import GraphDatabase
    return GraphDatabase.driver(os.environ['NEO4J_URI'], max_connection_pool_size=256,
                                auth=(os.environ.get('NEO4J_USER', 'neo4j'), os.environ['NEO4J_PASSWORD']))


_worker_driver = None


def _init_worker(uri, user, password):
    global _worker_driver
    from neo4j import GraphDatabase
    _worker_driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_pool_size=4)


def _job_write(params):
    with _worker_driver.session(database=DATABASE) as session:
        session.execute_write(lambda tx: tx.run(WRITE_ROBOT, **params).consume())


def context_for(graph, run_tag, robot_id, tick=None):
    params = {'run': run_tag, 'rid': f'{run_tag}:{robot_id}', 'robot_id': robot_id, 't': 0}
    timings, began = {}, time.perf_counter()
    with graph.session(database=DATABASE, default_access_mode='READ') as session:
        for name, query in CONTEXT.items():
            t0 = time.perf_counter()
            rows = session.run(query, **params).data()
            if name == 'latest_step':
                params['t'] = rows[0]['t'] if rows else 0
            timings[name] = (time.perf_counter() - t0) * 1000
    return {'tick': tick, 'total_ms': (time.perf_counter() - began) * 1000, **timings}


def _job_context(run_tag, robot_id, tick=None):
    return context_for(_worker_driver, run_tag, robot_id, tick)


class KGSimulation(WheeledSimulation):
    """Robots write per step; global writer marks Step(t); dispatch optionally reads from the graph."""

    def setup_graph(self, graph, run_tag, *, read_state=True, history_interval=4, context_every=0, batched=False, procs=False):
        self.graph, self.run_tag, self.read_state, self.batched = graph, run_tag, read_state, batched
        self.history_interval, self.context_every = history_interval, context_every
        self.procs = procs
        if procs:  # separate processes, no shared GIL: stands in for one Ray actor per robot
            from concurrent.futures import ProcessPoolExecutor
            from multiprocessing import get_context
            init = (os.environ['NEO4J_URI'], os.environ.get('NEO4J_USER', 'neo4j'), os.environ['NEO4J_PASSWORD'])
            make = lambda: ProcessPoolExecutor(max_workers=len(self.robots), mp_context=get_context('spawn'),
                                               initializer=_init_worker, initargs=init)
            self.writers, self.readers = make(), make()
        else:
            self.writers = ThreadPoolExecutor(max_workers=len(self.robots))
            self.readers = ThreadPoolExecutor(max_workers=len(self.robots))
        self.timing = {'robot_write_ms': [], 'step_write_ms': [], 'read_ms': []}
        self.contexts, self._events = [], {}
        original = self.event

        def capture(kind, robot_id=None, task_id=None, **fields):
            if robot_id is not None:
                self._events.setdefault(robot_id, []).append({'type': kind, 'task_id': task_id, 'fields': fields})
            return original(kind, robot_id, task_id, **fields)
        self.event = capture
        with graph.session(database=DATABASE) as session:
            for query in SCHEMA:
                session.run(query).consume()
        self._write_step()

    def _robot_params(self, robot_id, robot, tick, history, events):
        return {'rid': f'{self.run_tag}:{robot_id}', 'sid': f'{self.run_tag}:{robot_id}:{tick}', 'run': self.run_tag,
                  'robot_id': robot_id, 'tick': tick, 'history': history, 'payload': json.dumps(robot, sort_keys=True),
                  'x': robot['x'], 'y': robot['y'], 'battery': robot['battery'], 'stage': robot['stage'],
                  'task_id': robot.get('task_id'),
                  'events': [{'id': f'{self.run_tag}:{robot_id}:{tick}:{n}', 'type': e['type'], 'task_id': e['task_id'],
                              'payload': json.dumps(e['fields'], sort_keys=True, default=str)} for n, e in enumerate(events)]}

    def _write_robot(self, robot_id, robot, tick, history, events):
        params = self._robot_params(robot_id, robot, tick, history, events)
        with self.graph.session(database=DATABASE) as session:
            session.execute_write(lambda tx: tx.run(WRITE_ROBOT, **params).consume())

    def _write_all_in_one(self, tick, history, events):
        # Comparison only: all 100 robots in ONE transaction (LaDe-style central write).
        rows = [self._robot_params(i, r, tick, history, events.get(i, [])) for i, r in sorted(self.robots.items())]
        query = 'UNWIND $rows AS p CALL { WITH p ' + WRITE_ROBOT.replace('$', 'p.').replace('WITH r UNWIND', 'WITH r, p UNWIND') + ' } '
        with self.graph.session(database=DATABASE) as session:
            session.execute_write(lambda tx: tx.run(query, rows=rows).consume())

    def _write_step(self):
        tick, began = self.elapsed, time.perf_counter()
        events, self._events = self._events, {}
        history = tick % self.history_interval == 0
        if self.batched:
            self._write_all_in_one(tick, history, events)
        else:
            if self.procs:
                futures = [self.writers.submit(_job_write, self._robot_params(i, r, tick, history, events.get(i, [])))
                           for i, r in sorted(self.robots.items())]
            else:
                futures = [self.writers.submit(self._write_robot, i, r, tick, history, events.get(i, []))
                           for i, r in sorted(self.robots.items())]
            for future in futures:
                future.result()  # every robot's step receipt
        self.timing['robot_write_ms'].append((time.perf_counter() - began) * 1000)
        began = time.perf_counter()
        stages = [r['stage'] for r in self.robots.values()]
        summary = {'id': f'{self.run_tag}:step:{tick}', 'run': self.run_tag, 'tick': tick, 'robots': len(stages),
                   'working': sum(s not in ('idle', 'charging') for s in stages), 'charging': stages.count('charging'),
                   'idle': stages.count('idle'), 'low': sum(r['battery'] < 35 for r in self.robots.values()),
                   'completed': sum(t['status'] == 'completed' for t in self.tasks.values())}
        with self.graph.session(database=DATABASE) as session:
            session.execute_write(lambda tx: tx.run(WRITE_STEP, **summary).consume())
        self.timing['step_write_ms'].append((time.perf_counter() - began) * 1000)
        if self.context_every and tick and tick % self.context_every == 0:
            job = (lambda i: self.readers.submit(_job_context, self.run_tag, i, tick)) if self.procs else (lambda i: self.readers.submit(context_for, self.graph, self.run_tag, i, tick))
            self.contexts.extend(job(i) for i in sorted(self.robots))

    def _read_states(self):
        tick, began = self.elapsed, time.perf_counter()
        with self.graph.session(database=DATABASE) as session:
            rows = session.run(READ_STATE, run=self.run_tag).data()
        if not rows or rows[0]['t'] != tick or len(rows) != len(self.robots) or any(r['tick'] != tick for r in rows):
            raise RuntimeError(f'graph state for tick {tick} incomplete: {len(rows)} rows')
        for row in rows:
            robot = self.robots[row['robot_id']]
            robot.clear()
            robot.update(json.loads(row['payload']))
        self.timing['read_ms'].append((time.perf_counter() - began) * 1000)

    def release_and_dispatch(self):
        if getattr(self, 'graph', None) is not None and self.read_state:
            self._read_states()
        return super().release_and_dispatch()

    def step(self):
        super().step()
        if getattr(self, 'graph', None) is not None:
            self._write_step()


def summary(values):
    if not values:
        return None
    ordered = sorted(values)
    pick = lambda q: round(ordered[min(len(ordered) - 1, int(q * (len(ordered) - 1)))], 2)
    return {'n': len(values), 'mean': round(statistics.fmean(values), 2), 'p50': pick(.5), 'p95': pick(.95),
            'p99': pick(.99), 'max': round(ordered[-1], 2), 'over_500': sum(v > 500 for v in values)}


def run(mode, dataset, max_ticks, graph=None, run_tag=None, *, shift=True, read_state=True, context_every=0, batched=False, procs=False, bucket_seconds=7200):
    config = WheeledConfig(**json.loads(CONFIG.read_text(encoding='utf-8')))
    cls = KGSimulation if mode == 'neo4j' else WheeledSimulation
    sim = cls(SCENE, ROOT / dataset, run_tag or 'kgproto-memory', config)
    if shift:
        first = min(task['release_second'] for task in sim.tasks.values())
        for task in sim.tasks.values():
            task['release_second'] -= first
    if mode == 'neo4j':
        sim.setup_graph(graph, run_tag, read_state=read_state, context_every=context_every, batched=batched, procs=procs)
    seen, assignments, step_ms = {}, [], []
    began = time.perf_counter()
    for tick in range(1, max_ticks + 1):
        t0 = time.perf_counter()
        sim.step()
        step_ms.append((time.perf_counter() - t0) * 1000)
        for task_id, task in sorted(sim.tasks.items()):
            key = (task.get('robot_id'), task.get('assigned_at'))
            if task.get('robot_id') is not None and seen.get(task_id) != key:
                seen[task_id] = key
                assignments.append([tick, task_id, task['robot_id'], task.get('assigned_at')])
        if all(task['status'] == 'completed' for task in sim.tasks.values()):
            break
    result = {'mode': mode, 'ticks': tick, 'wall_s': round(time.perf_counter() - began, 2), 'assignments': assignments,
              'completed_at': {k: t.get('completed_at') for k, t in sorted(sim.tasks.items())}, 'step_ms': summary(step_ms)}
    if mode == 'neo4j':
        result.update({k: summary(v) for k, v in sim.timing.items()})
        done = [f.result() for f in sim.contexts if f.exception() is None]
        result['context_agents'] = len(sim.contexts)
        result['context_failures'] = sum(f.exception() is not None for f in sim.contexts)
        result['context_total_ms'] = summary([d['total_ms'] for d in done])
        result['context_query_ms'] = {name: summary([d[name] for d in done]) for name in CONTEXT}
        # Per time window (sim seconds), to see whether anything slows down as the run grows.
        span = max(1, bucket_seconds)
        buckets = {}
        for name, values in sim.timing.items():
            for i, v in enumerate(values):
                buckets.setdefault(i // span, {}).setdefault(name, []).append(v)
        for d in done:
            b = buckets.setdefault((d['tick'] or 0) // span, {})
            b.setdefault('context_total_ms', []).append(d['total_ms'])
            for name in CONTEXT:
                b.setdefault(name, []).append(d[name])
        result['by_window'] = {f'{k * span}-{(k + 1) * span}': {n: {x: v[x] for x in ('mean', 'p99', 'max')}
                               for n, vals in sorted(b.items()) for v in [summary(vals)]}
                               for k, b in sorted(buckets.items())}
        sim.writers.shutdown(); sim.readers.shutdown()
    return result


def filler(graph, run_tag, ticks, batch=20000):
    """History prefix for the SAME run at negative ticks: 100 robots x ticks AgentStates, ~1 event per
    robot per 6 ticks, and one Step per tick; the measured run then already holds this many nodes."""
    began, rows, made = time.perf_counter(), [], 0
    with graph.session(database=DATABASE) as session:
        for query in SCHEMA:
            session.run(query).consume()
        def flush(label, items):
            if label == 'AgentState':
                q = ('UNWIND $rows AS r CREATE (:AgentState {id: r.id, run_id: $run, robot_id: r.robot, tick: r.tick, '
                     'x: 1.0, y: 1.0, battery: 55.0, stage: "idle", task_id: null})')
            elif label == 'RuntimeEvent':
                q = ('UNWIND $rows AS r CREATE (:RuntimeEvent {id: r.id, run_id: $run, robot_id: r.robot, tick: r.tick, '
                     'type: "picked", task_id: "HIST", payload: "{}"})')
            else:
                q = 'UNWIND $rows AS r CREATE (:Step {id: r.id, run_id: $run, tick: r.tick, robots: 100, working: 40})'
            session.run(q, rows=items, run=run_tag).consume()
        for label, gen in (
                ('AgentState', ({'id': f'{run_tag}:{r}:{-t}', 'robot': r, 'tick': -t} for t in range(1, ticks + 1) for r in range(1, 101))),
                ('RuntimeEvent', ({'id': f'{run_tag}:{r}:{-t}:h', 'robot': r, 'tick': -t} for t in range(1, ticks + 1, 6) for r in range(1, 101))),
                ('Step', ({'id': f'{run_tag}:step:{-t}', 'tick': -t} for t in range(1, ticks + 1)))):
            for item in gen:
                rows.append(item)
                if len(rows) >= batch:
                    flush(label, rows); made += len(rows); rows = []
            if rows:
                flush(label, rows); made += len(rows); rows = []
    return made, round(time.perf_counter() - began, 1)


def counts(graph, run_tag):
    with graph.session(database=DATABASE) as session:
        rows = session.run('MATCH (n) WHERE n.run_id = $run RETURN labels(n)[0] AS label, count(*) AS c', run=run_tag).data()
    return {r['label']: r['c'] for r in rows}


def cleanup(graph, prefix):
    with graph.session(database=DATABASE) as session:
        session.run('MATCH (n) WHERE n.run_id STARTS WITH $p CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS',
                    p=prefix).consume()
        return session.run('MATCH (n) WHERE n.run_id STARTS WITH $p RETURN count(n) AS c', p=prefix).single()['c']


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--dataset', default='data/examples/wheeled-medium-sample19')
    p.add_argument('--max-ticks', type=int, default=4000)
    p.add_argument('--no-shift', action='store_true')
    p.add_argument('--run-tag', default='kgproto-run')
    p.add_argument('--compare', action='store_true', help='also run from memory and compare dispatch')
    p.add_argument('--no-read-state', action='store_true', help='dispatch from memory; graph is write-only')
    p.add_argument('--context-every', type=int, default=0, help='every N ticks, 100 agents fetch context')
    p.add_argument('--filler-ticks', type=int, default=0, help='history prefix of N ticks for the same run')
    p.add_argument('--batched', action='store_true', help='comparison: all robots in one transaction per step')
    p.add_argument('--procs', action='store_true', help='robot writers and context readers as separate processes')
    p.add_argument('--cleanup', action='store_true')
    p.add_argument('--output', type=Path)
    args = p.parse_args(argv)
    graph, out = driver(), {}
    try:
        if args.cleanup:
            print('remaining:', cleanup(graph, 'kgproto-')); return 0
        if args.filler_ticks:
            out['filler_nodes'], out['filler_s'] = filler(graph, args.run_tag, args.filler_ticks)
            print('filler', out['filler_nodes'], 'nodes in', out['filler_s'], 's', flush=True)
        kg = run('neo4j', args.dataset, args.max_ticks, graph, args.run_tag, shift=not args.no_shift,
                 read_state=not args.no_read_state, context_every=args.context_every, batched=args.batched, procs=args.procs)
        out['neo4j'] = {k: v for k, v in kg.items() if k not in ('assignments', 'completed_at')}
        if args.compare:
            mem = run('memory', args.dataset, args.max_ticks, shift=not args.no_shift)
            out['memory'] = {k: v for k, v in mem.items() if k not in ('assignments', 'completed_at')}
            out['dispatch_identical'] = (mem['assignments'] == kg['assignments'] and mem['completed_at'] == kg['completed_at']
                                         and mem['ticks'] == kg['ticks'])
            out['assignments'] = len(kg['assignments'])
        out['run_nodes'] = counts(graph, args.run_tag)
    finally:
        graph.close()
    text = json.dumps(out, ensure_ascii=False, indent=1)
    print(text)
    if args.output:
        args.output.write_text(text + '\n', encoding='utf-8')
    return 0 if out.get('dispatch_identical', True) else 1


if __name__ == '__main__':
    raise SystemExit(main())
