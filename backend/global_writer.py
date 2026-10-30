"""Plain-process global writer for the V5 live runtime.

This is intentionally not a Ray actor: it is a single child process with a
bounded multiprocessing queue.  The simulation owner only enqueues committed
global records; Neo4j latency never blocks the physics tick.

Responsibilities:
- one RunStat node per committed global record;
- Run global summary fields;
- global RuntimeEvent rows (robot_id is null), e.g. cross-robot assistance
  records that do not belong to one robot actor.

Per-robot Robot/AgentState/Execution/Event rows remain owned by the Ray robot
actors when --robot-graph is enabled.
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import queue
import time
from collections import Counter
from typing import Any

GLOBAL_EVENT_SKIP = {'initial_stock'}
GLOBAL_EVENT_LIMIT = 200


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def build_dispatch_state(sim, *, commit_seq: int, identity: dict[str, Any] | None = None) -> dict[str, Any]:
    """Bounded dispatch snapshot for the future KG_STATE_SOURCE=neo4j path.

    It intentionally contains summaries and bounded queues, not a full 100-robot
    position dump; per-robot positions remain AgentState rows.
    """
    identity = identity or {}
    jobs = []
    for owner, job in sorted(getattr(sim, 'jobs', {}).items()):
        parts = job.get('parts') or []
        idx = min(max(0, int(job.get('phase_index', 0))), len(parts) - 1) if parts else 0
        part = parts[idx] if parts else {}
        jobs.append({
            'robot_id': int(owner),
            'task_id': job.get('task_id'),
            'task_ids': list(job.get('task_ids') or []),
            'dock_id': job.get('dock_id'),
            'phase': part.get('phase'),
            'phase_index': idx,
            'parts': len(parts),
            'start': job.get('start'),
            'end': parts[-1].get('end') if parts else None,
            'assist_donor': job.get('assist_donor'),
        })
    tasks = getattr(sim, 'tasks', {})
    pending = []
    for task_id in (getattr(sim, 'pending', []) or [])[:200]:
        task = tasks.get(task_id)
        if not task:
            continue
        pending.append({
            'task_id': task_id,
            'status': task.get('status'),
            'task_type': task.get('task_type'),
            'source_id': task.get('source_id'),
            'target_id': task.get('target_id'),
            'release_second': task.get('release_second'),
            'due_second': task.get('due_second'),
        })
    robots = list(getattr(sim, 'robots', {}).values())
    batteries = [float(robot.get('battery', 0.0)) for robot in robots]
    stage_counts = Counter(str(robot.get('stage', 'idle')) for robot in robots)
    task_counts = Counter(str(task.get('status', 'unknown')) for task in tasks.values())
    ports = {str(key): int(value) for key, value in (getattr(sim, 'ports', {}) or {}).items()}
    reservations = getattr(sim, 'reservations', None)
    fixed = getattr(reservations, 'fixed', {}) if reservations is not None else {}
    return {
        'schema_version': 1,
        'run_id': str(sim.run_id),
        'commit_seq': int(commit_seq),
        'tick': int(sim.tick),
        'elapsed': int(sim.elapsed),
        'generated_at_ms': int(time.time() * 1000),
        'source': 'local-v5',
        'backend': str(sim.run_metadata.get('backend', 'warehouse-v5')),
        'dataset_id': identity.get('graph_dataset_id') or identity.get('dataset_id'),
        'active_jobs': jobs,
        'active_job_count': len(jobs),
        'pending': pending,
        'pending_count': len(getattr(sim, 'pending', []) or []),
        'ports': ports,
        'robots': {
            'count': len(robots),
            'stage_counts': dict(stage_counts),
            'carrying': sum(bool(robot.get('carrying')) for robot in robots),
            'faults': len(getattr(sim, 'faults', {}) or {}),
            'min_battery': min(batteries) if batteries else None,
            'avg_battery': sum(batteries) / len(batteries) if batteries else None,
        },
        'tasks': {
            'count': len(tasks),
            'status_counts': dict(task_counts),
            'cursor': getattr(sim, 'cursor', None),
            'released': getattr(sim, 'cursor', None),
        },
        'reservations': {
            'fixed_owners': len(fixed) if hasattr(fixed, '__len__') else 0,
            'cells': len(getattr(reservations, 'fixed_cells', {}) or {}) if reservations is not None else 0,
        },
        'assists': len(getattr(sim, 'assists', {}) or {}),
    }


def build_global_record(sim, events, *, status: str, identity: dict[str, Any] | None = None,
                        commit_seq: int | None = None, durable_seq: int | None = None,
                        mirrored_seq: int | None = None, writer_errors: int = 0,
                        writer_dropped: int = 0) -> dict[str, Any]:
    """Build a small picklable record in the simulation owner; no sim object
    is sent to the writer process."""
    identity = identity or {}
    counts = Counter(str(robot.get('stage', 'idle')) for robot in sim.robots.values())
    tasks = Counter(str(task.get('status', 'unknown')) for task in sim.tasks.values())
    assists = {}
    for owner, assist in getattr(sim, 'assists', {}).items():
        row = {key: value for key, value in assist.items()
               if isinstance(value, (str, int, float, bool, type(None)))}
        row['donor_id'] = int(owner)
        row['status'] = 'active'
        assists[str(owner)] = row
    global_events = []
    handoffs = []
    for event in events:
        if event.get('kind') == 'cargo_handoff':
            donor = event.get('donor_robot_id', event.get('donor_id'))
            receiver = event.get('receiver_robot_id', event.get('receiver_id'))
            if donor is not None and receiver is not None:
                handoffs.append({
                    'event_id': event.get('event_id', event.get('event_seq')),
                    'tick': int(event.get('tick', sim.tick)),
                    'task_id': event.get('task_id'), 'goods_id': event.get('goods_id'),
                    'donor_id': int(donor), 'receiver_id': int(receiver),
                    'reason': event.get('reason'), 'location_id': event.get('location_id'),
                })
        if event.get('robot_id') is not None or event.get('kind') in GLOBAL_EVENT_SKIP:
            continue
        global_events.append({
            'id': f"global:{event.get('event_seq', event.get('event_id'))}",
            'event_id': event.get('event_id'),
            'tick': event.get('tick'),
            'type': event.get('kind', 'runtime_event'),
            'text': str(event.get('kind', 'runtime_event')),
            'time': event.get('sim_time'),
            'severity': 'info',
            'source': 'v5-runtime',
            'seq': event.get('event_seq'),
            'details_json': _json({key: value for key, value in event.items()
                                   if key not in {'event_id', 'event_seq', 'kind', 'robot_id'}}),
            'task_id': event.get('task_id'),
            'cargo_id': event.get('goods_id'),
            'rack_id': event.get('rack_id'),
            'slot_id': event.get('location_id'),
        })
        if len(global_events) >= GLOBAL_EVENT_LIMIT:
            break
    generated_at_ms = int(time.time() * 1000)
    config = identity.get('config') if isinstance(identity.get('config'), dict) else {}
    effective_commit = int(commit_seq if commit_seq is not None else getattr(sim, 'knowledge_seq', sim.tick))
    return {
        'run_id': str(sim.run_id),
        'tick': int(sim.tick),
        'elapsed': int(sim.elapsed),
        'status': str(status),
        'generated_at_ms': generated_at_ms,
        'commit_seq': effective_commit,
        'durable_seq': int(durable_seq) if durable_seq is not None else None,
        'mirrored_seq': int(mirrored_seq) if mirrored_seq is not None else None,
        'writer_errors': int(writer_errors),
        'writer_dropped': int(writer_dropped),
        'robot_count': len(sim.robots),
        'max_active_robots': int(getattr(sim.config, 'max_active_robots', len(sim.robots))),
        'backend': str(sim.run_metadata.get('backend', 'warehouse-v5')),
        'dataset_id': identity.get('graph_dataset_id') or identity.get('dataset_id'),
        'scene_sha256': identity.get('scene_sha256'),
        'tasks_sha256': identity.get('tasks_sha256'),
        'manifest_sha256': identity.get('manifest_sha256'),
        'config_json': _json(config),
        'counts': dict(counts),
        'tasks': dict(tasks),
        'assists': assists,
        'metrics': {key: value for key, value in sim.metrics.items()
                    if isinstance(value, (str, int, float, bool, type(None)))},
        'inside': int(getattr(sim, 'inside', 0)),
        'dispatch': build_dispatch_state(sim, commit_seq=effective_commit, identity=identity),
        'events': global_events,
        # A Step is deliberately not inferred here.  Only the future
        # stateful-actor coordinator knows which StepResult acknowledgements
        # arrived for this tick, so it must add the explicit ``step`` payload
        # before calling the HTTP service.
        'handoffs': handoffs,
        # The main coordinator is the authority for task ownership.  It adds
        # assignment/handoff changes here; robot actors never infer the
        # mutable ``Execution.current_robot_id`` field themselves.
        'ownership_changes': [],
    }


_SCHEMA_READY = False


class GlobalWriter:
    def __init__(self, *, uri: str, user: str, password: str | None, database: str = 'neo4j'):
        if not password:
            raise ValueError('WAREHOUSE_NEO4J_PASSWORD is required for global writer')
        from neo4j import GraphDatabase
        self.uri, self.user, self.password, self.database = uri, user, password, database
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._schema_ready = False
        self._active_assists: dict[str, set[str]] = {}

    def close(self):
        self.driver.close()

    def _ensure_schema(self, session):
        if self._schema_ready:
            return
        from .warehouse_runtime_graph import RUNTIME_SCHEMA
        for query in RUNTIME_SCHEMA:
            session.run(query).consume()
        session.run(
            'CREATE INDEX warehouse_runtime_runstat_run_tick IF NOT EXISTS '
            'FOR (n:RunStat) ON (n.run_id, n.tick)').consume()
        self._schema_ready = True

    def write(self, record: dict[str, Any]) -> None:
        run_id = str(record['run_id'])
        tick = int(record['tick'])
        commit_seq = int(record.get('commit_seq') or tick)
        now = int(record.get('generated_at_ms') or time.time() * 1000)
        status = str(record['status'])
        finished = now if status in {'complete', 'failed'} else None
        run = {
            'id': run_id,
            'backend': record['backend'],
            'robot_count': int(record['robot_count']),
            'max_active_robots': int(record['max_active_robots']),
            'updated_tick': tick,
            'updated_at_ms': now,
            'updated_commit_seq': commit_seq,
            'durable_seq': record.get('durable_seq'),
            'mirrored_seq': record.get('mirrored_seq'),
            'dataset_id': record.get('dataset_id'),
            'scene_sha256': record.get('scene_sha256'),
            'tasks_sha256': record.get('tasks_sha256'),
            'manifest_sha256': record.get('manifest_sha256'),
            'config_json': record.get('config_json'),
            'status': status,
            'global_stats_json': _json({key: record[key] for key in
                                        ('counts', 'tasks', 'assists', 'metrics', 'inside')}),
        }
        stat = {
            'id': f'{run_id}:stat:{commit_seq}',
            'run_id': run_id,
            'commit_seq': commit_seq,
            'tick': tick,
            'elapsed': int(record['elapsed']),
            'status': status,
            'durable_seq': record.get('durable_seq'),
            'mirrored_seq': record.get('mirrored_seq'),
            'generated_at_ms': now,
            'updated_at_ms': now,
            'writer_errors': int(record.get('writer_errors', 0)),
            'writer_dropped': int(record.get('writer_dropped', 0)),
            'counts_json': _json(record['counts']),
            'tasks_json': _json(record['tasks']),
            'assists_json': _json(record['assists']),
            'metrics_json': _json(record['metrics']),
            'inside': int(record['inside']),
        }
        with self.driver.session(database=self.database) as session:
            self._ensure_schema(session)
            if run_id not in self._active_assists:
                # Recover active assists after a writer restart so a later
                # snapshot without the assist can close it instead of leaving
                # a permanently active node.
                rows = session.run(
                    'MATCH (a:Assist {run_id: $run_id}) WHERE a.status = "active" RETURN a.id AS id',
                    run_id=run_id).data()
                self._active_assists[run_id] = {row['id'] for row in rows}
            session.run(
                'MERGE (run:Run {id: $run.id}) SET run += $run, '
                'run.created_at_ms = coalesce(run.created_at_ms, $now), '
                'run.started_at_ms = coalesce(run.started_at_ms, $now), '
                'run.finished_at_ms = CASE WHEN $finished IS NULL THEN run.finished_at_ms ELSE $finished END',
                run=run, now=now, finished=finished).consume()
            if record.get('dataset_id'):
                session.run(
                    'MATCH (run:Run {id: $run_id}), (dataset:Dataset {id: $dataset_id}) '
                    'MERGE (run)-[:USES_DATASET]->(dataset)',
                    run_id=run_id, dataset_id=str(record['dataset_id'])).consume()
            session.run(
                'MATCH (run:Run {id: $run_id}) '
                'MERGE (stat:RunStat {id: $stat.id}) SET stat += $stat '
                'MERGE (stat)-[:IN_RUN]->(run)',
                run_id=run_id, stat=stat).consume()
            self._write_dispatch(session, run_id, record.get('dispatch') or {}, status)
            self._write_step(session, run_id, record.get('step') or {}, now)
            self._write_assists(session, run_id, tick, now, record.get('assists') or {}, status)
            self._write_handoffs(session, run_id, record.get('handoffs') or [], now)
            self._write_ownership_changes(session, run_id, record.get('ownership_changes') or [], now)
            events = record.get('events') or []
            if events:
                rows = [{**event, 'id': f'{run_id}:global-event:{event["id"]}', 'run_id': run_id}
                        for event in events]
                session.run(
                    'MATCH (run:Run {id: $run_id}) UNWIND $events AS row '
                    'MERGE (event:RuntimeEvent {id: row.id}) SET event += row '
                    'MERGE (event)-[:IN_RUN]->(run)',
                    run_id=run_id, events=rows).consume()

    def _write_step(self, session, run_id: str, step: dict[str, Any], now: int):
        """Write a monotonic complete-frame marker for bounded graph reads."""
        if not step:
            return
        tick = int(step.get('tick', 0)); expected = int(step.get('expected_robot_count', 0))
        received = sorted({int(value) for value in step.get('received_robot_ids', [])})
        complete = bool(step.get('complete')) and expected > 0 and len(received) >= expected
        if not complete:
            return
        node = {'id': f'{run_id}:step:{tick}', 'run_id': run_id, 'tick': tick,
                'commit_seq': int(step.get('commit_seq', tick)), 'expected_robot_count': expected,
                'received_robot_ids_json': _json(received), 'received_robot_count': len(received),
                'complete': True, 'source': str(step.get('source', 'main-coordinator')),
                'updated_at_ms': now}
        session.run(
            'MATCH (run:Run {id:$run_id}) '
            'MERGE (step:Step {id:$step.id}) SET step += $step '
            'MERGE (step)-[:IN_RUN]->(run) '
            'SET run.latest_step = CASE WHEN coalesce(run.latest_step,-1) < $step.tick THEN $step.tick ELSE run.latest_step END',
            run_id=run_id, step=node).consume()

    def _write_handoffs(self, session, run_id: str, handoffs: list[dict[str, Any]], now: int):
        """Persist the immutable cross-robot cargo transfer fact."""
        for row in handoffs:
            event_id = row.get('event_id')
            if event_id is None:
                continue
            node = {'id': f'{run_id}:handoff:{event_id}', 'run_id': run_id,
                    'event_id': str(event_id), 'tick': int(row.get('tick', 0)),
                    'task_id': row.get('task_id'), 'goods_id': row.get('goods_id'),
                    'donor_id': int(row['donor_id']), 'receiver_id': int(row['receiver_id']),
                    'reason': row.get('reason'), 'location_id': row.get('location_id'),
                    'recorded_at_ms': now}
            session.run(
                'MATCH (run:Run {id:$run_id}) '
                'MERGE (donor:Robot {id:$donor_id}) '
                'ON CREATE SET donor.run_id=$run_id, donor.robot_id=$donor_number '
                'MERGE (receiver:Robot {id:$receiver_id}) '
                'ON CREATE SET receiver.run_id=$run_id, receiver.robot_id=$receiver_number '
                'MERGE (handoff:Handoff {id:$handoff.id}) SET handoff += $handoff '
                'MERGE (handoff)-[:IN_RUN]->(run) '
                'MERGE (handoff)-[:DONOR]->(donor) '
                'MERGE (handoff)-[:RECEIVER]->(receiver)',
                run_id=run_id, donor_id=f"{run_id}:robot:{node['donor_id']}",
                receiver_id=f"{run_id}:robot:{node['receiver_id']}", donor_number=node['donor_id'],
                receiver_number=node['receiver_id'], handoff=node).consume()
            if node['task_id']:
                session.run(
                    'MATCH (handoff:Handoff {id:$id}), (execution:Execution {id:$execution_id}) '
                    'MERGE (handoff)-[:ABOUT_EXECUTION]->(execution)',
                    id=node['id'], execution_id=f"{run_id}:execution:{node['task_id']}").consume()

    def _write_ownership_changes(self, session, run_id: str,
                                 changes: list[dict[str, Any]], now: int):
        """Apply coordinator-authoritative task ownership in tick order.

        Actor graph writes may add historical ``ASSIGNED_TO`` participants,
        but must not update ``current_robot_id``.  This method is called by
        the single global writer worker, so task handoff ordering is stable.
        """
        if not changes:
            return
        rows = []
        for index, raw in enumerate(changes):
            task_id = str(raw['task_id'])
            tick = int(raw['tick'])
            to_robot_id = int(raw['to_robot_id'])
            from_robot_id = raw.get('from_robot_id')
            change_id = str(raw.get('change_id') or f'{task_id}:{tick}:{to_robot_id}:{index}')
            rows.append({
                'id': f'{run_id}:ownership:{change_id}', 'run_id': run_id,
                'change_id': change_id, 'task_id': task_id, 'tick': tick,
                'from_robot_id': int(from_robot_id) if from_robot_id is not None else None,
                'to_robot_id': to_robot_id, 'reason': str(raw.get('reason', 'assigned')),
                'recorded_at_ms': now,
                'execution_id': f'{run_id}:execution:{task_id}',
                'to_robot_node_id': f'{run_id}:robot:{to_robot_id}',
                'from_robot_node_id': f'{run_id}:robot:{int(from_robot_id)}' if from_robot_id is not None else None,
            })
        session.run(
            'MATCH (run:Run {id:$run_id}) UNWIND $changes AS row '
            'MERGE (execution:Execution {id:row.execution_id}) '
            'ON CREATE SET execution.run_id=row.run_id,execution.task_id=row.task_id '
            'MERGE (execution)-[:IN_RUN]->(run) '
            'MERGE (target:Robot {id:row.to_robot_node_id}) '
            'ON CREATE SET target.run_id=row.run_id,target.robot_id=row.to_robot_id '
            'MERGE (target)-[:IN_RUN]->(run) '
            'MERGE (change:OwnershipChange {id:row.id}) SET change += row '
            'MERGE (change)-[:IN_RUN]->(run) '
            'MERGE (change)-[:ABOUT_EXECUTION]->(execution) '
            'MERGE (change)-[:TO_ROBOT]->(target) '
            'WITH execution,target,row '
            'OPTIONAL MATCH (execution)-[previous:ASSIGNED_TO]->(prior:Robot) '
            'WHERE prior.id <> target.id AND previous.until_tick IS NULL '
            'SET previous.until_tick=row.tick '
            'WITH DISTINCT execution,target,row '
            'MERGE (execution)-[assignment:ASSIGNED_TO]->(target) '
            'SET assignment.since_tick=coalesce(assignment.since_tick,row.tick),assignment.until_tick=null, '
            'execution.current_robot_id=CASE WHEN coalesce(execution.current_owner_tick,-1) <= row.tick THEN row.to_robot_id ELSE execution.current_robot_id END, '
            'execution.current_robot_node_id=CASE WHEN coalesce(execution.current_owner_tick,-1) <= row.tick THEN row.to_robot_node_id ELSE execution.current_robot_node_id END, '
            'execution.current_owner_tick=CASE WHEN coalesce(execution.current_owner_tick,-1) <= row.tick THEN row.tick ELSE execution.current_owner_tick END',
            run_id=run_id, changes=rows).consume()
        session.run(
            'UNWIND $changes AS row WITH row WHERE row.from_robot_id IS NOT NULL '
            'MERGE (source:Robot {id:row.from_robot_node_id}) '
            'ON CREATE SET source.run_id=row.run_id,source.robot_id=row.from_robot_id '
            'MATCH (run:Run {id:row.run_id}) MERGE (source)-[:IN_RUN]->(run) '
            'MERGE (change:OwnershipChange {id:row.id}) '
            'MERGE (change)-[:FROM_ROBOT]->(source)',
            changes=rows).consume()

    def _write_dispatch(self, session, run_id: str, dispatch: dict[str, Any], status: str):
        if not dispatch:
            return
        seq = int(dispatch.get('commit_seq') or dispatch.get('tick') or 0)
        node = {
            'id': f'{run_id}:dispatch:{seq}',
            'run_id': run_id,
            'commit_seq': seq,
            'tick': int(dispatch.get('tick') or 0),
            'elapsed': int(dispatch.get('elapsed') or 0),
            'status': status,
            'schema_version': int(dispatch.get('schema_version') or 1),
            'source': str(dispatch.get('source') or 'local-v5'),
            'backend': dispatch.get('backend'),
            'dataset_id': dispatch.get('dataset_id'),
            'generated_at_ms': int(dispatch.get('generated_at_ms') or time.time() * 1000),
            'active_jobs_json': _json(dispatch.get('active_jobs') or []),
            'pending_json': _json(dispatch.get('pending') or []),
            'ports_json': _json(dispatch.get('ports') or {}),
            'robots_json': _json(dispatch.get('robots') or {}),
            'tasks_json': _json(dispatch.get('tasks') or {}),
            'reservations_json': _json(dispatch.get('reservations') or {}),
        }
        session.run(
            'MATCH (run:Run {id: $run_id}) '
            'MERGE (state:DispatchState {id: $state.id}) SET state += $state '
            'MERGE (state)-[:IN_RUN]->(run) '
            'SET run.latest_dispatch_seq=$state.commit_seq,run.latest_dispatch_tick=$state.tick',
            run_id=run_id, state=node).consume()

    def _write_assists(self, session, run_id: str, tick: int, now: int, assists: dict[str, Any], status: str):
        current = set()
        rows = []
        for donor, data in assists.items():
            task_id = data.get('task_id')
            handoff_at = data.get('handoff_at')
            assist_id = f"{run_id}:assist:{donor}:{task_id or 'none'}:{handoff_at if handoff_at is not None else 0}"
            current.add(assist_id)
            rows.append({
                'id': assist_id,
                'run_id': run_id,
                'donor_id': int(donor),
                'receiver_id': data.get('receiver'),
                'task_id': task_id,
                'reason': data.get('reason'),
                'handoff_at': handoff_at,
                'status': 'active',
                'start_tick': tick,
                'last_seen_tick': tick,
                'updated_at_ms': now,
            })
        if rows:
            session.run(
                'MATCH (run:Run {id: $run_id}) UNWIND $assists AS row '
                'MERGE (a:Assist {id: row.id}) SET a += row, '
                'a.start_tick = coalesce(a.start_tick, row.start_tick) '
                'MERGE (a)-[:IN_RUN]->(run)',
                run_id=run_id, assists=rows).consume()
            for row in rows:
                if row['task_id']:
                    session.run(
                        'MATCH (a:Assist {id: $id}), (task:WarehouseTask {task_id: $task_id}) '
                        'MERGE (a)-[:ABOUT_TASK]->(task)',
                        id=row['id'], task_id=row['task_id']).consume()
                for key, rel in (('donor_id', 'DONOR'), ('receiver_id', 'RECEIVER')):
                    if row.get(key):
                        session.run(
                            'MATCH (a:Assist {id: $id}), (robot:Robot {id: $robot_id}) '
                            f'MERGE (a)-[:{rel}]->(robot)',
                            id=row['id'], robot_id=f"{run_id}:robot:{row[key]}").consume()
        previous = self._active_assists.get(run_id, set())
        ended = previous - current
        if status in {'complete', 'failed'}:
            ended |= current
            current = set()
        if ended:
            session.run(
                'MATCH (a:Assist) WHERE a.id IN $ids '
                'SET a.status = $status, a.end_tick = $tick, a.updated_at_ms = $now',
                ids=sorted(ended), status='ended' if status not in {'complete', 'failed'} else status,
                tick=tick, now=now).consume()
        self._active_assists[run_id] = current


def _writer_loop(records, config, errors, last_tick):
    writer = None
    try:
        writer = GlobalWriter(**config)
        while True:
            item = records.get()
            if item is None:
                break
            for attempt in range(10):
                try:
                    writer.write(item)
                    last_tick.value = int(item['tick'])
                    break
                except Exception as exc:
                    transient = 'TransientError' in type(exc).__name__ or 'DeadlockDetected' in str(exc)
                    if transient and attempt < 9:
                        time.sleep(0.05 * (attempt + 1))
                        continue
                    errors.value += 1
                    print(f'global writer failed: {type(exc).__name__}: {exc}', flush=True)
                    break
    finally:
        if writer is not None:
            writer.close()


class GlobalWriterProcess:
    """Parent-side handle: bounded queue + one plain child process."""

    def __init__(self, config: dict[str, Any], *, max_queue: int = 256):
        if type(max_queue) is not int or not 16 <= max_queue <= 10000:
            raise ValueError('global writer queue must be within 16..10000')
        ctx = mp.get_context('spawn')
        self.records = ctx.Queue(maxsize=max_queue)
        self.errors = ctx.Value('i', 0)
        self.last_tick = ctx.Value('q', -1)
        self.dropped = 0
        self._closed = False
        self.process = ctx.Process(target=_writer_loop,
                                   args=(self.records, dict(config), self.errors, self.last_tick),
                                   name='warehouse-global-writer', daemon=True)
        self.process.start()

    def record(self, record: dict[str, Any]) -> bool:
        if self._closed:
            return False
        try:
            self.records.put_nowait(record)
            return True
        except queue.Full:
            self.dropped += 1
            return False

    def health(self) -> dict[str, Any]:
        return {
            'enabled': True,
            'processAlive': bool(self.process.is_alive()),
            'pid': self.process.pid,
            'errors': int(self.errors.value),
            'dropped': int(self.dropped),
            'lastTick': int(self.last_tick.value),
            'queueSize': int(self.records.qsize()) if not self._closed else 0,
            'processCount': 1,
        }

    def close(self, timeout: float = 10.0):
        if self._closed:
            return
        self._closed = True
        try:
            self.records.put(None, timeout=2.0)
        except queue.Full:
            pass
        self.process.join(timeout)
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(2.0)
        try:
            self.records.close()
        except Exception:
            pass
