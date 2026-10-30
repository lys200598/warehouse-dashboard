"""HTTP/JSON API for the independent warehouse global-record writer.

The API owns Neo4j writes.  The simulation's main process talks to it through
the bounded ``GlobalWriterProcess`` HTTP transport, so an unavailable graph
never blocks the simulation tick.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Request

from .global_writer import GlobalWriter


def _body(raw: bytes) -> dict[str, Any]:
    if len(raw) > 8 * 1_048_576:
        raise ValueError('request exceeds 8 MiB')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('JSON object required')
    return value


def _ident(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 160:
        raise ValueError(f'{name} must be a non-empty string up to 160 characters')
    return value


def _validate_record(record: dict[str, Any]) -> None:
    required = ('run_id', 'tick', 'elapsed', 'status', 'backend', 'robot_count',
                'max_active_robots', 'counts', 'tasks', 'assists', 'metrics', 'inside',
                'dispatch', 'events', 'handoffs')
    missing = [name for name in required if name not in record]
    if missing:
        raise ValueError('missing required fields: ' + ', '.join(missing))
    _ident(record['run_id'], 'run_id')
    if not isinstance(record['tick'], int) or record['tick'] < 0:
        raise ValueError('tick must be a non-negative integer')
    for name in ('counts', 'tasks', 'assists', 'metrics', 'dispatch'):
        if not isinstance(record[name], dict):
            raise ValueError(f'{name} must be an object')
    for name in ('events', 'handoffs'):
        if not isinstance(record[name], list):
            raise ValueError(f'{name} must be an array')
    ownership_changes = record.get('ownership_changes', [])
    if not isinstance(ownership_changes, list):
        raise ValueError('ownership_changes must be an array')
    for index, change in enumerate(ownership_changes):
        if not isinstance(change, dict):
            raise ValueError(f'ownership_changes[{index}] must be an object')
        for name in ('task_id', 'tick', 'to_robot_id', 'reason'):
            if name not in change:
                raise ValueError(f'ownership_changes[{index}].{name} is required')
        _ident(change['task_id'], f'ownership_changes[{index}].task_id')
        if not isinstance(change['tick'], int) or change['tick'] < 0:
            raise ValueError(f'ownership_changes[{index}].tick must be a non-negative integer')
        for name in ('to_robot_id', 'from_robot_id'):
            if name in change and change[name] is not None and (
                not isinstance(change[name], int) or change[name] < 1
            ):
                raise ValueError(f'ownership_changes[{index}].{name} must be a positive integer')
        if not isinstance(change['to_robot_id'], int) or change['to_robot_id'] < 1:
            raise ValueError(f'ownership_changes[{index}].to_robot_id must be a positive integer')
        if not isinstance(change['reason'], str) or not change['reason']:
            raise ValueError(f'ownership_changes[{index}].reason must be a non-empty string')
    step = record.get('step')
    if step is not None:
        if not isinstance(step, dict):
            raise ValueError('step must be an object')
        for name in ('tick', 'expected_robot_count', 'received_robot_ids', 'complete'):
            if name not in step:
                raise ValueError(f'step.{name} is required')
        if not isinstance(step['expected_robot_count'], int) or step['expected_robot_count'] < 1:
            raise ValueError('step.expected_robot_count must be a positive integer')
        if not isinstance(step['received_robot_ids'], list) or any(not isinstance(value, int) or value < 1 for value in step['received_robot_ids']):
            raise ValueError('step.received_robot_ids must contain positive integers')
        if not isinstance(step['complete'], bool):
            raise ValueError('step.complete must be boolean')
        if step['complete'] and len(set(step['received_robot_ids'])) < step['expected_robot_count']:
            raise ValueError('complete Step has fewer receipt ids than expected_robot_count')


def _validate_batch(records: Any) -> list[dict[str, Any]]:
    if not isinstance(records, list) or not records or len(records) > 128:
        raise ValueError('records must contain 1..128 global records')
    if any(not isinstance(record, dict) for record in records):
        raise ValueError('records must contain JSON objects')
    for record in records:
        _validate_record(record)
    run_ids = {str(record['run_id']) for record in records}
    if len(run_ids) != 1:
        raise ValueError('a batch must contain records for exactly one run_id')
    ticks = [int(record['tick']) for record in records]
    if any(later <= earlier for earlier, later in zip(ticks, ticks[1:])):
        raise ValueError('batch ticks must be strictly increasing')
    ownership_keys = [
        (str(record['run_id']), str(change['task_id']), int(change['tick']))
        for record in records for change in record.get('ownership_changes', [])
    ]
    if len(set(ownership_keys)) != len(ownership_keys):
        raise ValueError('a batch cannot contain multiple ownership changes for one task at one tick')
    return records


class RecordQueue:
    """14284-owned bounded queue.  HTTP handlers never wait for Neo4j I/O."""
    def __init__(self, writer: GlobalWriter, size: int):
        self.writer, self.records = writer, queue.Queue(maxsize=size)
        self.errors = 0; self.last_tick = -1; self.closed = False
        self._submit_lock = threading.Lock()
        self.thread = threading.Thread(target=self._run, name='global-writer-neo4j', daemon=True)
        self.thread.start()

    def submit(self, record: dict[str, Any]) -> bool:
        return self.submit_many([record])

    def submit_many(self, items: list[dict[str, Any]]) -> bool:
        if self.closed:
            return False
        # Keep a batch adjacent in the queue.  This preserves the caller's
        # tick ordering while still leaving Neo4j I/O to the worker thread.
        with self._submit_lock:
            if self.closed or self.records.maxsize - self.records.qsize() < len(items):
                return False
            try:
                for item in items:
                    self.records.put_nowait(item)
                return True
            except queue.Full:  # defensive; capacity was checked under lock
                return False

    def _run(self):
        while True:
            item = self.records.get()
            if item is None:
                return
            for attempt in range(10):
                try:
                    self.writer.write(item); self.last_tick = int(item['tick']); break
                except Exception:
                    if attempt == 9:
                        self.errors += 1
                    else:
                        time.sleep(.05 * (attempt + 1))

    def health(self):
        return {'queue_size': self.records.qsize(), 'queue_capacity': self.records.maxsize,
                'errors': self.errors, 'last_tick': self.last_tick, 'worker_alive': self.thread.is_alive()}

    def close(self):
        self.closed = True
        try:
            self.records.put(None, timeout=2.)
        except queue.Full:
            pass
        self.thread.join(10.)


def create_app(writer: GlobalWriter, *, queue_size: int = 256):
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    records = RecordQueue(writer, queue_size)

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            records.close(); writer.close()

    app = FastAPI(title='Warehouse global record writer', version='v1', lifespan=lifespan)

    def _complete_step_states(run_id: str, tick: int, limit: int) -> list[dict[str, Any]]:
        limit = max(1, min(limit, 500))
        with writer.driver.session(database=writer.database) as session:
            rows = session.run('MATCH (step:Step {run_id:$run_id}) WHERE step.tick=$tick AND step.complete=true '
                               'MATCH (run:Run {id:$run_id})<-[:IN_RUN]-(robot:Robot) '
                               'CALL { WITH robot, $tick AS cutoff '
                               'MATCH (state:AgentState)-[:OF_ROBOT]->(robot) WHERE state.tick <= cutoff '
                               'RETURN state ORDER BY state.tick DESC LIMIT 1 } '
                               'RETURN robot.robot_id AS robot_id, '
                               'state { .tick,.stage,.battery,.x,.y,.task_id,.cargo_id } AS state '
                               'ORDER BY robot_id LIMIT $limit', run_id=run_id, tick=tick, limit=limit).data()
        return [{**row['state'], 'robot_id': row['robot_id'], 'sampled_tick': row['state']['tick']} for row in rows]

    @app.get('/v1/health')
    async def health():
        return {'status': 'ok', 'writer': 'global-record', 'database': writer.database,
                'queue': records.health()}

    @app.post('/v1/records', status_code=202)
    async def write_record(request: Request):
        try:
            body = _body(await request.body())
            batch = _validate_batch(body['records']) if 'records' in body else [_validate_record(body) or body]
            if not records.submit_many(batch):
                return JSONResponse({'detail': 'global writer queue is full or closed'}, status_code=503)
            return {'accepted': True, 'run_id': batch[0]['run_id'],
                    'ticks': [record['tick'] for record in batch], 'record_count': len(batch),
                    'queue': records.health()}
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({'detail': str(exc)}, status_code=400)

    # A dataset is a safe logical namespace, not a Neo4j physical database.
    # This keeps the endpoint usable in Community Edition and prevents a client
    # from deleting another user's Neo4j database.
    @app.get('/v1/datasets')
    async def list_datasets():
        with writer.driver.session(database=writer.database) as session:
            rows = session.run('MATCH (d:GlobalRecordDataset) RETURN d {.*} AS dataset ORDER BY d.id').data()
        return {'datasets': [row['dataset'] for row in rows]}

    @app.post('/v1/datasets', status_code=201)
    async def create_dataset(request: Request):
        try:
            body = _body(await request.body()); dataset_id = _ident(body.get('dataset_id'), 'dataset_id')
            label = str(body.get('label', dataset_id))[:200]
            with writer.driver.session(database=writer.database) as session:
                writer._ensure_schema(session)
                session.run('MERGE (d:GlobalRecordDataset {id:$id}) '
                            'ON CREATE SET d.created_at_ms=timestamp() '
                            'SET d.label=$label', id=dataset_id, label=label).consume()
            return {'created': True, 'dataset_id': dataset_id}
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({'detail': str(exc)}, status_code=400)

    @app.delete('/v1/datasets/{dataset_id}')
    async def delete_dataset(dataset_id: str, request: Request):
        try:
            body = _body(await request.body())
            if body.get('confirm_dataset_id') != dataset_id:
                raise ValueError('confirm_dataset_id must equal the path dataset_id')
            with writer.driver.session(database=writer.database) as session:
                session.run('MATCH (d:GlobalRecordDataset {id:$id}) DETACH DELETE d', id=dataset_id).consume()
            return {'deleted': True, 'dataset_id': dataset_id, 'scope': 'logical namespace metadata only'}
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({'detail': str(exc)}, status_code=400)

    @app.post('/v1/datasets/{dataset_id}/imports', status_code=202)
    async def one_time_import(dataset_id: str, request: Request):
        try:
            body = _body(await request.body()); import_id = _ident(body.get('import_id'), 'import_id')
            records = body.get('records')
            if not isinstance(records, list) or not records or len(records) > 1000:
                raise ValueError('records must contain 1..1000 global records')
            with writer.driver.session(database=writer.database) as session:
                exists = session.run('MATCH (i:GlobalRecordImport {id:$id}) RETURN i.id AS id',
                                     id=f'{dataset_id}:{import_id}').data()
                if exists:
                    raise ValueError('import_id already applied for this dataset')
            for record in records:
                if not isinstance(record, dict):
                    raise ValueError('records must be JSON objects')
                record = dict(record); record['dataset_id'] = dataset_id
                _validate_record(record)
                writer.write(record)
            with writer.driver.session(database=writer.database) as session:
                session.run('MERGE (i:GlobalRecordImport {id:$id}) '
                            'SET i.dataset_id=$dataset_id,i.import_id=$import_id,i.record_count=$count,i.imported_at_ms=timestamp()',
                            id=f'{dataset_id}:{import_id}', dataset_id=dataset_id,
                            import_id=import_id, count=len(records)).consume()
            return {'accepted': True, 'dataset_id': dataset_id, 'import_id': import_id, 'record_count': len(records)}
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({'detail': str(exc)}, status_code=400)

    @app.get('/v1/runs/{run_id}/steps/latest')
    async def get_latest_step(run_id: str, states: bool = False, limit: int = 200):
        with writer.driver.session(database=writer.database) as session:
            rows = session.run('MATCH (run:Run {id:$run_id}) '
                               'MATCH (step:Step {run_id:$run_id}) '
                               'WHERE step.tick=run.latest_step AND step.complete=true '
                               'RETURN step {.*} AS step', run_id=run_id).data()
        if not rows:
            return JSONResponse({'detail': 'complete Step not found'}, status_code=404)
        step = rows[0]['step']
        return {'step': step, 'states': _complete_step_states(run_id, int(step['tick']), limit)} if states else step

    @app.get('/v1/runs/{run_id}/steps/{tick}')
    async def get_step(run_id: str, tick: int):
        with writer.driver.session(database=writer.database) as session:
            rows = session.run('MATCH (s:Step {id:$id}) RETURN s {.*} AS step', id=f'{run_id}:step:{tick}').data()
        return rows[0]['step'] if rows else JSONResponse({'detail': 'Step not found'}, status_code=404)

    @app.get('/v1/runs/{run_id}/steps/{tick}/states')
    async def get_step_states(run_id: str, tick: int, limit: int = 200):
        return {'run_id': run_id, 'tick': tick,
                'states': _complete_step_states(run_id, tick, limit)}

    @app.get('/v1/runs/{run_id}/handoffs')
    async def list_handoffs(run_id: str, task_id: str | None = None, goods_id: str | None = None,
                            robot_id: int | None = None, limit: int = 200):
        if robot_id is not None and robot_id < 1:
            return JSONResponse({'detail': 'robot_id must be positive'}, status_code=400)
        limit = max(1, min(limit, 500))
        with writer.driver.session(database=writer.database) as session:
            rows = session.run('MATCH (h:Handoff {run_id:$run_id}) '
                               'WHERE ($task_id IS NULL OR h.task_id=$task_id) '
                               'AND ($goods_id IS NULL OR h.goods_id=$goods_id) '
                               'AND ($robot_id IS NULL OR h.donor_id=$robot_id OR h.receiver_id=$robot_id) '
                               'RETURN h {.*} AS handoff ORDER BY h.tick DESC,h.id DESC LIMIT $limit',
                               run_id=run_id, task_id=task_id, goods_id=goods_id,
                               robot_id=robot_id, limit=limit).data()
        return {'run_id': run_id, 'handoffs': [row['handoff'] for row in rows]}

    @app.get('/v1/runs/{run_id}/handoffs/{event_id}')
    async def get_handoff(run_id: str, event_id: str):
        with writer.driver.session(database=writer.database) as session:
            rows = session.run('MATCH (h:Handoff {id:$id}) RETURN h {.*} AS handoff',
                               id=f'{run_id}:handoff:{event_id}').data()
        return rows[0]['handoff'] if rows else JSONResponse({'detail': 'Handoff not found'}, status_code=404)

    @app.delete('/v1/runs/{run_id}')
    async def delete_run(run_id: str, request: Request):
        try:
            body = _body(await request.body())
            if body.get('confirm_run_id') != run_id:
                raise ValueError('confirm_run_id must equal the path run_id')
            with writer.driver.session(database=writer.database) as session:
                session.run('MATCH (n {run_id:$run_id}) DETACH DELETE n', run_id=run_id).consume()
                session.run('MATCH (run:Run {id:$run_id}) DETACH DELETE run', run_id=run_id).consume()
            return {'deleted': True, 'run_id': run_id}
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({'detail': str(exc)}, status_code=400)

    return app


def main(argv=None):
    parser = argparse.ArgumentParser(description='Warehouse global record HTTP writer')
    parser.add_argument('--host', choices=('127.0.0.1', 'localhost', '::1'), default='127.0.0.1')
    parser.add_argument('--port', type=int, default=14284)
    parser.add_argument('--queue-size', type=int, default=256)
    args = parser.parse_args(argv)
    writer = GlobalWriter(uri=os.environ.get('WAREHOUSE_NEO4J_URI', 'bolt://127.0.0.1:47687'),
                          user=os.environ.get('WAREHOUSE_NEO4J_USER', 'neo4j'),
                          password=os.environ.get('WAREHOUSE_NEO4J_PASSWORD'),
                          database=os.environ.get('WAREHOUSE_NEO4J_DATABASE', 'neo4j'))
    import uvicorn
    if not 16 <= args.queue_size <= 10000:
        parser.error('--queue-size must be within 16..10000')
    uvicorn.run(create_app(writer, queue_size=args.queue_size), host=args.host, port=args.port, log_level='info')


if __name__ == '__main__':
    main()
