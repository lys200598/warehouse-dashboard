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

from .global_writer import GlobalWriter


def _body(raw: bytes) -> dict[str, Any]:
    if len(raw) > 1_048_576:
        raise ValueError('request exceeds 1 MiB')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('JSON object required')
    return value


def _ident(value: object, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 160:
        raise ValueError(f'{name} must be a non-empty string up to 160 characters')
    return value


class RecordQueue:
    """14284-owned bounded queue.  HTTP handlers never wait for Neo4j I/O."""
    def __init__(self, writer: GlobalWriter, size: int):
        self.writer, self.records = writer, queue.Queue(maxsize=size)
        self.errors = 0; self.last_tick = -1; self.closed = False
        self.thread = threading.Thread(target=self._run, name='global-writer-neo4j', daemon=True)
        self.thread.start()

    def submit(self, record: dict[str, Any]) -> bool:
        if self.closed:
            return False
        try:
            self.records.put_nowait(record)
            return True
        except queue.Full:
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
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse

    records = RecordQueue(writer, queue_size)

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            records.close(); writer.close()

    app = FastAPI(title='Warehouse global record writer', version='v1', lifespan=lifespan)

    @app.get('/v1/health')
    async def health():
        return {'status': 'ok', 'writer': 'global-record', 'database': writer.database,
                'queue': records.health()}

    @app.post('/v1/records', status_code=202)
    async def write_record(request: Request):
        try:
            record = _body(await request.body())
            for name in ('run_id', 'tick', 'elapsed', 'status', 'backend', 'robot_count', 'max_active_robots'):
                if name not in record:
                    raise ValueError(f'{name} is required')
            if not records.submit(record):
                return JSONResponse({'detail': 'global writer queue is full or closed'}, status_code=503)
            return {'accepted': True, 'run_id': record['run_id'], 'tick': record['tick'],
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
                writer.write(record)
            with writer.driver.session(database=writer.database) as session:
                session.run('MERGE (i:GlobalRecordImport {id:$id}) '
                            'SET i.dataset_id=$dataset_id,i.import_id=$import_id,i.record_count=$count,i.imported_at_ms=timestamp()',
                            id=f'{dataset_id}:{import_id}', dataset_id=dataset_id,
                            import_id=import_id, count=len(records)).consume()
            return {'accepted': True, 'dataset_id': dataset_id, 'import_id': import_id, 'record_count': len(records)}
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({'detail': str(exc)}, status_code=400)

    @app.get('/v1/runs/{run_id}/steps/{tick}')
    async def get_step(run_id: str, tick: int):
        with writer.driver.session(database=writer.database) as session:
            rows = session.run('MATCH (s:Step {id:$id}) RETURN s {.*} AS step', id=f'{run_id}:step:{tick}').data()
        return rows[0]['step'] if rows else JSONResponse({'detail': 'Step not found'}, status_code=404)

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
