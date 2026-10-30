"""Small immutable SHA-fenced points for native query-only routing."""
from collections import deque
from contextlib import redirect_stderr
import io
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from backend.checkpoint import read_checkpoint
from backend.real_scale.persistence import write_checkpoint
from ops.warehouse_live_runtime import WarehouseLiveRuntime, QUERY_POINT_VERSION
from ops import warehouse_query_sidecar as sidecar

try:
    from fastapi.testclient import TestClient
except ImportError:
    TestClient = None


ROOT = Path(__file__).resolve().parents[1]
TEMP = ROOT / 'data/local-check/live-published-point-tests'
TEMP.mkdir(parents=True, exist_ok=True)


def payload(**changes):
    return {'version': QUERY_POINT_VERSION, 'run_id': 'live-point-run', 'seq': 3,
            'runtime_instance_id': 'owner-123', 'source_sha256': 'a' * 64, **changes}


class PublishedPointTests(unittest.TestCase):
    def owner(self, folder, *, enabled=True):
        owner = object.__new__(WarehouseLiveRuntime)
        root = Path(folder)
        owner._output = root / 'run'
        owner._output.mkdir()
        owner.query_point_path = root / 'query-point.json' if enabled else None
        owner._last_query_point = None
        owner._sim = SimpleNamespace(run_id='live-point-run', knowledge_seq=3, elapsed=12,
            pending_events=[], config=SimpleNamespace(speed=.8), run_metadata={},
            frame=lambda: {'runId': owner._sim.run_id},
            checkpoint=lambda: {'run_id': owner._sim.run_id})
        owner._status = {'durable_seq': 3, 'mirrored_seq': 3, 'last_error': None}
        owner._store = SimpleNamespace(status=lambda run: dict(owner._status), mirror=object(),
            last_error=None, append=lambda cp, events: None)
        owner._writes = []
        def write(path, value, **options):  # durable=False for latest.json
            owner._writes.append((Path(path), value))
            write_checkpoint(path, value)
        owner._core = SimpleNamespace(persistence=SimpleNamespace(write_checkpoint=write))
        owner._query_summary = object()
        owner._query_summary_status = {'published_seq': 3}
        owner._rate_points = deque([(1., 0), (2., 12)])
        owner._last_commit = 12
        owner._state = 'paused'
        owner._speed = 1.
        owner._error = None
        owner._events = deque()
        owner._dataset_id = 'sample'
        owner._identity = {key: 'b' * 64 for key in ('tasks_sha256', 'scene_sha256',
            'initial_inventory_sha256', 'manifest_sha256', 'adapter_sha256')}
        owner._source_sha = 'a' * 64
        owner.runtime_instance_id = 'owner-123'
        owner.commit_seconds = 5
        owner.knowledge_mode = 'rules'
        owner.knowledge_backend = 'neo4j'
        owner.resource_graph_enabled = True
        owner._lock = threading.RLock()
        owner._changed = threading.Condition(owner._lock)
        owner._revision = 0
        owner._latest = None
        owner._scene = {'fixture': True}
        owner._disturbances = []
        owner._applied = []
        return owner

    def test_atomic_sha_envelope_contains_only_light_point(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            owner = self.owner(folder)
            owner._publish()
            point = owner.query_point_path
            self.assertEqual(read_checkpoint(point), payload())
            envelope = json.loads(point.read_bytes())
            self.assertEqual(set(envelope), {'format', 'payload', 'sha256'})
            self.assertEqual(envelope['format'], 'warehouse-checkpoint-v2')
            self.assertLess(point.stat().st_size, 1024)
            self.assertEqual(list(Path(folder).glob('.query-point.json.*.tmp')), [])
            self.assertEqual(sidecar._read_live_query_point(point, 'owner-123'), payload())

    def test_publish_mode_defaults_and_explicit_values(self):
        for mode in (None, 'memory', 'neo4j'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(dir=TEMP) as folder:
                owner = self.owner(folder)
                if mode is not None:
                    owner.kg_state_source = mode
                    owner.legacy_a_graph = True
                owner._publish()
                frame = owner._latest
                self.assertEqual(frame['kgStateSource'], mode or 'memory')
                self.assertEqual(frame['metadata']['kg_state_source'], mode or 'memory')
                self.assertEqual(frame['metadata']['legacy_a_graph'], mode is not None)

    def test_writes_only_when_committed_run_or_seq_changes(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            owner = self.owner(folder)
            for tick in range(10):
                owner._sim.elapsed += 1
                owner._publish()
            self.assertEqual(sum(p == owner.query_point_path for p, value in owner._writes), 1)
            owner._sim.knowledge_seq = 4
            owner._status.update(durable_seq=4, mirrored_seq=4)
            owner._query_summary_status = {'published_seq': 4}
            owner._publish()
            self.assertEqual(read_checkpoint(owner.query_point_path)['seq'], 4)
            owner._sim.run_id = 'next-live-run'
            owner._publish()
            self.assertEqual(read_checkpoint(owner.query_point_path)['run_id'], 'next-live-run')
            self.assertEqual(sum(p == owner.query_point_path for p, value in owner._writes), 3)

    def test_disabled_path_keeps_original_latest_writes_only(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            owner = self.owner(folder, enabled=False)
            owner._query_summary = None
            owner._publish()
            self.assertEqual([p.name for p, value in owner._writes], ['latest.json'])
            self.assertFalse((Path(folder) / 'query-point.json').exists())

    def test_incomplete_or_failed_boundary_cannot_advance_old_point(self):
        for broken in ('source', 'graph', 'summary', 'mirror-error'):
            with self.subTest(broken=broken), tempfile.TemporaryDirectory(dir=TEMP) as folder:
                owner = self.owner(folder)
                owner._publish()
                old = owner.query_point_path.read_bytes()
                owner._sim.knowledge_seq = 4
                owner._status.update(durable_seq=4, mirrored_seq=4)
                owner._query_summary_status = {'published_seq': 4}
                if broken == 'source': owner._status['durable_seq'] = 3
                if broken == 'graph': owner._status['mirrored_seq'] = 3
                if broken == 'summary': owner._query_summary_status = {'published_seq': 3}
                if broken == 'mirror-error': owner._status['last_error'] = 'actual graph failure'
                with self.assertRaisesRegex(RuntimeError, 'successful source, graph and summary'):
                    owner._publish()
                self.assertEqual(owner.query_point_path.read_bytes(), old)
                self.assertEqual(owner._last_query_point, ('live-point-run', 3))

    def test_failed_latest_or_point_write_does_not_mark_new_point_published(self):
        for fail_target in ('latest.json', 'query-point.json'):
            with self.subTest(target=fail_target), tempfile.TemporaryDirectory(dir=TEMP) as folder:
                owner = self.owner(folder)
                owner._publish()
                old = owner.query_point_path.read_bytes()
                owner._sim.knowledge_seq = 4
                owner._status.update(durable_seq=4, mirrored_seq=4)
                owner._query_summary_status = {'published_seq': 4}
                def failed_write(path, value, **options):
                    if Path(path).name == fail_target: raise OSError('unit filesystem failure')
                    write_checkpoint(path, value)
                owner._core.persistence.write_checkpoint = failed_write
                with self.assertRaisesRegex(OSError, 'unit filesystem'):
                    owner._publish()
                self.assertEqual(owner.query_point_path.read_bytes(), old)
                self.assertEqual(owner._last_query_point, ('live-point-run', 3))

    def test_actual_persist_failures_leave_previous_point_unchanged(self):
        for broken in ('source', 'graph', 'summary'):
            with self.subTest(broken=broken), tempfile.TemporaryDirectory(dir=TEMP) as folder:
                owner = self.owner(folder)
                owner._publish()
                old = owner.query_point_path.read_bytes()
                def append(cp, events):
                    if broken == 'source': raise RuntimeError('unit source failure')
                    owner._status.update(durable_seq=4, mirrored_seq=3 if broken == 'graph' else 4)
                owner._store.append = append
                if broken == 'graph': owner._store.last_error = 'unit graph failure'
                def summary(*args, **kwargs):
                    raise RuntimeError('unit summary failure')
                owner._query_summary = SimpleNamespace(catch_up_from_graph=summary)
                with self.assertRaises(RuntimeError): owner._persist()
                self.assertEqual(owner.query_point_path.read_bytes(), old)
                self.assertEqual(owner._last_query_point, ('live-point-run', 3))

    def test_constructor_rejects_existing_outside_nested_or_non_graph_paths(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            output = Path(folder) / 'output'
            output.mkdir()
            existing = output / 'existing.json'
            existing.write_bytes(b'original owned artifact')
            options = dict(source_root=ROOT, output_root=output, datasets={'sample': {}},
                dataset_id='sample', knowledge_backend='neo4j', query_summary_root=output,
                legacy_a_graph=True)
            with self.assertRaises(FileExistsError):
                WarehouseLiveRuntime(**options, query_point_path=existing)
            self.assertEqual(existing.read_bytes(), b'original owned artifact')
            for path in (Path(folder) / 'outside.json', output / 'nested' / 'point.json'):
                with self.subTest(path=path), self.assertRaisesRegex(ValueError, 'direct child'):
                    WarehouseLiveRuntime(**options, query_point_path=path)
            with self.assertRaisesRegex(ValueError, 'Neo4j and enabled query summaries'):
                WarehouseLiveRuntime(**{**options, 'query_summary_root': None},
                    query_point_path=output / 'point.json')
            with self.assertRaisesRegex(ValueError, 'Neo4j and enabled query summaries'):
                WarehouseLiveRuntime(**{**options, 'query_summary_root': None, 'knowledge_backend': 'journal'},
                    query_point_path=output / 'point.json')

    def test_sha_owner_version_and_bounded_envelope_are_verified(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            point = Path(folder) / 'point.json'
            for changed in ({'runtime_instance_id': 'other-owner'}, {'seq': 0}, {'seq': True},
                            {'version': 'wrong'}, {'run_id': '../outside'}, {'source_sha256': 'broken'}):
                write_checkpoint(point, payload(**changed))
                with self.subTest(changed=changed), self.assertRaises(ValueError):
                    sidecar._read_live_query_point(point, 'owner-123')
            write_checkpoint(point, payload())
            bad = json.loads(point.read_bytes())
            bad['payload']['seq'] += 1
            point.write_text(json.dumps(bad), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'SHA256 mismatch'):
                sidecar._read_live_query_point(point, 'owner-123')
            for raw in (json.dumps(payload()).encode(), b'x' * 8193):
                point.write_bytes(raw)
                with self.assertRaises(ValueError):
                    sidecar._read_live_query_point(point, 'owner-123')

    def test_options_are_paired_and_cli_rejects_before_loading_a_catalog(self):
        for args in ((Path('point.json'), None), (None, 'owner'), (Path('point.json'), True)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                sidecar._point_options(*args)
        for args in (['--live-query-point', 'point.json'], ['--expected-runtime-instance', 'owner']):
            with self.subTest(args=args), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
                sidecar.main(args)
            self.assertEqual(exc.exception.code, 2)


@unittest.skipUnless(TestClient is not None, 'optional local FastAPI dependencies required')
class LivePointSidecarHTTPTests(unittest.TestCase):
    def app(self, point=None, expected='owner-123'):
        calls = []
        class Service:
            def execute(self, body):
                calls.append(body)
                return {'original_catalog_result': body}
            def close(self): pass
        kwargs = {'live_query_point': point, 'expected_runtime_instance': expected} if point else {}
        return sidecar.create_app(catalog_factory=Service, **kwargs), calls

    def test_fill_missing_values_keep_older_cutoff_and_reject_cross_run_or_future(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            point = Path(folder) / 'point.json'
            write_checkpoint(point, payload())
            app, calls = self.app(point)
            with TestClient(app) as client:
                for body in ({'query_id': 'self'}, {'query_id': 'self', 'run_id': 'live-point-run'},
                             {'query_id': 'self', 'seq': 2}):
                    response = client.post('/api/knowledge/query', json=body)
                    self.assertEqual(response.status_code, 200)
                    actual = response.json()['original_catalog_result']
                    self.assertEqual(actual['run_id'], 'live-point-run')
                    self.assertEqual(actual['seq'], body.get('seq', 3))
                for body in ({'run_id': 'other-run'}, {'seq': 4}, {'seq': True}, {'seq': 0},
                             {'run_id': None}, {'seq': None}):
                    self.assertEqual(client.post('/api/knowledge/query', json=body).status_code, 400)
                self.assertEqual(len(calls), 3)
                write_checkpoint(point, payload(run_id='new-current-run', seq=1))
                self.assertEqual(client.post('/api/knowledge/query', json={'seq': 2}).status_code, 400)
                self.assertEqual(client.post('/api/knowledge/query', json={}).json()[
                    'original_catalog_result'], {'run_id': 'new-current-run', 'seq': 1})

    def test_point_failure_is_503_and_never_calls_catalog_or_reads_latest(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            point = Path(folder) / 'point.json'
            app, calls = self.app(point)
            with TestClient(app) as client:
                self.assertEqual(client.post('/api/knowledge/query', json={}).status_code, 503)
                write_checkpoint(point, payload(runtime_instance_id='other-owner'))
                self.assertEqual(client.post('/api/knowledge/query', json={}).status_code, 503)
                point.write_bytes(b'{"format":"warehouse-checkpoint-v2","sha256":"bad","payload":{}}')
                self.assertEqual(client.post('/api/knowledge/query', json={}).status_code, 503)
                write_checkpoint(point, payload())
                original = Path.open
                opened = []
                def tracked(path, *args, **kwargs):
                    opened.append(Path(path))
                    if Path(path).name == 'latest.json': raise AssertionError('heavy snapshot read')
                    return original(path, *args, **kwargs)
                with patch.object(Path, 'open', tracked):
                    self.assertEqual(client.post('/api/knowledge/query', json={}).status_code, 200)
                self.assertEqual(opened, [point])
                self.assertEqual(len(calls), 1)

    def test_same_origin_or_absent_allowed_cross_origin_rejected(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as folder:
            point = Path(folder) / 'point.json'
            write_checkpoint(point, payload())
            app, calls = self.app(point)
            with TestClient(app) as client:
                for origin in ('http://evil.invalid', 'null'):
                    response = client.post('/api/knowledge/query', json={}, headers={'Origin': origin})
                    self.assertEqual(response.status_code, 403)
                    self.assertEqual(response.headers['Cache-Control'], 'no-store')
                for headers in ({}, {'Origin': 'http://testserver'}):
                    self.assertEqual(client.post('/api/knowledge/query', json={}, headers=headers).status_code, 200)
                self.assertEqual(len(calls), 2)

    def test_no_point_keeps_explicit_unfenced_old_contract(self):
        app, calls = self.app()
        with TestClient(app) as client:
            self.assertEqual(client.post('/api/knowledge/query', json={}).status_code, 400)
            explicit = {'run_id': 'any-explicit-run', 'seq': 999}
            self.assertEqual(client.post('/api/knowledge/query', json=explicit).json(),
                             {'original_catalog_result': explicit})
        self.assertEqual(calls, [explicit])


if __name__ == '__main__': unittest.main()
