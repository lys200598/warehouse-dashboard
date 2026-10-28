"""Owned live producer over an explicitly selected, unchanged warehouse core.

All physics, journal writes and controls run on one worker thread. HTTP/WS
handlers consume snapshots or submit commands; they never touch the simulator.
No legacy server, replay cursor, random demo, signal pause, or old run is used.
"""
from collections import Counter, deque
from concurrent.futures import Future
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import queue
import re
import sys
import threading
import time
import traceback
from types import SimpleNamespace
import uuid


VERSION = 'warehouse-owned-live-runtime-v1'


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def run_name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', value):
        raise ValueError('run_id must be a safe single directory name (1..128 ASCII characters)')
    return value


def positive_speed(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not .1 <= value <= 120:
        raise ValueError('wall-clock speed must be finite and within [0.1,120]')
    return float(value)


def validate_disturbances(value):
    if not isinstance(value, list):
        raise ValueError('disturbances must be a list')
    kinds = {'soc', 'delay', 'stop', 'persistent_stop', 'clear_fault', 'handoff'}
    selectors = {'carrying', 'moving', 'carrying_moving', 'handoff_receiver', 'last_persistent'}
    for item in value:
        if not isinstance(item, dict) or type(item.get('at')) is not int or item['at'] < 0 or item.get('kind') not in kinds:
            raise ValueError('disturbance needs a nonnegative integer at and supported kind')
        selector = item.get('robot', 'carrying')
        if not (type(selector) is int and 1 <= selector <= 100 or isinstance(selector, str) and selector in selectors):
            raise ValueError('unsupported disturbance robot selector')
        if type(item.get('selection_timeout', 300)) is not int or item.get('selection_timeout', 300) < 0:
            raise ValueError('disturbance selection_timeout must be nonnegative integer seconds')
        if item['kind'] == 'soc':
            soc = item.get('soc')
            if isinstance(soc, bool) or not isinstance(soc, (int, float)) or not math.isfinite(soc) or not 0 <= soc <= 100:
                raise ValueError('reported SOC must be finite in [0,100]')
        if item['kind'] in ('delay', 'stop') and (type(item.get('seconds')) is not int or item['seconds'] <= 0):
            raise ValueError('delay/stop seconds must be a positive integer')
        if 'handoff' in item and type(item['handoff']) is not bool:
            raise ValueError('handoff flag must be boolean')
    return value


def _load_core(root):
    root = Path(root).resolve(strict=True)
    loaded = sys.modules.get('backend')
    if loaded is not None and Path(loaded.__file__).resolve().parent.parent != root:
        raise ValueError('a different backend tree is already imported; start a fresh explicitly configured process')
    sys.path.insert(0, str(root))
    modules = {key: importlib.import_module(name) for key, name in {
        'sim': 'backend.knowledge_runtime.sim', 'writer': 'backend.real_scale.writer',
        'knowledge': 'backend.knowledge', 'checkpoint': 'backend.checkpoint',
        'persistence': 'backend.real_scale.persistence', 'layout': 'backend.scheduling_v3.layout',
        'provider': 'backend.knowledge_runtime.provider',
        'queries': 'backend.query_v2.service_fragments',
    }.items()}
    if Path(modules['sim'].__file__).resolve().parents[2] != root:
        raise ValueError('selected producer import did not resolve to its explicit source_root')
    files = {p.relative_to(root).as_posix(): file_sha(p) for p in sorted((root / 'backend').rglob('*'))
             if p.is_file() and p.suffix in ('.py', '.json')}
    manifest_path = root / 'source-manifest.json'
    if manifest_path.exists():
        expected = json.loads(manifest_path.read_text(encoding='utf-8'))['files']
        for name, checksum in files.items():
            if expected.get(name) != checksum:
                raise ValueError('frozen backend file changed or is unlisted: ' + name)
    return SimpleNamespace(**modules), digest(files)


class WarehouseLiveRuntime:
    def __init__(self, *, source_root, output_root, datasets, dataset_id,
                 run_id=None, config=None, commit_seconds=5, knowledge_mode='rules',
                 knowledge_backend='journal', wall_interval=1., disturbances_json=None,
                 resume_run_id=None, expected_neo4j_uri=None, startup_timeout=180, resource_graph=False):
        if knowledge_mode not in ('rules', 'required') or knowledge_backend not in ('journal', 'neo4j'):
            raise ValueError('explicit rules/required and journal/neo4j modes required')
        if knowledge_mode == 'required' and knowledge_backend != 'neo4j':
            raise ValueError('required live history needs Neo4j')
        if type(commit_seconds) is not int or not 1 <= commit_seconds <= 300:
            raise ValueError('commit_seconds must be within [1,300]')
        if isinstance(wall_interval, bool) or not math.isfinite(wall_interval) or wall_interval <= 0:
            raise ValueError('wall_interval must be positive')
        if not isinstance(datasets, dict) or dataset_id not in datasets:
            raise ValueError('dataset_id must select a server-registered dataset')
        if run_id and resume_run_id and run_id != resume_run_id:
            raise ValueError('run_id and resume_run_id disagree')
        self.source_root = Path(source_root).resolve(strict=True)
        self.output_root = Path(output_root).resolve()
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.registry = {}
        for alias, spec in datasets.items():
            if not isinstance(alias, str) or not isinstance(spec, dict) or set(spec) - {'scene', 'dataset', 'config', 'config_json', 'disturbances_json', 'label'}:
                raise ValueError('invalid registered dataset specification')
            normalized = deepcopy(spec)
            normalized['scene'] = str(Path(spec['scene']).resolve(strict=True))
            normalized['dataset'] = str(Path(spec['dataset']).resolve(strict=True))
            if normalized.get('config_json'):
                normalized['config_json'] = str(Path(normalized['config_json']).resolve(strict=True))
            if normalized.get('disturbances_json'):
                normalized['disturbances_json'] = str(Path(normalized['disturbances_json']).resolve(strict=True))
            self.registry[alias] = normalized
        self.default_config = deepcopy(config or {})
        self.default_disturbances = str(Path(disturbances_json).resolve(strict=True)) if disturbances_json else None
        self.commit_seconds = commit_seconds
        self.knowledge_mode = knowledge_mode
        if type(resource_graph) is not bool or resource_graph and knowledge_backend!='neo4j':
            raise ValueError('resource_graph requires explicit Neo4j mode')
        self.resource_graph_enabled=resource_graph
        self.knowledge_backend = knowledge_backend
        self.wall_interval = float(wall_interval)
        self.expected_neo4j_uri = expected_neo4j_uri
        self.runtime_instance_id = str(uuid.uuid4())
        self._speed = 1.
        self._commands = queue.Queue(maxsize=128)
        self._lock = threading.RLock()
        self._changed = threading.Condition(self._lock)
        self._revision = 0
        self._latest = None
        self._state = 'initializing'
        self._error = None
        self._store = self._provider = self._sim = None
        self._stopping = False
        self._ready = Future()
        self._initial = (dataset_id, run_id, resume_run_id)
        self._thread = threading.Thread(target=self._worker, name='warehouse-live-owner', daemon=True)
        self._thread.start()
        try:
            self._ready.result(timeout=startup_timeout)
        except TimeoutError:
            self._stopping = True
            raise

    def latest(self):
        with self._lock:
            return deepcopy(self._latest)

    def status(self):
        snap = self.latest()
        return {key: snap.get(key) for key in ('runId', 'runtimeInstanceId', 'status', 'running', 'elapsed', 'tick',
                                              'speed', 'knowledge', 'metadata', 'lastError', 'liveRevision')}

    def scene(self):
        with self._lock:
            return deepcopy(self._scene)

    def scene_point(self):
        with self._lock:
            return {'run_id':self._latest['runId'],
                    'scene_sha256':self._latest['metadata']['scene_sha256'],
                    'scene':deepcopy(self._published_scene)}

    def datasets(self):
        return [{'dataset_id': alias, 'label': spec.get('label', alias)} for alias, spec in self.registry.items()]

    def watch(self, after_revision=0, timeout=15):
        with self._changed:
            self._changed.wait_for(lambda: self._revision > after_revision or self._stopping, timeout)
            return deepcopy(self._latest)

    def submit(self, action, value=None, expected_run_id=None):
        future = Future()
        if self._stopping or not self._thread.is_alive():
            future.set_exception(RuntimeError('live runtime has shut down'))
            return future
        try:
            self._commands.put_nowait((action, deepcopy(value), expected_run_id, future))
        except queue.Full:
            future.set_exception(RuntimeError('control queue full; no command was applied'))
        return future

    def command(self, action, value=None, expected_run_id=None, timeout=180):
        return self.submit(action, value, expected_run_id).result(timeout=timeout)

    def shutdown(self, timeout=180):
        if not self._thread.is_alive():
            return
        self.command('shutdown', timeout=timeout)
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError('owned live worker did not stop at its step boundary')

    def _worker(self):
        try:
            self._core, self._source_sha = _load_core(self.source_root)
            dataset, run, resume = self._initial
            self._create_run(dataset, run_id=run or resume, resume=bool(resume))
            self._ready.set_result(True)
        except Exception as exc:
            self._ready.set_exception(exc)
            self._close()
            return
        next_step = time.monotonic()
        while not self._stopping:
            wait = max(0., next_step - time.monotonic()) if self._state == 'running' else .5
            try:
                action, value, expected, future = self._commands.get(timeout=wait)
            except queue.Empty:
                if self._state == 'running':
                    begin = time.monotonic()
                    try:
                        self._advance_one()
                    except Exception as exc:
                        self._fail(exc)
                    next_step = max(time.monotonic(), begin + self.wall_interval / self._speed)
                continue
            if not future.set_running_or_notify_cancel():
                continue
            try:
                result = self._control(action, value, expected)
                future.set_result(result)
            except (ValueError, KeyError, FileExistsError) as exc:
                # Rejected input never changes physical state or fails the run.
                future.set_exception(exc)
            except Exception as exc:
                self._fail(exc)
                future.set_exception(exc)
            next_step = time.monotonic() + self.wall_interval / self._speed
        self._close()
        while not self._commands.empty():
            _, _, _, future = self._commands.get_nowait()
            if not future.done():
                future.set_exception(RuntimeError('live worker shut down before applying command'))

    def _create_run(self, dataset_id, *, run_id=None, config=None, resume=False):
        names = ('_output', '_dataset_id', '_identity', '_sim', '_store', '_provider',
                 '_disturbances', '_applied', '_done', '_events', '_error', '_last_commit',
                 '_rate_points', '_run_started', '_state', '_scene')
        previous = {name: getattr(self, name, None) for name in names}
        try:
            self._create_run_impl(dataset_id, run_id=run_id, config=config, resume=resume)
        except Exception as exc:
            if self._store is not previous['_store']:
                self._close()
            for name, value in previous.items():
                setattr(self, name, value)
            if previous['_sim'] is not None:
                raise ValueError('new_run initialization rejected; prior run preserved: ' + str(exc)) from exc
            raise
        # Only retire old resources AFTER the new, independently owned run has
        # completed its initial durable boundary and published a real frame.
        try:
            if previous['_provider'] is not None:
                previous['_provider'].close()
            if previous['_store'] is not None:
                previous['_store'].close()
        except Exception:
            traceback.print_exc()

    def _create_run_impl(self, dataset_id, *, run_id=None, config=None, resume=False):
        if dataset_id not in self.registry:
            raise ValueError('unknown registered dataset_id')
        spec = self.registry[dataset_id]
        run_id = run_name(run_id or ('wh-live-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid.uuid4().hex[:8]))
        output = (self.output_root / run_id).resolve()
        if output.parent != self.output_root or output == self.source_root:
            raise ValueError('run output escaped its dedicated owner root')
        cfg = dict(self.default_config)
        if spec.get('config_json'):
            cfg.update(json.loads(Path(spec['config_json']).read_text(encoding='utf-8')))
        cfg.update(spec.get('config') or {})
        cfg.update(config or {})
        physical_config = self._core.writer.Config(**cfg)
        disturbance_path = spec.get('disturbances_json') or self.default_disturbances
        disturbances = json.loads(Path(disturbance_path).read_text(encoding='utf-8')) if disturbance_path else []
        validate_disturbances(disturbances)
        identity = {'version': VERSION, 'run_id': run_id, 'dataset_id': dataset_id,
                    'source_sha256': self._source_sha, 'adapter_sha256': file_sha(__file__),
                    'scene_sha256': file_sha(spec['scene']),
                    'tasks_sha256': file_sha(Path(spec['dataset']) / 'tasks.csv'),
                    'initial_inventory_sha256': file_sha(Path(spec['dataset']) / 'initial_inventory.csv'),
                    'manifest_sha256': file_sha(Path(spec['dataset']) / 'manifest.json'),
                    'config': asdict(physical_config), 'commit_seconds': self.commit_seconds,
                    'knowledge_mode': self.knowledge_mode, 'knowledge_backend': self.knowledge_backend,
                    'resource_graph':self.resource_graph_enabled,
                    'disturbance_sha256': digest(disturbances)}
        checkpoint = None
        if resume:
            if any(output.glob('failure-*.json')):
                raise ValueError('failed boundary requires explicit operator reconciliation before resume')
            old = json.loads((output / 'live-manifest.json').read_text(encoding='utf-8'))
            if old != identity:
                raise ValueError('owned live source/input/config/adapter identity changed; refusing resume')
        elif output.exists():
            raise FileExistsError('existing run output preserved; choose a new run_id')
        if self.knowledge_backend == 'neo4j':
            uri = os.environ.get('NEO4J_URI')
            if not uri or self.expected_neo4j_uri and uri != self.expected_neo4j_uri:
                raise ValueError('live Neo4j URI differs from its explicit isolated target')
        # Validate core/input before closing a previous completed/paused run.
        sim = self._core.sim.KnowledgeSimulation(spec['scene'], spec['dataset'], run_id,
            physical_config, commit_seconds=self.commit_seconds, knowledge_mode=self.knowledge_mode,
            **({'resource_graph':True} if self.resource_graph_enabled else {}))
        layout = self._core.layout.audit_roadmap(sim.roadmap, circle_probes=False)
        if not layout['initial_roadmap_usable']:
            raise ValueError('layout static safety gate failed')
        output.mkdir(parents=False, exist_ok=resume)
        mirror_class=self._core.writer.RealMirror
        if self.resource_graph_enabled:
            from backend.resource_graph_mirror import ResourceMirror
            mirror_class=ResourceMirror
        mirror = mirror_class.from_env() if self.knowledge_backend == 'neo4j' else None
        try:
            if mirror and not resume and mirror.query('MATCH (r:WHRun {run_id:$run}) RETURN count(r) AS runs', {'run': run_id})[0]['runs']:
                raise ValueError('graph run_id already exists; old run is protected')
            store = self._core.knowledge.JournalKnowledgeStore(output / 'journal.sqlite', mirror)
        except BaseException:
            if mirror:
                mirror.close()
            raise
        self._output, self._dataset_id, self._identity = output, dataset_id, identity
        self._sim, self._store = sim, store
        self._provider = None
        self._disturbances, self._applied, self._done = disturbances, [], set()
        self._events = deque(maxlen=100)
        self._error = None
        self._last_commit = sim.elapsed
        self._rate_points = deque(maxlen=60)
        self._rate_points.append((time.monotonic(), sim.elapsed))
        self._run_started = time.monotonic()
        if resume:
            state = store.recovery_payload(self._core.checkpoint.read_checkpoint(output / 'checkpoint.json'))
            if state.get('experiment', {}).get('disturbance_sha256') != digest(disturbances):
                raise ValueError('resume disturbance identity changed')
            if any(not row['success'] for row in state['experiment']['applied']):
                raise ValueError('failed external disturbance needs explicit reconciliation')
            sim.restore(state)
            self._applied = deepcopy(state['experiment']['applied'])
            self._done = {row['index'] for row in self._applied}
            self._last_commit = sim.elapsed
            store.flush(run_id)
        else:
            with (output / 'live-manifest.json').open('x', encoding='utf-8', newline='\n') as stream:
                stream.write(canonical(identity) + '\n')
        if self.knowledge_mode == 'required':
            self._provider = self._core.provider.GraphHistoryProvider(store,
                self._core.queries.FragmentQueryService.from_env(timeout=5, pool_size=4), max_lag_seconds=30)
            sim.history_provider = self._provider
        sim.running = False
        self._rate_points.clear()
        self._rate_points.append((time.monotonic(), sim.elapsed))
        already_complete = (resume and sim.elapsed >= sim.manifest['duration_seconds']
            and sim.cursor == len(sim.tasks) and sim.metrics['completed'] == len(sim.tasks) and not sim.assists)
        self._state = 'complete' if already_complete else ('paused' if resume else 'ready')
        self._scene = deepcopy(sim.scene)
        self._persist()
        if already_complete:
            self._write_results()
        self._publish()

    def _control(self, action, value, expected):
        if expected is not None and expected != self._sim.run_id:
            raise ValueError('stale run control rejected; refresh current run_id')
        if action in ('new_run', 'newrun'):
            if self._state == 'running':
                raise ValueError('pause the current run before creating a new run')
            if not isinstance(value, dict) or set(value) - {'dataset_id', 'run_id', 'config'}:
                raise ValueError('new_run accepts only registered dataset_id, optional run_id and config')
            self._create_run(value.get('dataset_id', self._dataset_id), run_id=value.get('run_id'), config=value.get('config'))
            return self.latest()
        if action == 'shutdown':
            if self._state != 'failed':
                self._sim.running = False
                if self._state != 'complete':
                    self._state = 'paused'
                self._physical(self._persist)
                if self._state == 'complete':
                    self._physical(self._write_results)
                self._physical(self._publish)
            self._stopping = True
            return self.latest()
        if self._state == 'failed':
            raise ValueError('failed run preserved; create an explicit new_run or restart with matching owned resume')
        if action in ('speed', 'set_speed'):
            self._speed = positive_speed(value)
            self._physical(self._publish)
            return self.latest()
        if action in ('start', 'resume'):
            if self._state == 'complete':
                raise ValueError('completed run preserved; choose new_run')
            self._sim.running = True
            self._state = 'running'
            self._physical(self._persist)
        elif action == 'pause':
            self._sim.running = False
            if self._state != 'complete':
                self._state = 'paused'
            self._physical(self._persist)
        elif action == 'step':
            if self._state not in ('ready', 'paused'):
                raise ValueError('step requires a ready/paused run')
            self._physical(lambda: self._advance_one(force_persist=True))
            return self.latest()
        elif action == 'checkpoint':
            self._physical(self._persist)
        else:
            raise ValueError('unsupported fixed-input control; physical config changes require an explicit new_run')
        if self._state == 'complete':
            self._physical(self._write_results)
        self._physical(self._publish)
        return self.latest()

    @staticmethod
    def _physical(operation):
        try:
            return operation()
        except ValueError as exc:
            # Source validation failures at a physical/persistence boundary
            # must never be mistaken for a harmless rejected HTTP parameter.
            raise RuntimeError('live producer boundary failed: ' + str(exc)) from exc

    def _persist(self):
        sim, store = self._sim, self._store
        start_ns = time.time_ns()
        sim.knowledge_seq += 1
        checkpoint = sim.checkpoint()
        checkpoint['experiment'] = {'disturbance_sha256': digest(self._disturbances), 'applied': deepcopy(self._applied)}
        store.append(checkpoint, sim.pending_events)
        self._events.extend(deepcopy(sim.pending_events))
        sim.pending_events = []
        self._core.persistence.write_checkpoint(self._output / 'checkpoint.json', checkpoint)
        self._last_commit = sim.elapsed
        status = store.status(sim.run_id)
        with (self._output / 'writer.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(canonical({'run_id': sim.run_id, 'seq': sim.knowledge_seq, 'sim_time': sim.elapsed,
                'started_ns': start_ns, 'observed_at_ns': time.time_ns(), **status}) + '\n')
        if store.mirror and store.last_error:
            raise RuntimeError('graph mirror failed; durable boundary preserved: ' + str(store.last_error))

    def _disturb(self):
        sim = self._sim
        for index, spec in enumerate(self._disturbances):
            if index in self._done or sim.elapsed < spec['at']:
                continue
            selector = spec.get('robot', 'carrying')
            if type(selector) is int:
                owner = selector if selector in sim.robots else None
            elif selector == 'last_persistent':
                previous = [row['owner'] for row in self._applied if row['kind'] == 'persistent_stop' and row['success']]
                owner = previous[-1] if previous else None
            else:
                owners = [i for i, r in sim.robots.items() if
                    (selector == 'carrying' and r['carrying'] and i not in sim.assists
                     or selector == 'moving' and r['stage'] in {'to_source', 'to_target', 'returning'}
                     or selector == 'carrying_moving' and r['carrying'] and r['stage'] == 'to_target' and i not in sim.assists
                     or selector == 'handoff_receiver' and any(a['receiver'] == i for a in sim.assists.values()))]
                owner = min(owners) if owners else None
            if owner is None:
                if sim.elapsed - spec['at'] > spec.get('selection_timeout', 300):
                    raise RuntimeError(f'disturbance {index} had no eligible robot')
                continue
            # Same coherent pre/post external-measurement publication as the
            # frozen runner, including source cutoff before history admission.
            if self.knowledge_mode == 'required':
                self._persist()
            kind = spec['kind']
            try:
                if kind == 'soc':
                    ok = sim.report_soc(owner, spec['soc'])
                elif kind == 'delay':
                    ok = sim.inject_delay(owner, spec['seconds'])
                elif kind == 'stop':
                    ok = sim.inject_stop(owner, spec['seconds'])
                    if ok and spec.get('handoff'):
                        ok = sim.request_assistance(owner, reason='finite_drive_fault')
                elif kind == 'persistent_stop':
                    ok = sim.inject_stop(owner, None, reason='persistent_drive_fault')
                    if ok and sim.robots[owner]['carrying']:
                        ok = sim.request_assistance(owner, reason='persistent_drive_fault')
                elif kind == 'clear_fault':
                    ok = sim.clear_fault(owner)
                elif kind == 'handoff':
                    ok = sim.request_assistance(owner)
                else:
                    raise ValueError('unknown disturbance kind')
            except Exception as exc:
                self._applied.append({'index': index, 'requested_at': spec['at'], 'applied_at': sim.elapsed,
                    'kind': kind, 'owner': owner, 'success': False, 'error': type(exc).__name__ + ': ' + str(exc),
                    'resolution': 'explicit_operator_reconciliation_required'})
                self._done.add(index)
                raise
            self._applied.append({'index': index, 'requested_at': spec['at'], 'applied_at': sim.elapsed,
                                  'kind': kind, 'owner': owner, 'success': bool(ok)})
            self._done.add(index)
            if not ok:
                raise RuntimeError(f'disturbance {index} was not safely recovered')
            self._persist()

    def _advance_one(self, force_persist=False):
        sim = self._sim
        self._disturb()
        if sim.knowledge_boundary_due() and sim.elapsed != self._last_commit:
            self._persist()
        sim.step()
        if force_persist or sim.elapsed - self._last_commit >= self.commit_seconds:
            self._persist()
        complete = sim.elapsed >= sim.manifest['duration_seconds'] and sim.cursor == len(sim.tasks) and sim.metrics['completed'] == len(sim.tasks) and not sim.assists
        if complete:
            if len(self._done) != len(self._disturbances):
                raise RuntimeError('input complete but requested disturbances remain unexercised')
            sim.running = False
            self._state = 'complete'
            self._persist()
            self._write_results()
        elif force_persist:
            self._state = 'paused'
        self._rate_points.append((time.monotonic(), sim.elapsed))
        self._publish()

    def _publish(self):
        snap = self._sim.frame()
        stamp = int(time.time() * 1000)
        knowledge = self._store.status(self._sim.run_id)
        knowledge.update(committed_sim_time=self._last_commit, physical_sim_time=self._sim.elapsed)
        first, last = self._rate_points[0], self._rate_points[-1]
        rate = (last[1] - first[1]) / max(1e-9, last[0] - first[0]) if len(self._rate_points) > 1 else 0.
        snap.update(status=self._state, speed=self._speed, generatedAtMs=stamp, capturedAtMs=stamp,
                    runtimeInstanceId=self.runtime_instance_id,
                    sourceKind='live_simulation_memory', lastError=self._error, knowledge=knowledge,
                    events=deepcopy(list(self._events) + self._sim.pending_events)[-100:],
                    runtime={'workerProcessCount': 1, 'workerPids': [os.getpid()], 'coordination': 'single-central-owner'},
                    measured_simulation_speed=rate,
                    metadata={'dataset_id': self._dataset_id, 'dataset_sha256': self._identity['tasks_sha256'],
                        'scene_sha256': self._identity['scene_sha256'], 'initial_inventory_sha256': self._identity['initial_inventory_sha256'],
                        'manifest_sha256': self._identity['manifest_sha256'], 'source_sha256': self._source_sha,
                        'adapter_sha256': self._identity['adapter_sha256'], 'commit_seconds': self.commit_seconds,
                        'knowledge_mode': self.knowledge_mode, 'knowledge_backend': self.knowledge_backend,
                        'output': str(self._output), 'motion_speed_mps': self._sim.config.speed,
                        'resource_graph':self.resource_graph_enabled,
                        'fleet_mode':getattr(self._sim.config,'fleet_mode','all-x3'),
                        'fleet_capabilities':self._sim.run_metadata.get('fleet_capabilities')})
        self._core.persistence.write_checkpoint(self._output / 'latest.json', snap)
        with self._changed:
            self._revision += 1
            snap['liveRevision'] = self._revision
            if self._latest is None or self._latest['metadata']['scene_sha256']!=snap['metadata']['scene_sha256']:
                self._published_scene=deepcopy(self._scene)
            self._latest = snap
            self._changed.notify_all()

    def _fail(self, exc):
        self._state = 'failed'
        self._error = type(exc).__name__ + ': ' + str(exc)
        # A physical step can raise after partial in-memory mutation. Do not
        # publish that state or append it as a valid commit. Freeze the last
        # completely published step; the last durable CP remains authoritative.
        raw = {'error': self._error, 'traceback': traceback.format_exc(),
               'run_id': self._sim.run_id, 'last_durable_seq': self._store.status(self._sim.run_id)['durable_seq'],
               'uncommitted_partial_state_not_published': True, 'disturbances': deepcopy(self._applied),
               'physical_state_for_diagnostics_only': deepcopy(list(self._sim.robots.values()))}
        try:
            with (self._output / f'failure-{time.time_ns()}.json').open('x', encoding='utf-8') as stream:
                stream.write(canonical(raw) + '\n')
        except Exception:
            traceback.print_exc()
        with self._changed:
            if self._latest:
                self._latest = deepcopy(self._latest)
                self._latest.update(status='failed', running=False, lastError=self._error)
                self._revision += 1
                self._latest['liveRevision'] = self._revision
            self._changed.notify_all()

    def _write_results(self):
        sim = self._sim
        released = [t for t in sim.tasks.values() if t['status'] != 'scheduled']
        flow = sorted(t['completed_at'] - t['release_second'] for t in released if t['status'] == 'completed')
        def quantile(q):
            return flow[min(len(flow) - 1, max(0, math.ceil(len(flow) * q) - 1))] if flow else 0
        result = {'run_id': sim.run_id, 'status': self._state, 'error': self._error, 'elapsed': sim.elapsed,
            'wall_seconds': time.monotonic() - self._run_started, 'config': asdict(sim.config),
            'input_identity': {k: self._identity[k] for k in ('scene_sha256', 'tasks_sha256', 'initial_inventory_sha256', 'manifest_sha256')},
            'code_identity': sim.run_metadata['execution_code_sha256'], 'tasks_total': len(sim.tasks),
            'released': len(released), 'unfinished': sum(t['status'] != 'completed' for t in released),
            'late_unfinished': sum(t['status'] != 'completed' and t['due_second'] < sim.elapsed for t in released),
            'task_statuses': dict(Counter(t['status'] for t in sim.tasks.values())),
            'flow_seconds': {'mean_completed_only': sum(flow) / max(1, len(flow)), 'p95': quantile(.95), 'p99': quantile(.99), 'max': max(flow, default=0)},
            'metrics': deepcopy(sim.metrics), 'disturbances': deepcopy(self._applied), 'inside': sim.inside,
            'knowledge': self._store.status(sim.run_id), 'scheduler': sim.execution_status(),
            'registered_robots': len(sim.robots), 'robots_completed_tasks': sum(r['completed'] > 0 for r in sim.robots.values()),
            'live_runtime': deepcopy(self._identity), 'graph_audit': None,
            'acceptance': {'all_input_completed': sim.metrics['completed'] == len(sim.tasks),
                'all_disturbances_exercised': len(self._done) == len(self._disturbances),
                'no_recorded_safety_failure': self._error is None, 'hardware_calibrated': False,
                'hundred_simultaneously_working': sim.metrics['max_active_business'] >= 100,
                'hundred_execution_processes': False, 'million_node_joint': False, 'latency_joint': False}}
        self._core.persistence.write_checkpoint(self._output / 'live-results-checkpoint.json', result)
        temporary = self._output / 'results.json.tmp'
        temporary.write_text(canonical(result) + '\n', encoding='utf-8')
        os.replace(temporary, self._output / 'results.json')

    def _close(self):
        try:
            if self._provider is not None:
                self._provider.close()
        finally:
            self._provider = None
            if self._store is not None:
                self._store.close()
                self._store = None
