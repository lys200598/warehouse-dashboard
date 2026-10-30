"""Owned live producer over an explicitly selected, unchanged warehouse core.

All physics, journal writes and controls run on one worker thread. HTTP/WS
handlers consume snapshots or submit commands; they never touch the simulator.
No legacy server, replay cursor, random demo, signal pause, or old run is used.
"""
from collections import Counter, deque
from concurrent.futures import Future, TimeoutError as FuturesTimeoutError
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

from ops.live_motion_view import LiveMotionView


VERSION = 'warehouse-owned-live-runtime-v1'
MAX_CATCH_UP_SECONDS = 3.0
QUERY_POINT_VERSION = 'warehouse-live-query-point-v1'


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
                 resume_run_id=None, expected_neo4j_uri=None, startup_timeout=180, resource_graph=False,
                 query_summary_root=None, query_summary_cache=None, query_point_path=None,
                 mobility='legacy', robot_agents=False, agent_options=None, max_active_robots=None,
                 ray=False, ray_address=None, ray_actor_cpus=0.1, robot_processes='off',
                 robot_graph=False, graph_state_interval=4,
                 global_writer=False, global_writer_queue=256, global_writer_url=None,
                 kg_state_source='memory', legacy_a_graph=False, robot_context_interval=10):
        if type(robot_context_interval) is not int or not 0 <= robot_context_interval <= 3600:
            raise ValueError('robot_context_interval must be within 0..3600')
        self.robot_context_interval = robot_context_interval
        if type(legacy_a_graph) is not bool:
            raise ValueError('legacy_a_graph must be boolean')
        self.legacy_a_graph = legacy_a_graph
        if kg_state_source not in ('memory', 'neo4j'):
            raise ValueError('kg_state_source must be memory or neo4j')
        if kg_state_source == 'neo4j' and (robot_processes != 'ray' or not robot_graph or not global_writer_url):
            raise ValueError('neo4j state requires --robot-processes ray --robot-graph --global-writer-url')
        self.kg_state_source = kg_state_source
        if max_active_robots is not None and (type(max_active_robots) is not int or not 1<=max_active_robots<=100):
            raise ValueError('max_active_robots must be an integer within 1..100, or None')
        self.max_active_robots=max_active_robots
        if mobility not in ('legacy','wheeled') or type(robot_agents) is not bool:
            raise ValueError('explicit mobility and robot_agents options required')
        if robot_agents:
            raise ValueError('--robot-agents is deprecated; use --robot-processes ray for robot graph I/O')
        self.mobility=mobility
        self.ray=bool(ray)
        if self.ray and mobility!='wheeled':
            raise ValueError('Ray medium execution requires mobility=wheeled')
        if robot_processes not in ('off','local','ray'):
            raise ValueError('robot_processes must be off, local or ray')
        if robot_processes!='off' and mobility!='wheeled':
            raise ValueError('robot processes require mobility=wheeled')
        if robot_processes!='off' and self.ray:
            raise ValueError('choose either --ray (motion actors) or --robot-processes, not both')
        self.robot_processes=robot_processes
        if isinstance(ray_actor_cpus,bool) or not isinstance(ray_actor_cpus,(int,float)) or not math.isfinite(ray_actor_cpus) or not 0<ray_actor_cpus<=1:
            raise ValueError('ray_actor_cpus must be finite within (0,1]')
        self.ray_address=ray_address
        self.ray_actor_cpus=float(ray_actor_cpus)
        self.robot_graph=bool(robot_graph)
        if self.robot_graph and not (self.ray or robot_processes=='ray'):
            raise ValueError('robot graph writers require --ray or --robot-processes ray')
        if type(graph_state_interval) is not int or not 1<=graph_state_interval<=300:
            raise ValueError('graph_state_interval must be an integer within 1..300')
        self.graph_state_interval=graph_state_interval
        self.global_writer_enabled=bool(global_writer)
        if self.global_writer_enabled and not (os.environ.get('WAREHOUSE_NEO4J_PASSWORD') or os.environ.get('NEO4J_PASSWORD')):
            raise ValueError('--global-writer requires WAREHOUSE_NEO4J_PASSWORD')
        if type(global_writer_queue) is not int or not 16<=global_writer_queue<=10000:
            raise ValueError('global_writer_queue must be an integer within 16..10000')
        self.global_writer_queue=global_writer_queue
        self._global_writer=None
        self._global_writer_error=None
        # HTTP global record service (lys, e.g. 14284); records are sent in tick order
        # by backend.global_record_client. Independent of the --global-writer queue path.
        self.global_writer_url=global_writer_url
        self._global_client=None
        self._global_client_run=None
        self.robot_agents=robot_agents
        self.agent_options=deepcopy(agent_options or {})
        if isinstance(startup_timeout,bool) or not isinstance(startup_timeout,(int,float)) or not math.isfinite(startup_timeout) or startup_timeout<=0:
            raise ValueError('startup_timeout must be finite and positive')
        if knowledge_mode not in ('rules', 'required') or knowledge_backend not in ('journal', 'neo4j'):
            raise ValueError('explicit rules/required and journal/neo4j modes required')
        if knowledge_mode == 'required' and knowledge_backend != 'neo4j':
            raise ValueError('required live history needs Neo4j')
        if knowledge_mode == 'required' and not self.legacy_a_graph:
            raise ValueError('legacy required-history decisions need explicit --legacy-a-graph; C context is observational')
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
        self.query_summary_root=Path(query_summary_root).resolve() if query_summary_root is not None else None
        self.query_summary_cache=Path(query_summary_cache).resolve() if query_summary_cache is not None else None
        if self.query_summary_root is not None:
            if not self.legacy_a_graph:
                raise ValueError('legacy A query summaries require explicit --legacy-a-graph')
            if knowledge_backend!='neo4j' or self.query_summary_root!=self.output_root:
                raise ValueError('live query summaries require Neo4j and the owned output_root as their journal root')
        elif self.query_summary_cache is not None:
            raise ValueError('query summary cache requires an enabled summary root')
        self.query_point_path = None
        self._last_query_point = None
        if query_point_path is not None:
            candidate = Path(query_point_path)
            resolved = candidate.resolve()
            if resolved.parent != self.output_root:
                raise ValueError('query point must be a direct child of the owned output_root')
            if candidate.exists() or candidate.is_symlink() or resolved.exists():
                raise FileExistsError('existing live query point is preserved; select a new owned path')
            if knowledge_backend != 'neo4j' or self.query_summary_root is None:
                raise ValueError('live query point requires Neo4j and enabled query summaries')
            self.query_point_path = resolved
        self._query_summary=None
        self._query_summary_status=None
        self.registry = {}
        for alias, spec in datasets.items():
            if not isinstance(alias, str) or not isinstance(spec, dict) or set(spec) - {'scene', 'dataset', 'config', 'config_json', 'disturbances_json', 'label', 'graph_dataset_id'}:
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
        if resource_graph and not self.legacy_a_graph:
            raise ValueError('legacy A resource graph requires explicit --legacy-a-graph')
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
        except FuturesTimeoutError as exc:
            self._stopping = True
            # Keep cleanup with the owner when a bounded driver call is in
            # progress. Cancelling its ready Future would break error cleanup.
            self._thread.join(min(5., startup_timeout))
            raise TimeoutError('live startup deadline exceeded; owner cancellation requested') from exc

    def latest(self):
        with self._lock:
            return deepcopy(self._latest)

    def status(self):
        snap = self.latest()
        return {key: snap.get(key) for key in ('runId', 'runtimeInstanceId', 'status', 'running', 'elapsed', 'tick',
                                              'speed', 'knowledge', 'metadata', 'lastError', 'liveRevision')}

    def query_point(self):
        """Atomically fence a query without copying the fleet/map/UI snapshot."""
        with self._lock:
            if self._latest is None:
                raise RuntimeError('live query point has not been published')
            return {'run_id':self._latest['runId'],
                    'seq':self._latest.get('knowledge',{}).get('mirrored_seq',0)}

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
            if self.query_summary_root is not None:
                from backend.query_views.summary import SummaryDirectory
                self._query_summary=SummaryDirectory(self.query_summary_root,cache_root=self.query_summary_cache)
            dataset, run, resume = self._initial
            self._create_run(dataset, run_id=run or resume, resume=bool(resume))
            if self._stopping:
                self._close()
                return
            self._ready.set_result(True)
        except Exception as exc:
            self._ready.set_exception(exc)
            self._close()
            return
        next_step = time.monotonic()
        while not self._stopping:
            if self._state != 'running':
                next_step = time.monotonic()
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
                    # Keep the requested wall-clock speed on average: after a
                    # slow tick (save or dispatch round) the next ticks run
                    # back-to-back until the schedule is met. Debt is capped so
                    # an overloaded run slows down instead of spiralling.
                    next_step = max(next_step + self.wall_interval / self._speed,
                                    time.monotonic() - MAX_CATCH_UP_SECONDS)
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
                 '_rate_points', '_run_started', '_state', '_scene', '_query_summary_status',
                 '_last_query_point', '_published_scene', '_latest', '_revision')
        previous = {name: getattr(self, name, None) for name in names}
        try:
            self._create_run_impl(dataset_id, run_id=run_id, config=config, resume=resume)
        except Exception as exc:
            if self._store is not previous['_store']:
                try:self._close()
                except Exception:traceback.print_exc()
            for name, value in previous.items():
                setattr(self, name, value)
            if previous['_sim'] is not None:
                raise ValueError('new_run initialization rejected; prior run preserved: ' + str(exc)) from exc
            raise
        # Only retire old resources AFTER the new, independently owned run has
        # completed its initial durable boundary and published a real frame.
        retirement_errors=[]
        close_previous = getattr(previous['_sim'], 'close', None)
        if close_previous is not None:
            try:close_previous()
            except Exception as exc:
                retirement_errors.append({'resource':'_sim','error':type(exc).__name__})
        for name in ('_provider','_store'):
            if previous[name] is not None:
                try:previous[name].close()
                except Exception as exc:
                    retirement_errors.append({'resource':name,'error':type(exc).__name__})
                    traceback.print_exc()
        if retirement_errors:
            try:
                (self._output/'retirement-errors.json').write_text(canonical(retirement_errors)+'\n',encoding='utf-8')
            finally:
                raise RuntimeError('prior live run resource cleanup failed; all resources attempted')

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
        # A launch-only override wins over dataset defaults for each new run.
        # Its effective value is recorded in the run identity/checkpoint, never
        # written back into the dataset configuration file.
        if self.max_active_robots is not None:
            cfg['max_active_robots']=self.max_active_robots
        if self.mobility=='legacy':
            simulation_class,config_class=self._core.sim.KnowledgeSimulation,self._core.writer.Config
        else:
            from backend.warehouse.composition import physical_classes
            simulation_class,config_class=physical_classes('wheeled_ray' if self.ray else self.mobility,
                robot_processes=getattr(self,'robot_processes','off'),ray_address=self.ray_address,ray_actor_cpus=self.ray_actor_cpus)
        physical_config = config_class(**cfg)
        disturbance_path = spec.get('disturbances_json') or self.default_disturbances
        disturbances = json.loads(Path(disturbance_path).read_text(encoding='utf-8')) if disturbance_path else []
        validate_disturbances(disturbances)
        identity = {'version': VERSION, 'run_id': run_id, 'dataset_id': dataset_id,
                    'graph_dataset_id': spec.get('graph_dataset_id') or dataset_id,
                    'mobility':self.mobility, 'ray':self.ray, 'ray_actor_cpus':self.ray_actor_cpus,
                    'robot_processes':getattr(self,'robot_processes','off'),
                    'kg_state_source':self.kg_state_source,
                    'ray_address':self.ray_address, 'robot_graph':self.robot_graph,
                    'graph_state_interval':self.graph_state_interval,
                    'global_writer':self.global_writer_enabled, 'global_writer_queue':self.global_writer_queue,
                    'robot_agents':self.robot_agents,
                    'legacy_a_graph':self.legacy_a_graph,
                    'robot_context_interval':self.robot_context_interval,
                    'agent_options':self.agent_options,
                    'source_sha256': self._source_sha, 'adapter_sha256': file_sha(__file__),
                    'scene_sha256': file_sha(spec['scene']),
                    'tasks_sha256': file_sha(Path(spec['dataset']) / 'tasks.csv'),
                    'initial_inventory_sha256': file_sha(Path(spec['dataset']) / 'initial_inventory.csv'),
                    'manifest_sha256': file_sha(Path(spec['dataset']) / 'manifest.json'),
                    'config': asdict(physical_config), 'commit_seconds': self.commit_seconds,
                    'knowledge_mode': self.knowledge_mode, 'knowledge_backend': self.knowledge_backend,
                    'resource_graph':self.resource_graph_enabled,
                    'disturbance_sha256': digest(disturbances)}
        if self.query_summary_root is not None:
            identity['query_summary']='source-journal-derived-exact-cutoff-v1'
        # Ray actor graph writers read this environment variable in
        # backend/ray_medium/sim.py.  Use the canonical Dataset id so their
        # Execution/RuntimeEvent rows join the static medium task graph.
        os.environ['WAREHOUSE_NEO4J_DATASET_ID'] = str(identity['graph_dataset_id'])
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
            uri = (os.environ.get('NEO4J_URI') if self.legacy_a_graph else
                   os.environ.get('WAREHOUSE_NEO4J_URI') or os.environ.get('NEO4J_URI'))
            if not uri or self.expected_neo4j_uri and uri != self.expected_neo4j_uri:
                raise ValueError('live Neo4j URI differs from its explicit isolated target')
        # Validate core/input before closing a previous completed/paused run.
        sim_kwargs = {'resource_graph':True} if self.resource_graph_enabled else {}
        if self.ray:
            sim_kwargs.update(ray_address=self.ray_address, ray_actor_cpus=self.ray_actor_cpus,
                              robot_graph=self.robot_graph, graph_state_interval=self.graph_state_interval)
        elif getattr(self,'robot_processes','off')=='ray':
            sim_kwargs.update(robot_graph=self.robot_graph, graph_state_interval=self.graph_state_interval,
                              kg_state_source=self.kg_state_source, global_writer_url=self.global_writer_url,
                              robot_context_interval=self.robot_context_interval)
        sim = simulation_class(spec['scene'], spec['dataset'], run_id,
            physical_config, commit_seconds=self.commit_seconds, knowledge_mode=self.knowledge_mode,
            **sim_kwargs)
        sim.run_metadata['kg_state_source'] = self.kg_state_source
        sim.global_record_identity = identity
        sim.context_log_path = str(output / 'robot-context.jsonl')
        layout = self._core.layout.audit_roadmap(sim.roadmap, circle_probes=False)
        if not layout['initial_roadmap_usable']:
            raise ValueError('layout static safety gate failed')
        output.mkdir(parents=False, exist_ok=resume)
        mirror_class=self._core.writer.RealMirror
        if self.resource_graph_enabled:
            from backend.resource_graph_mirror import ResourceMirror
            mirror_class=ResourceMirror
        if self.robot_agents:
            from backend.warehouse.composition import with_agents
            mirror_class=with_agents(mirror_class,output=output,enabled=True,options=self.agent_options)
        mirror = mirror_class.from_env() if self.knowledge_backend == 'neo4j' and self.legacy_a_graph else None
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
        self._query_summary_status=None
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

    def _ensure_global_writer(self):
        if not getattr(self, "global_writer_enabled", False) or self._global_writer is not None:
            return
        from backend.global_writer import GlobalWriterProcess
        config = {
            'uri': os.environ.get('WAREHOUSE_NEO4J_URI') or os.environ.get('NEO4J_URI') or 'bolt://127.0.0.1:47687',
            'user': os.environ.get('WAREHOUSE_NEO4J_USER') or os.environ.get('NEO4J_USER') or 'neo4j',
            'password': os.environ.get('WAREHOUSE_NEO4J_PASSWORD') or os.environ.get('NEO4J_PASSWORD'),
            'database': os.environ.get('WAREHOUSE_NEO4J_DATABASE') or os.environ.get('NEO4J_DATABASE') or 'neo4j',
        }
        self._global_writer = GlobalWriterProcess(config, max_queue=self.global_writer_queue)

    def _record_global_http(self, sim, events):
        """Send this commit's global record to the HTTP global record service.

        Waits for the robot-actor graph writes up to this tick first, so the Step
        marker lists only robots whose own records are really written."""
        try:
            from backend.global_record_client import GlobalRecordClient
            from backend.global_writer import build_global_record
            if self._global_client is None or self._global_client_run != sim.run_id:
                if self._global_client is not None:
                    self._global_client.close(timeout=5)
                self._global_client = GlobalRecordClient(self.global_writer_url, sim.run_id)
                self._global_client_run = sim.run_id
            receipt = sim.flush_robot_graph() if hasattr(sim, 'flush_robot_graph') else {'tick': None, 'robots': []}
            store_status = self._store.status(sim.run_id)
            record = build_global_record(sim, events, status=self._state, identity=self._identity,
                commit_seq=sim.knowledge_seq, durable_seq=store_status.get('durable_seq'),
                mirrored_seq=store_status.get('mirrored_seq'))
            received = receipt.get('robots', []) if receipt.get('tick') == record['tick'] else []
            record['step'] = {'tick': record['tick'], 'commit_seq': record.get('commit_seq', sim.knowledge_seq),
                              'expected_robot_count': len(sim.robots), 'received_robot_ids': received,
                              'complete': len(received) == len(sim.robots), 'source': 'ray-robot-actors'}
            self._global_client.submit(record)
        except Exception as exc:
            self._global_writer_error = type(exc).__name__ + ': ' + str(exc)

    def _record_global(self, sim, events):
        if getattr(sim, 'global_writer_url', None):
            # Stateful actors submit exactly once per tick, including between saves.
            # Reset cursors before _persist replaces pending_events.
            sim._global_cursor = sim._graph_cursor = 0
            return
        if getattr(self, 'global_writer_url', None):
            return self._record_global_http(sim, events)
        if not getattr(self, "global_writer_enabled", False):
            return
        try:
            self._ensure_global_writer()
            from backend.global_writer import build_global_record
            health = self._global_writer.health()
            store_status = self._store.status(sim.run_id)
            record = build_global_record(
                sim, events, status=self._state, identity=self._identity,
                commit_seq=sim.knowledge_seq,
                durable_seq=store_status.get('durable_seq'),
                mirrored_seq=store_status.get('mirrored_seq'),
                writer_errors=health.get('errors', 0),
                writer_dropped=health.get('dropped', 0))
            if not self._global_writer.record(record):
                self._global_writer_error = 'global writer queue full; intermediate stat dropped'
        except Exception as exc:
            # Global graph persistence is optional and must never turn a
            # durable simulation commit into a failed physical run.
            self._global_writer_error = type(exc).__name__ + ': ' + str(exc)

    def _persist(self):
        sim, store = self._sim, self._store
        start_ns = time.time_ns()
        # Observation only: per-phase wall time of this synchronous save, written
        # to persist_timing.jsonl. Order and semantics of the save are unchanged.
        phases, mark = {}, [time.perf_counter_ns()]

        def lap(name):
            now = time.perf_counter_ns()
            phases[name] = round((now - mark[0]) / 1e6, 3)
            mark[0] = now
        journal_before = getattr(store, 'timings', {}).get('journal_seconds', 0.)
        events_count = len(sim.pending_events)
        sim.knowledge_seq += 1
        checkpoint = sim.checkpoint()
        checkpoint['experiment'] = {'disturbance_sha256': digest(self._disturbances), 'applied': deepcopy(self._applied)}
        lap('checkpoint_build_ms')
        events = deepcopy(sim.pending_events)
        store.append(checkpoint, events)
        lap('store_append_ms')
        journal_ms = round((getattr(store, 'timings', {}).get('journal_seconds', 0.) - journal_before) * 1e3, 3)
        phases['of_which_journal_ms'] = journal_ms
        phases['of_which_graph_mirror_ms'] = round(phases['store_append_ms'] - journal_ms, 3)
        self._events.extend(events)
        self._record_global(sim, events)
        sim.pending_events = []
        self._core.persistence.write_checkpoint(self._output / 'checkpoint.json', checkpoint)
        lap('checkpoint_file_ms')
        self._last_commit = sim.elapsed
        status = store.status(sim.run_id)
        lap('store_status_ms')
        with (self._output / 'writer.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(canonical({'run_id': sim.run_id, 'seq': sim.knowledge_seq, 'sim_time': sim.elapsed,
                'started_ns': start_ns, 'observed_at_ns': time.time_ns(), **status}) + '\n')
        lap('writer_log_ms')
        if store.mirror and store.last_error:
            raise RuntimeError('graph mirror failed; durable boundary preserved: ' + str(store.last_error))
        if getattr(self,'_query_summary',None) is not None:
            if store.mirror is None:
                raise RuntimeError('query summary publisher requires its actual graph mirror')
            self._query_summary_status=self._query_summary.catch_up_from_graph(
                store.mirror,sim.run_id,sim.knowledge_seq,timeout=15)
        lap('query_summary_ms')
        self._write_persist_timing(sim, phases, events_count, start_ns)

    def _write_persist_timing(self, sim, phases, events_count, start_ns):
        loop = getattr(self, '_loop_stats', None) or {}
        row = {'run_id': sim.run_id, 'seq': sim.knowledge_seq, 'sim_time': sim.elapsed,
               'events': events_count, 'total_ms': round((time.time_ns() - start_ns) / 1e6, 3),
               **phases, 'since_last_save': loop}
        with (self._output / 'persist_timing.jsonl').open('a', encoding='utf-8') as stream:
            stream.write(canonical(row) + '\n')
        self._loop_stats = {}

    def _note_loop(self, name, began_ns):
        stats = getattr(self, '_loop_stats', None)
        if stats is None:
            stats = self._loop_stats = {}
        ms = (time.perf_counter_ns() - began_ns) / 1e6
        stats[name + '_count'] = stats.get(name + '_count', 0) + 1
        stats[name + '_sum_ms'] = round(stats.get(name + '_sum_ms', 0.) + ms, 3)
        stats[name + '_max_ms'] = round(max(stats.get(name + '_max_ms', 0.), ms), 3)

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
        began = time.perf_counter_ns()
        sim.step()
        self._note_loop('sim_step', began)
        if force_persist or sim.elapsed - self._last_commit >= self.commit_seconds:
            if not force_persist:
                # Let the browser animate this frame while the save runs. The
                # frame still reports the previous durable/mirrored seq, so
                # query fences stay on fully committed knowledge; the publish
                # after the save reports the new seq.
                self._publish_timed()
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
        self._publish_timed()

    def _publish_timed(self):
        began = time.perf_counter_ns()
        self._publish()
        self._note_loop('publish', began)

    def _publish(self):
        lap = time.perf_counter_ns()
        snap = self._sim.frame()
        snap['kgStateSource'] = self.kg_state_source
        self._note_loop('pub_frame', lap); lap = time.perf_counter_ns()
        if 'robots' in snap and hasattr(self._sim, 'jobs'):
            if not hasattr(self, '_motion_view'):
                self._motion_view = LiveMotionView()
            self._motion_view.annotate(snap['robots'], self._sim.jobs, self._sim.elapsed,
                tasks=getattr(self._sim, 'tasks', {}), slots=getattr(self._sim, 'slots', {}))
        self._note_loop('pub_motion', lap); lap = time.perf_counter_ns()
        stamp = int(time.time() * 1000)
        knowledge = self._store.status(self._sim.run_id)
        knowledge.update(committed_sim_time=self._last_commit, physical_sim_time=self._sim.elapsed)
        if getattr(self,'_query_summary',None) is not None:
            knowledge['query_summary']=deepcopy(self._query_summary_status)
            published=(self._query_summary_status or {}).get('published_seq',0)
            knowledge['query_summary_lag']=max(0,knowledge.get('mirrored_seq',0)-published)
        self._note_loop('pub_knowledge', lap); lap = time.perf_counter_ns()
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
                        'max_active_robots':getattr(self._sim.config,'max_active_robots',100),
                        'fleet_mode':getattr(self._sim.config,'fleet_mode','all-x3'),
                        'fleet_capabilities':self._sim.run_metadata.get('fleet_capabilities')})
        if self._sim.run_metadata.get('boundary_routing'):
            snap['metadata']['boundary_routing']=deepcopy(self._sim.run_metadata['boundary_routing'])
        snap['metadata']['mobility']=getattr(self,'mobility','legacy')
        snap['metadata']['robot_processes']=getattr(self,'robot_processes','off')
        snap['metadata']['kg_state_source']=self.kg_state_source
        snap['metadata']['robot_agents_enabled']=getattr(self,'robot_agents',False)
        snap['metadata']['legacy_a_graph']=self.legacy_a_graph
        fleet=getattr(self._store.mirror,'agent_fleet',None) if self._store.mirror else None
        if fleet is not None:
            pids=sorted(fleet.pids.values())
            snap['runtime']={'workerProcessCount':len(pids),'workerPids':pids,
                'coordinatorPid':os.getpid(),'coordination':'central-physics-independent-robot-http-io',
                'process_count_scope':'registered independent robot I/O processes; coordinator additional',
                'agent_io_failed':fleet.failed}
        if getattr(self, "global_writer_enabled", False):
            writer = self._global_writer.health() if self._global_writer is not None else {
                'enabled': True, 'processAlive': False, 'pid': None, 'errors': 0,
                'dropped': 0, 'lastTick': -1, 'queueSize': 0, 'processCount': 1}
            if self._global_writer_error:
                writer['error'] = self._global_writer_error
            snap['runtime']['globalWriter'] = writer
            if writer.get('processAlive') and writer.get('pid'):
                snap['runtime']['workerProcessCount'] = int(snap['runtime'].get('workerProcessCount', 1)) + 1
                snap['runtime']['workerPids'] = list(snap['runtime'].get('workerPids', [os.getpid()])) + [int(writer['pid'])]
                snap['runtime']['coordination'] = 'central-physics + plain-global-writer-process'
        point = None
        if getattr(self, 'query_point_path', None) is not None:
            seq = self._sim.knowledge_seq
            committed = (knowledge.get('durable_seq'), knowledge.get('mirrored_seq'),
                         (self._query_summary_status or {}).get('published_seq'))
            if (type(seq) is not int or seq < 1
                    or any(type(value) is not int or value != seq for value in committed)
                    or knowledge.get('last_error')):
                raise RuntimeError('live query point requires matching successful source, graph and summary commits')
            point = {'version': QUERY_POINT_VERSION, 'run_id': self._sim.run_id, 'seq': seq,
                     'runtime_instance_id': self.runtime_instance_id, 'source_sha256': self._source_sha}
        # latest.json is a viewing copy written every tick; recovery uses checkpoint.json
        # and the journal, so it is replaced atomically without a per-tick fsync.
        self._note_loop('pub_assemble', lap); lap = time.perf_counter_ns()
        self._core.persistence.write_checkpoint(self._output / 'latest.json', snap, durable=False)
        self._note_loop('pub_latest_file', lap)
        if point is not None and getattr(self, '_last_query_point', None) != (point['run_id'], point['seq']):
            self._core.persistence.write_checkpoint(self.query_point_path, point)
            self._last_query_point = (point['run_id'], point['seq'])
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
        close_sim = getattr(getattr(self, '_sim', None), 'close', None)
        if close_sim is not None:
            try:
                close_sim()
            except Exception as exc:
                self._global_writer_error = type(exc).__name__ + ': ' + str(exc)
                self._error = self._error or self._global_writer_error
                traceback.print_exc()
        if getattr(self,'_global_client',None) is not None:
            try:self._global_client.close(timeout=10)
            except Exception:pass
        try:
            if self._global_writer is not None:
                self._global_writer.close()
                self._global_writer = None
        except Exception:
            traceback.print_exc()
        try:
            if self._provider is not None:
                self._provider.close()
        finally:
            self._provider = None
            if self._store is not None:
                self._store.close()
                self._store = None
