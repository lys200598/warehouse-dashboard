"""Bounded real-time HTTP check on 14590, then compare the same memory boundary."""
import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import threading
import time
from urllib.request import Request, urlopen
import uuid

import uvicorn
from ops.warehouse_live import create_app
from ops.warehouse_live_runtime import WarehouseLiveRuntime
from tests import test_dispatch_golden as golden
from tests.test_robot_process_kg import audit, cleanup
from backend.checkpoint import read_checkpoint


def request(path, value=None):
    req = Request('http://127.0.0.1:14590' + path,
                  data=None if value is None else json.dumps(value).encode(),
                  headers={'Content-Type': 'application/json'})
    with urlopen(req, timeout=180) as response:
        return json.load(response)


def normalized(value, run_id):
    # Full checkpoint comparison: only identity/mode and measured wall times/PIDs differ.
    excluded = {'worker_pid', 'planning_seconds', 'dispatch_wall_seconds', 'recovery_wall_seconds',
                'query_seconds', 'kg_state_source'}
    if isinstance(value, dict):
        return {k: normalized(v, run_id) for k, v in value.items() if k not in excluded}
    if isinstance(value, list):
        return [normalized(v, run_id) for v in value]
    if isinstance(value, str):
        return value.replace(run_id, 'RUN')
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticks', type=int, default=400)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--speed', type=float, default=1.)
    parser.add_argument('--robot-context-interval', type=int, default=10)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    disturbance = args.output / 'disturbance.json'
    disturbance.write_text(json.dumps([{'at': 250, 'kind': 'soc', 'robot': 'carrying_moving', 'soc': 24,
                                       'selection_timeout': 600}]))
    registry = {'medium19': {'scene': str(golden.SCENE), 'dataset': str(golden.DATASET),
                            'config_json': str(golden.CONFIG), 'disturbances_json': str(disturbance)}}

    class BoundedRuntime(WarehouseLiveRuntime):
        def _advance_one(self, force_persist=False):
            super()._advance_one(force_persist=force_persist)
            if self._sim.tick >= args.ticks:
                self._control('pause', None, self._sim.run_id)

    checkpoints, reports = {}, {}
    for mode in ('neo4j', 'memory'):
        run_id = 'wz-test-live-' + mode + '-' + uuid.uuid4().hex[:8]
        runtime = BoundedRuntime(source_root=golden.ROOT, output_root=args.output / mode,
            datasets=registry, dataset_id='medium19', run_id=run_id,
            mobility='wheeled', robot_processes='ray', ray_address='127.0.0.1:27380', ray_actor_cpus=.02,
            robot_graph=True, global_writer_url='http://127.0.0.1:14591', kg_state_source=mode,
            robot_context_interval=args.robot_context_interval)
        server = uvicorn.Server(uvicorn.Config(create_app(runtime, registry, golden.ROOT / 'dist', own_runtime=False),
                                host='127.0.0.1', port=14590, log_level='warning'))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 20
            while not server.started and time.monotonic() < deadline:
                time.sleep(.1)
            initial = request('/api/snapshot')
            assert initial['kgStateSource'] == mode
            # Warm the 100 processes outside the measured real-time interval.
            request('/api/control', {'action': 'step'})
            if mode == 'memory':
                request('/api/control', {'action': 'speed', 'value': 120})
            elif args.speed != 1:
                request('/api/control', {'action': 'speed', 'value': args.speed})
            began = time.monotonic()
            request('/api/control', {'action': 'start'})
            last = -1
            while True:
                frame = request('/api/snapshot')
                if frame['status'] == 'failed':
                    raise RuntimeError(frame['lastError'])
                if frame['tick'] // 50 != last:
                    last = frame['tick'] // 50
                    print(f'[live {mode}] tick={frame["tick"]} status={frame["status"]}', flush=True)
                if frame['tick'] == args.ticks and frame['status'] == 'paused':
                    break
                if time.monotonic() - began > args.ticks * 3 + 180:
                    raise TimeoutError('bounded live run did not reach its requested boundary')
                time.sleep(.2)
            checkpoint_path = runtime._output / 'checkpoint.json'
            checkpoint = read_checkpoint(checkpoint_path)
            checkpoints[mode] = normalized(checkpoint, run_id)
            reports[mode] = {'tick': frame['tick'], 'wall_s': time.monotonic() - began,
                             'checkpoint': str(checkpoint_path), 'mode': frame['kgStateSource'],
                             'requested_speed': 120 if mode == 'memory' else args.speed}
            if mode == 'neo4j':
                reports[mode]['graph'] = audit(runtime._sim)
            else:
                runtime._sim._step_client.flush(15.)
        finally:
            runtime.shutdown()
            server.should_exit = True
            thread.join(15)
            cleanup(runtime._sim)
    if checkpoints['memory'] != checkpoints['neo4j']:
        differences = [k for k in checkpoints['memory'] if checkpoints['memory'].get(k) != checkpoints['neo4j'].get(k)]
        raise AssertionError(f'checkpoint differences: {differences}')
    reports['checkpoint_equal'] = True
    reports['normalized_sha256'] = hashlib.sha256(json.dumps(checkpoints['memory'], sort_keys=True).encode()).hexdigest()
    (args.output / 'result.json').write_text(json.dumps(reports, indent=2) + '\n')
    print('[live acceptance] ' + json.dumps(reports), flush=True)


if __name__ == '__main__':
    main()
