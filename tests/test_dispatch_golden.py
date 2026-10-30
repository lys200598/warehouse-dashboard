"""Dispatch result baseline for the wheeled medium sample.

Replaces the old contract that V5 dispatch must stay a line-by-line copy of V3.
What matters is behaviour: the same input must produce the same dispatch
decisions (which task, which robot, when) and the same completion times.
This guards the planned split of the main process (dispatch only) and robot
state moving into Ray actors.

When a change is meant to alter dispatch, regenerate the baseline with
    python -m tests.test_dispatch_golden --write
and explain the difference in the commit message.

Scope: ordinary dispatch and execution of 19 tasks. Low-battery relay is
triggered by live disturbances and is not covered here yet.
"""
import json
from pathlib import Path
import sys
import unittest

from backend.wheeled.sim import WheeledConfig, WheeledSimulation

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/dispatch-golden-medium19.json'
SCENE = ROOT / 'data/layouts/expansion-candidates/medium/layout.json'
DATASET = ROOT / 'data/examples/wheeled-medium-sample19'
CONFIG = ROOT / 'config/wheeled-medium-100.json'
MAX_TICKS = 4000


def record():
    config = WheeledConfig(**json.loads(CONFIG.read_text(encoding='utf-8')))
    sim = WheeledSimulation(SCENE, DATASET, 'dispatch-golden', config)
    # Release the first task at t=0; relative release times are unchanged.
    shift = min(task['release_second'] for task in sim.tasks.values())
    for task in sim.tasks.values():
        task['release_second'] -= shift
    seen, assignments = {}, []
    for tick in range(1, MAX_TICKS + 1):
        sim.step()
        for task_id, task in sorted(sim.tasks.items()):
            key = (task.get('robot_id'), task.get('assigned_at'))
            if task.get('robot_id') is not None and seen.get(task_id) != key:
                seen[task_id] = key
                assignments.append({'tick': tick, 'task_id': task_id, 'robot_id': task['robot_id'],
                                    'assigned_at': task.get('assigned_at')})
        if all(task['status'] == 'completed' for task in sim.tasks.values()):
            break
    return {'version': 'dispatch-golden-v1', 'scene': str(SCENE.relative_to(ROOT)),
            'dataset': str(DATASET.relative_to(ROOT)), 'config': str(CONFIG.relative_to(ROOT)),
            'release_shift_seconds': shift, 'ticks': tick, 'assignments': assignments,
            'completed_at': {task_id: task.get('completed_at') for task_id, task in sorted(sim.tasks.items())}}


class DispatchGoldenTests(unittest.TestCase):
    def test_same_input_gives_same_dispatch_and_completion(self):
        expected = json.loads(FIXTURE.read_text(encoding='utf-8'))
        actual = record()
        for index, (got, want) in enumerate(zip(actual['assignments'], expected['assignments'])):
            self.assertEqual(got, want, f'assignment #{index} is the first difference')
        self.assertEqual(len(actual['assignments']), len(expected['assignments']))
        self.assertEqual(actual['completed_at'], expected['completed_at'])
        self.assertEqual(actual['ticks'], expected['ticks'])


if __name__ == '__main__':
    if '--write' in sys.argv:
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE.write_text(json.dumps(record(), ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
        print('wrote', FIXTURE)
    else:
        unittest.main()
