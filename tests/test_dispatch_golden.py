"""Dispatch result baseline for the wheeled medium sample.

Replaces the old contract that V5 dispatch must stay a line-by-line copy of V3.
What matters is behaviour: the same input must produce the same dispatch
decisions (which task, which robot, when) and the same completion times.
This guards the planned split of the main process (dispatch only) and robot
state moving into Ray actors.

When a change is meant to alter dispatch, regenerate the baseline with
    python -m tests.test_dispatch_golden --write
and explain the difference in the commit message.

Two scenarios on the 19-task sample:
- ordinary dispatch and execution (releases shifted to t=0);
- low-battery relay: original release times; at t>=250 the lowest-id robot that
  is carrying goods to its target reports 24% battery, as the live sample
  disturbance does, so a cargo handoff to another robot must happen. Handoffs
  are pinned as (tick, task, goods, donor, receiver, reason).
"""
import json
from pathlib import Path
import sys
import unittest

from backend.wheeled.sim import WheeledConfig, WheeledSimulation

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/dispatch-golden-medium19.json'
RELAY_FIXTURE = ROOT / 'tests/fixtures/dispatch-golden-medium19-relay.json'
SCENE = ROOT / 'data/layouts/expansion-candidates/medium/layout.json'
DATASET = ROOT / 'data/examples/wheeled-medium-sample19'
CONFIG = ROOT / 'config/wheeled-medium-100.json'
MAX_TICKS = 4000


def record(sim_class=WheeledSimulation):
    config = WheeledConfig(**json.loads(CONFIG.read_text(encoding='utf-8')))
    sim = sim_class(SCENE, DATASET, 'dispatch-golden', config)
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


def record_relay(report_at=250, report_soc=24, sim_class=WheeledSimulation, pick=min, with_sim=False):
    config = WheeledConfig(**json.loads(CONFIG.read_text(encoding='utf-8')))
    sim = sim_class(SCENE, DATASET, 'dispatch-golden-relay', config)
    handoffs, original = [], sim.event

    def capture(kind, robot_id=None, task_id=None, **fields):
        if kind == 'cargo_handoff':
            handoffs.append({'tick': sim.elapsed, 'task_id': task_id, 'goods_id': fields.get('goods_id'),
                             'donor_id': fields.get('donor_id'), 'receiver_id': fields.get('receiver_id'),
                             'reason': fields.get('reason')})
        return original(kind, robot_id, task_id, **fields)
    sim.event = capture
    reported, seen, assignments = None, {}, []
    for tick in range(1, MAX_TICKS + 1):
        if reported is None and sim.elapsed >= report_at:
            owners = [i for i, r in sim.robots.items()
                      if r['carrying'] and r['stage'] == 'to_target' and i not in sim.assists]
            if owners:
                reported = {'tick': sim.elapsed, 'robot_id': pick(owners), 'soc': report_soc}
                sim.report_soc(pick(owners), report_soc)
        sim.step()
        for task_id, task in sorted(sim.tasks.items()):
            key = (task.get('robot_id'), task.get('assigned_at'))
            if task.get('robot_id') is not None and seen.get(task_id) != key:
                seen[task_id] = key
                assignments.append({'tick': tick, 'task_id': task_id, 'robot_id': task['robot_id'],
                                    'assigned_at': task.get('assigned_at')})
        if all(task['status'] == 'completed' for task in sim.tasks.values()):
            break
    result = {'version': 'dispatch-golden-relay-v1', 'scene': str(SCENE.relative_to(ROOT)),
            'dataset': str(DATASET.relative_to(ROOT)), 'config': str(CONFIG.relative_to(ROOT)),
            'disturbance': reported, 'ticks': tick, 'handoffs': handoffs, 'assignments': assignments,
            'completed_at': {task_id: task.get('completed_at') for task_id, task in sorted(sim.tasks.items())}}
    return (result, sim) if with_sim else result


class DispatchGoldenTests(unittest.TestCase):
    def test_same_input_gives_same_dispatch_and_completion(self):
        expected = json.loads(FIXTURE.read_text(encoding='utf-8'))
        actual = record()
        for index, (got, want) in enumerate(zip(actual['assignments'], expected['assignments'])):
            self.assertEqual(got, want, f'assignment #{index} is the first difference')
        self.assertEqual(len(actual['assignments']), len(expected['assignments']))
        self.assertEqual(actual['completed_at'], expected['completed_at'])
        self.assertEqual(actual['ticks'], expected['ticks'])

    def test_low_battery_relay_gives_same_handoff_dispatch_and_completion(self):
        expected = json.loads(RELAY_FIXTURE.read_text(encoding='utf-8'))
        actual = record_relay()
        self.assertGreaterEqual(len(expected['handoffs']), 1, 'the relay scenario must contain a handoff')
        self.assertEqual(actual['disturbance'], expected['disturbance'])
        self.assertEqual(actual['handoffs'], expected['handoffs'])
        for index, (got, want) in enumerate(zip(actual['assignments'], expected['assignments'])):
            self.assertEqual(got, want, f'assignment #{index} is the first difference')
        self.assertEqual(len(actual['assignments']), len(expected['assignments']))
        self.assertEqual(actual['completed_at'], expected['completed_at'])
        self.assertEqual(actual['ticks'], expected['ticks'])


if __name__ == '__main__':
    if '--write' in sys.argv:
        FIXTURE.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE.write_text(json.dumps(record(), ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
        RELAY_FIXTURE.write_text(json.dumps(record_relay(), ensure_ascii=False, indent=1) + chr(10), encoding="utf-8")
        print("wrote", FIXTURE, RELAY_FIXTURE)
    else:
        unittest.main()
