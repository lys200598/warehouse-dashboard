"""Real CLI journal/checkpoint resume and non-mutating cross-mode refusal."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.checkpoint import read_checkpoint
from backend.wide.scenarios import generate_scenario
from tests.test_scheduler_v4 import physical
from backend.scheduling_v4.sim import Simulation, Config


def main():
    root = ROOT / 'data/recovery-v4'
    root.mkdir(exist_ok=False)
    scene = ROOT / 'data/layouts/warehouse100-wide-v2.json'
    generate_scenario(scene, root/'input', duration_seconds=1800,
                      slot_interval_seconds=3600, active_slots=64)
    events = root/'disturbances.json'
    events.write_text('[{"at":300,"kind":"stop","seconds":90,"handoff":true}]')
    commands = []

    def invoke(folder, mode='optimized', resume=False, window=False):
        command = [sys.executable, '-m', 'backend.scheduling_v4.runner', '--scene', str(scene),
                   '--dataset', str(root/'input'), '--output', str(root/folder),
                   '--run-id', 'v4-cli-recovery', '--scheduler', mode,
                   '--disturbances-json', str(events), '--until', '301' if window else '1800',
                   '--max-wall-seconds', '180', '--frames']
        if resume: command.append('--resume')
        if window: command.append('--window')
        with (root/f'{len(commands)}-{folder}.log').open('wb') as log:
            result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=200)
        commands.append({'argv': command, 'returncode': result.returncode})
        return result.returncode

    assert invoke('continuous') == 0
    assert invoke('resumed', window=True) == 0
    state = read_checkpoint(root/'resumed/checkpoint.json')
    assert state['elapsed'] == 301 and state['scheduler_v3']['assists'], 'must stop DURING handoff'
    before = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in (root/'resumed').iterdir() if p.is_file()}
    assert invoke('resumed', mode='baseline', resume=True) != 0
    after = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
             for p in (root/'resumed').iterdir() if p.is_file()}
    assert before == after, 'cross-mode preflight must not write journal or checkpoint'
    assert invoke('resumed', resume=True) == 0
    sims = [Simulation(scene, root/'input', 'v4-cli-recovery', Config(),
                       checkpoint=read_checkpoint(root/folder/'checkpoint.json'))
            for folder in ['continuous', 'resumed']]
    assert physical(sims[0]) == physical(sims[1]), 'resumed business and physical state must match'
    assert sims[0].planner.expansions == sims[1].planner.expansions
    report = {'status': 'passed', 'stopped_during_handoff': True,
              'wrong_mode_refusal_left_every_output_byte_unchanged': True,
              'resumed_physical_state_matches_continuous': True,
              'search_counters_match': True,
              'completed': [s.metrics['completed'] for s in sims], 'commands': commands}
    (root/'verification.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == '__main__':
    main()
