from copy import deepcopy
from contextlib import closing
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import zlib

from backend.knowledge import digest


SPEC = importlib.util.spec_from_file_location('summary_sidecar',
    Path(__file__).resolve().parents[1] / 'ops/probe_catalog_summary_sidecar.py')
MOD = importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(MOD)
TEMP = Path(__file__).resolve().parents[1] / 'data/local-check'
TEMP.mkdir(parents=True, exist_ok=True)


class AccumulatorTests(unittest.TestCase):
    def test_deadline_crossing_without_task_delta(self):
        a = MOD.SummaryAccumulator({'R01-C1': 'A'})
        t = {'task_id': 'T1', 'slot_id': 'R01-C1', 'status': 'waiting', 'due_second': 10}
        c, _ = a.consume([t], 10, 1)
        self.assertEqual(c[0]['overdue'], 0)
        c, changed = a.consume([], 11, 2)
        self.assertEqual(c[0]['overdue'], 1)
        self.assertEqual(len(changed), 1)
        self.assertTrue(changed[0]['overdue'])
        c, _ = a.consume([], 12, 3)
        self.assertEqual(c[0]['overdue'], 1)

    def test_completed_and_stale_heap_entry(self):
        a = MOD.SummaryAccumulator({'R01-C1': 'A'})
        t = {'task_id': 'T1', 'slot_id': 'R01-C1', 'status': 'waiting', 'due_second': 10}
        a.consume([t], 0, 1)
        t.update(status='completed')
        a.consume([t], 5, 2)
        c, _ = a.consume([], 20, 3)
        self.assertEqual(c, [{'zone': 'A', 'status': 'completed', 'tasks': 1, 'overdue': 0}])

    def test_changed_deadline_and_group_transfer(self):
        a = MOD.SummaryAccumulator({'R01-C1': 'A', 'R02-C1': 'B'})
        t = {'task_id': 'T1', 'slot_id': 'R01-C1', 'status': 'waiting', 'due_second': 0}
        a.consume([t], 1, 1)
        t.update(slot_id='R02-C1', status='assigned', due_second=5)
        c, _ = a.consume([t], 2, 2)
        self.assertEqual(c, [{'zone': 'B', 'status': 'assigned', 'tasks': 1, 'overdue': 0}])
        c, _ = a.consume([], 6, 3)
        self.assertEqual(c[0]['overdue'], 1)

    def test_clock_regression_rejects(self):
        a = MOD.SummaryAccumulator({});a.consume([], 5, 1)
        with self.assertRaisesRegex(ValueError, 'regressed'):
            a.consume([], 4, 2)


class SidecarTests(unittest.TestCase):
    def build(self, folder):
        journal = folder / 'source.sqlite'
        batch = {'run_id': 'run1', 'seq': 1, 'sim_time': 0, 'completed': 0,
                 'metadata': {'scene': {'racks': [{'id': 'R01', 'zone': 'A'}]}},
                 'config': {'test': 1}, 'slots': [{'slot_id': 'R01-C1', 'rack_id': 'R01'}],
                 'tasks': [{'task_id': 'T1', 'slot_id': 'R01-C1', 'status': 'waiting', 'due_second': 10}]}
        with closing(sqlite3.connect(journal)) as db:
            db.executescript('CREATE TABLE heads(run_id TEXT,seq INTEGER); CREATE TABLE commits(run_id TEXT,seq INTEGER,checksum TEXT,batch BLOB);')
            db.execute('INSERT INTO heads VALUES (?,?)', ('run1', 1))
            db.execute('INSERT INTO commits VALUES (?,?,?,?)',
                       ('run1', 1, digest(batch), zlib.compress(MOD.canonical(batch).encode())))
            db.commit()
        side = folder / 'derived.sqlite'
        report = MOD.build_sidecar(journal, side, 'created1')
        fleet = {'token': (digest(batch), 'created1'), 'metadata': batch['metadata'],
                 'config': batch['config'], 'step': {'sim_time': 0, 'completed': 0}}
        return side, report, fleet

    def test_build_and_exact_read_without_oracle(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as f:
            side, report, fleet = self.build(Path(f))
            reader = MOD.SidecarReader(side, report['manifest_sha256'])
            result = reader.read('run1', 1, fleet)
            self.assertEqual(result['counts'], [{'zone': 'A', 'status': 'waiting', 'tasks': 1, 'overdue': 0}])
            self.assertFalse(report['generation_uses_oracle_or_verifier'])

    def test_run_recreation_rejected(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as f:
            side, report, fleet = self.build(Path(f))
            reader = MOD.SidecarReader(side, report['manifest_sha256'])
            fleet['token'] = (fleet['token'][0], 'created2')
            with self.assertRaisesRegex(ValueError, 'identity mismatch'):
                reader.read('run1', 1, fleet)

    def test_corrupt_counts_and_missing_cutoff_rejected(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as f:
            side, report, fleet = self.build(Path(f))
            reader = MOD.SidecarReader(side, report['manifest_sha256'])
            with self.assertRaisesRegex(ValueError, 'exact committed cutoff'):
                reader.read('run1', 2, fleet)
            with closing(sqlite3.connect(side)) as db:
                value = json.loads(db.execute('SELECT payload FROM summary').fetchone()[0])
                value['counts'][0]['tasks'] = 99
                db.execute('UPDATE summary SET payload=?', (MOD.canonical(value),))
                db.commit()
            with self.assertRaisesRegex(ValueError, 'checksum'):
                reader.read('run1', 1, fleet)

    def test_point_completion_mismatch_rejected(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as f:
            side, report, fleet = self.build(Path(f))
            fleet['step']['completed'] = 1
            with self.assertRaisesRegex(ValueError, 'exact-point mismatch'):
                MOD.SidecarReader(side, report['manifest_sha256']).read('run1', 1, fleet)

    def test_actual_graph_entity_budget_stays_authoritative(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as f:
            side, report, fleet = self.build(Path(f))
            service = MOD.SidecarCatalog(None, MOD.SidecarReader(side, report['manifest_sha256']))
            service._read = lambda q, p: [{'entity_count': 2}]
            service._point = lambda *args: None
            with self.assertRaisesRegex(ValueError, 'task entity budget exceeded'):
                service._tasks('run1', 1, fleet, 1)
            self.assertEqual(service._tasks('run1', 1, fleet, 2)[0]['tasks'], 1)


if __name__ == '__main__':
    unittest.main()
