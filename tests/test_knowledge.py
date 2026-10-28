from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
import zlib

from backend.checkpoint import read_checkpoint, write_checkpoint
from backend.knowledge import JournalKnowledgeStore, KnowledgeError
from backend.runtime import LocalRobotRuntime
from backend.simulation import SimulationConfig, SimulationCoordinator


class Mirror:
    def __init__(self):
        self.fail = False
        self.rows = []
    def apply_many(self, rows):
        if self.fail:
            raise RuntimeError("simulated unavailable database")
        self.rows.extend(rows)
    def close(self):
        pass


class KnowledgeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.mirror = Mirror()
        self.store = JournalKnowledgeStore(Path(self.temp.name)/"journal.sqlite", self.mirror)
        self.sim = SimulationCoordinator(LocalRobotRuntime(), SimulationConfig(robot_count=3), knowledge_store=self.store)
    def tearDown(self):
        self.sim.close()
        self.store.close()
        self.temp.cleanup()

    def test_committed_states_match_actual_runtime(self):
        self.sim.step_once()
        batch = self.mirror.rows[-1]
        self.assertEqual(batch["tick"], self.sim.tick)
        self.assertEqual(batch["robots"], [p["state"] for p in self.sim.runtime.snapshots()])
        self.assertEqual(self.store.status(self.sim.run_id)["lag"], 0)

    def test_full_event_archive_not_ui_ring(self):
        for index in range(160):
            self.sim.record_system_event(str(index))
        self.sim.persist_knowledge()
        all_events = [e for b in self.mirror.rows for e in b["events"]]
        self.assertGreater(len(all_events), 160)
        self.assertEqual(len(self.sim.events), 100)
        self.assertEqual(len({e["event_id"] for e in all_events}), len(all_events))

    def test_same_tick_controls_have_unique_sequence(self):
        self.sim.control("pause")
        self.sim.control("resume")
        self.assertEqual(self.sim.tick, 0)
        self.assertEqual(self.sim.knowledge_seq, 3)
        self.assertEqual(len({b["seq"] for b in self.mirror.rows}), 3)

    def test_graph_failure_keeps_durable_checkpoint_and_replays(self):
        self.mirror.fail = True
        self.sim.step_once()
        self.sim.step_once()
        status = self.store.status(self.sim.run_id)
        self.assertEqual(status["lag"], 2)
        self.assertIsNotNone(status["last_error"])
        self.assertEqual(self.store.latest_checkpoint(self.sim.run_id)["tick"], 2)
        self.mirror.fail = False
        self.store.flush(self.sim.run_id)
        self.assertEqual(self.store.status(self.sim.run_id)["lag"], 0)
        self.assertIsNone(self.store.last_error)

    def test_recovery_uses_newer_durable_checkpoint(self):
        older = deepcopy(self.sim.checkpoint_payload())
        self.sim.step_once()
        recovered = self.store.recovery_payload(older)
        self.assertEqual(recovered["tick"], 1)
        restored = SimulationCoordinator(LocalRobotRuntime(), self.sim.config, initialize=False)
        try:
            restored.restore_checkpoint(recovered)
            a, b = self.sim.step_once(), restored.step_once()
            self.assertEqual(a["robots"], b["robots"])
            self.assertEqual(a["racks"], b["racks"])
        finally:
            restored.close()

    def test_conflicting_or_missing_sequence_rejected(self):
        payload = self.sim.checkpoint_payload()
        self.store.append(payload, [])  # Idempotent identical replay.
        bad = deepcopy(payload)
        bad["elapsed"] += 1
        with self.assertRaises(KnowledgeError):
            self.store.append(bad, [])
        bad["knowledge_seq"] += 2
        with self.assertRaises(KnowledgeError):
            self.store.append(bad, [])

    def test_reset_isolates_runs(self):
        previous = self.sim.run_id
        self.sim.control("reset")
        self.assertNotEqual(previous, self.sim.run_id)
        self.assertEqual(self.store.latest_checkpoint(previous)["tick"], 0)
        self.assertEqual(self.store.latest_checkpoint(self.sim.run_id)["knowledge_seq"], 1)

    def test_checkpoint_hash_and_legacy(self):
        path = Path(self.temp.name)/"checkpoint.json"
        payload = self.sim.checkpoint_payload()
        write_checkpoint(path, payload)
        self.assertEqual(read_checkpoint(path), payload)
        corrupted = json.loads(path.read_text(encoding="utf-8"))
        corrupted["payload"]["tick"] += 1
        path.write_text(json.dumps(corrupted), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "SHA256"):
            read_checkpoint(path)
        path.write_text(json.dumps(payload), encoding="utf-8")
        self.assertEqual(read_checkpoint(path), payload)

    def test_recovery_refuses_journal_missing_or_behind(self):
        payload = self.sim.checkpoint_payload()
        payload["knowledge_seq"] += 1
        with self.assertRaises(KnowledgeError):
            self.store.recovery_payload(payload)
        payload["run_id"] = "missing-run"
        with self.assertRaises(KnowledgeError):
            self.store.recovery_payload(payload)

    def test_legitimate_flow_change_recovers_from_newer_journal(self):
        older = deepcopy(self.sim.checkpoint_payload())
        self.sim.control("set_flow_rate", 80)
        self.assertEqual(self.store.recovery_payload(older)["config"]["flow_rate"],80)

    def test_journal_checksum_checked(self):
        with self.store.db:
            self.store.db.execute("UPDATE heads SET checkpoint=? WHERE run_id=?", (zlib.compress(b'{}'), self.sim.run_id))
        with self.assertRaisesRegex(KnowledgeError, "checksum"):
            self.store.latest_checkpoint(self.sim.run_id)

    def test_missing_read_model_can_replay_retained_journal(self):
        self.sim.step_once()
        self.mirror.committed_point = lambda run: None
        count = len(self.mirror.rows)
        self.store.flush(self.sim.run_id)
        self.assertGreater(len(self.mirror.rows),count)
        self.assertEqual(self.sim.knowledge_status()["lag"],0)


if __name__ == "__main__":
    unittest.main()
