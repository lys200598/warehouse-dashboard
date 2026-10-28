import copy
import unittest

from backend.knowledge import KnowledgeError
from backend.scheduling_v3.graph import V3Mirror, audit_helpers


class Result:
    def __init__(self, value=None):
        self.value = value

    def single(self):
        return self.value

    def consume(self):
        return None


class Transaction:
    """Captures real parent+child Cypher while emulating commit identity only."""
    def __init__(self):
        self.head, self.steps, self.calls = 0, {}, []

    def run(self, query, **parameters):
        self.calls.append((query, copy.deepcopy(parameters)))
        if "RETURN r.committed_seq AS seq" in query:
            return Result({"seq": self.head})
        if "RETURN s.checksum AS checksum" in query:
            return Result({"checksum": self.steps[parameters["key"]]} if parameters["key"] in self.steps else None)
        if "MERGE (s:WHStep" in query:
            self.steps.update({row["key"]: row["checksum"] for row in parameters["rows"]})
        if "SET r.committed_seq=$seq" in query:
            self.head = parameters["seq"]
        return Result()


def batch(seq=1, *, alias=False):
    event = {"event_id": "test-v3:event:1", "event_seq": 1, "tick": 10, "sim_time": 10,
             "kind": "cargo_handoff", "robot_id": 2, "task_id": "T1", "goods_id": "G1",
             "location_id": "ROBOT-2", "reason": "low_battery"}
    event.update({"donor_id": 1, "receiver_id": 2} if alias else {"donor_robot_id": 1, "receiver_robot_id": 2})
    return {"run_id": "test-v3", "seq": seq, "tick": 10, "sim_time": 10, "completed": 0,
            "metadata": {"domain": "warehouse-inbound-outbound-day1", "scene": {"homes": [], "docks": [], "stations": []}},
            "config": {"chargers": 0}, "robots": [{"robot_id": 1, "robot_type_key": "X3"},
                                                      {"robot_id": 2, "robot_type_key": "X3"}],
            "tasks": [], "slots": [], "racks": [], "events": [event]}


class MirrorTests(unittest.TestCase):
    def test_handoff_edges_are_run_scoped_and_parent_writes_only_one_custody(self):
        tx = Transaction()
        V3Mirror._apply(tx, [batch()])
        writes = [(query, parameters) for query, parameters in tx.calls if "MERGE (e)-[:DONOR]" in query]
        self.assertEqual(len(writes), 1)
        row = writes[0][1]["rows"][0]
        self.assertEqual(row["donor_key"], "test-v3:1")
        self.assertEqual(row["receiver_key"], "test-v3:2")
        self.assertEqual(row["task_key"], "test-v3:T1")
        self.assertEqual(row["cargo_key"], "test-v3:G1")
        self.assertIn("MERGE (e)-[:TRANSFERS]->(g)", writes[0][0])
        custody = [(query, parameters) for query, parameters in tx.calls if "MERGE (v:WHGoodsState" in query]
        self.assertEqual(len(custody), 1)
        self.assertEqual(len(custody[0][1]["rows"]), 1)
        self.assertEqual(custody[0][1]["rows"][0]["location_id"], "ROBOT-2")

    def test_participant_aliases_supported_but_conflicts_and_bad_custody_rejected(self):
        tx = Transaction()
        V3Mirror._apply(tx, [batch(alias=True)])
        self.assertEqual(tx.head, 1)
        for mutate in [lambda e: e.update(donor_id=3), lambda e: e.update(receiver_robot_id=1),
                       lambda e: e.update(location_id="ROBOT-1"), lambda e: e.update(donor_robot_id=101),
                       lambda e: e.update(goods_id=None)]:
            value = batch()
            mutate(value["events"][0])
            fresh = Transaction()
            with self.assertRaises(ValueError):
                V3Mirror._apply(fresh, [value])
            self.assertEqual(fresh.calls, [])

    def test_replay_checks_checksum_without_custody_or_assistance_rewrite(self):
        tx, value = Transaction(), batch()
        V3Mirror._apply(tx, [value])
        count = len(tx.calls)
        V3Mirror._apply(tx, [value])
        replay = tx.calls[count:]
        self.assertTrue(any("RETURN s.checksum AS checksum" in query for query, _ in replay))
        self.assertFalse(any("MERGE (e)-[:DONOR]" in query or "MERGE (v:WHGoodsState" in query for query, _ in replay))
        self.assertEqual(tx.head, 1)
        changed = copy.deepcopy(value)
        changed["events"][0]["reason"] = "different"
        with self.assertRaises(KnowledgeError):
            V3Mirror._apply(tx, [changed])

    def test_mixed_or_gapped_runs_rejected(self):
        a, b = batch(), batch(2)
        b["run_id"] = "another"
        with self.assertRaises(KnowledgeError):
            V3Mirror._apply(Transaction(), [a, b])
        with self.assertRaises(KnowledgeError):
            V3Mirror._apply(Transaction(), [batch(2)])

    def test_recovery_arrays_preserved_and_linked_without_new_schema(self):
        value = batch()
        value["events"] = [{"event_id": "test-v3:event:3", "event_seq": 3, "tick": 10, "sim_time": 10,
                            "kind": "traffic_retimed", "robot_id": 1, "task_id": None,
                            "affected_robot_ids": [2, 1, 2], "reason": "temporary_stop", "delay_seconds": 8}]
        tx = Transaction()
        V3Mirror._apply(tx, [value])
        writes = [(query, parameters) for query, parameters in tx.calls if "MERGE (e)-[:AFFECTS]" in query]
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][1]["run"], "test-v3")
        self.assertEqual(writes[0][1]["rows"][0]["affected_robot_ids"], [1, 2])
        self.assertEqual(writes[0][1]["rows"][0]["reason"], "temporary_stop")
        self.assertFalse(any("CREATE CONSTRAINT" in query or "DELETE" in query for query, _ in tx.calls))

    def test_default_affected_robot_and_bad_array(self):
        value = batch()
        value["events"] = [{"event_id": "test-v3:event:2", "event_seq": 2, "tick": 10, "sim_time": 10,
                            "kind": "robot_stopped", "robot_id": 2, "task_id": None, "reason": "fault"}]
        tx = Transaction()
        V3Mirror._apply(tx, [value])
        rows = next(p["rows"] for q, p in tx.calls if "MERGE (e)-[:AFFECTS]" in q)
        self.assertEqual(rows[0]["affected_robot_ids"], [2])
        for invalid in ["1,2", [True], [0]]:
            bad = copy.deepcopy(value)
            bad["events"][0]["affected_robot_ids"] = invalid
            with self.assertRaises(ValueError):
                V3Mirror.check_batches([bad])

    def test_runtime_affected_robots_alias_and_reroute_events_keep_full_group(self):
        for kind in ["traffic_retimed", "traffic_rerouted", "robot_stopped"]:
            value = batch()
            value["events"] = [{"event_id": "test-v3:event:7", "event_seq": 7, "tick": 20, "sim_time": 20,
                                "kind": kind, "robot_id": None, "task_id": None,
                                "affected_robots": [3, 1, 2, 3], "reason": "changed_actual_occupancy"}]
            tx = Transaction()
            V3Mirror._apply(tx, [value])
            rows = next(p["rows"] for q, p in tx.calls if "MERGE (e)-[:AFFECTS]" in q)
            self.assertEqual(rows[0]["affected_robot_ids"], [1, 2, 3])
            self.assertEqual(rows[0]["kind"], kind)
            for invalid in ["1,2", [101], [True]]:
                bad = copy.deepcopy(value)
                bad["events"][0]["affected_robots"] = invalid
                with self.assertRaises(ValueError):
                    V3Mirror.check_batches([bad])


class AuditTests(unittest.TestCase):
    def test_audit_uses_run_index_and_checks_complete_mapping_not_current_custody(self):
        class Mirror:
            def query(self, query, parameters):
                self.query_text, self.parameters = query, parameters
                return [{"event_id": "test-v3:event:1", "donor_robot_id": 1, "receiver_robot_id": 2,
                         "task_id": "T1", "goods_id": "G1", "donor_ids": [1], "receiver_ids": [2],
                         "task_ids": ["T1"], "goods_ids": ["G1"]}]
        mirror = Mirror()
        self.assertEqual(audit_helpers(mirror, "test-v3", expected_handoffs=1),
                         {"handoff_events": 1, "valid_linkages": True})
        self.assertIn("USING INDEX r:WHRun(run_id)", mirror.query_text)
        self.assertIn("(r)<-[:IN_RUN]-(e:WHEvent)", mirror.query_text)
        self.assertNotIn("current_location", mirror.query_text)
        self.assertEqual(mirror.parameters, {"run": "test-v3"})
        with self.assertRaises(AssertionError):
            audit_helpers(mirror, "test-v3", expected_handoffs=2)
        mirror.query = lambda *args: [{"event_id": "x", "donor_robot_id": 1, "receiver_robot_id": 2,
                                     "task_id": "T1", "goods_id": "G1", "donor_ids": [], "receiver_ids": [2],
                                     "task_ids": ["T1"], "goods_ids": ["G1"]}]
        with self.assertRaises(AssertionError):
            audit_helpers(mirror, "test-v3")

    def test_empty_run_has_zero_handoffs(self):
        mirror = type("Mirror", (), {"query": lambda *args: []})()
        self.assertEqual(audit_helpers(mirror, "empty", expected_handoffs=0)["handoff_events"], 0)


if __name__ == "__main__":
    unittest.main()
