"""Fake-driver contract tests; no Neo4j, Ray or simulation is started."""
from copy import deepcopy
import json
import unittest

from backend.robot_actor import graph_writer as gw


class Result:
    def __init__(self, records=()):
        self.records = list(records)

    def single(self):
        return self.records[0] if self.records else None

    def consume(self):
        return None

    def __iter__(self):
        return iter(self.records)


class Transaction:
    """Store all writes privately until commit, including deliberately failed ones."""

    def __init__(self, driver):
        self.driver = driver
        self.data = deepcopy(driver.data)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def run(self, query, **p):
        d = self.data
        self.driver.calls.append((query, deepcopy(p)))
        if query == gw._RUN:
            return Result([{"id": p["run_id"]}] if p["run_id"] in d["runs"] else [])
        if query == gw._LOCK_ROBOT:
            robot = d["robots"].setdefault(p["robot_id"], {"updated_tick": -1})
            return Result([{"tick": robot["updated_tick"],
                            "checksum": robot.get("graph_writer_payload_sha256")}])
        if query == gw._WRITE_CURRENT:
            d["robots"][p["robot"]["id"]].update(deepcopy(p["robot"]))
        elif query == gw._WRITE_HISTORY:
            state = d["states"].setdefault(p["state"]["id"], deepcopy(p["state"]))
            return Result([{"checksum": state["payload_sha256"]}])
        elif query == gw._WRITE_EVENTS:
            records = []
            for row in p["events"]:
                event = d["events"].setdefault(row["id"], deepcopy(row))
                records.append({"id": row["id"], "checksum": event["payload_sha256"]})
            if self.driver.fail_events:
                self.driver.fail_events -= 1
                raise OSError("connection lost after event writes, before commit")
            return Result(records)
        elif query == gw._LINK_EXECUTIONS:
            d["tasks"].update(p["task_ids"])
        elif query == gw._LINK_EVENT_EXECUTIONS:
            d["event_tasks"].update((row["id"], row["task_id"]) for row in p["events"])
        else:
            raise AssertionError(f"unexpected query: {query}")
        return Result()

    def commit(self):
        self.driver.data = self.data
        self.committed = True
        self.driver.commits += 1
        if self.driver.lose_ack:
            self.driver.lose_ack -= 1
            raise OSError("commit succeeded but acknowledgement was lost")


class Session:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        if self.driver.fail_connect:
            self.driver.fail_connect -= 1
            raise OSError("unavailable")
        return self

    def __exit__(self, *_):
        return False

    def begin_transaction(self):
        return Transaction(self.driver)


class Driver:
    def __init__(self):
        self.data = {"runs": {"run-1", "run-2"}, "robots": {}, "states": {},
                     "events": {}, "tasks": set(), "event_tasks": set()}
        self.calls = []
        self.commits = self.fail_connect = self.fail_events = self.lose_ack = 0

    def session(self, **_):
        return Session(self)


def robot(tick=1):
    return {"id": 6, "type": {"key": "X3"}, "stage": "to_target",
            "x": float(tick), "y": 2.0, "battery": 80.0 - tick,
            "taskId": "T-12", "cargoId": "G-1"}


def handoff(tick=4):
    return {"event_seq": 92, "kind": "cargo_handoff", "tick": tick,
            "robot_id": 6, "task_id": "T-12", "goods_id": "G-1",
            "location_id": "ROBOT-6", "donor_id": 5, "receiver_id": 6,
            "reason": "measured_low_energy", "requested_at": 2,
            "location": {"x": 4.0, "y": 2.0},
            "extra": {"details": ["retain", "everything"]}}


class RobotGraphWriterTests(unittest.TestCase):
    def setUp(self):
        self.driver = Driver()
        self.writer = self.make_writer()

    def make_writer(self, run_id="run-1"):
        return gw.RobotGraphWriter(driver=self.driver, run_id=run_id, robot_id=6)

    def test_current_every_step_history_every_four_steps(self):
        for tick in range(1, 10):
            self.writer.record(tick, robot(tick))
            current = self.driver.data["robots"]["run-1:robot:6"]
            self.assertEqual(current["updated_tick"], tick)
            self.assertEqual(current["x"], float(tick))
            self.assertEqual(current["battery"], 80.0 - tick)
        self.assertEqual(set(self.driver.data["states"]),
                         {"run-1:robot:6:state:4", "run-1:robot:6:state:8"})
        self.assertEqual(self.driver.data["states"]["run-1:robot:6:state:4"]["x"], 4.0)
        self.assertEqual(self.writer.last_confirmed_tick, 9)

    def test_complete_handoff_on_unsampled_step_and_history_execution_links(self):
        event = handoff(3)
        self.writer.record(3, robot(3), [event])
        stored = self.driver.data["events"]["run-1:event:v5:92"]
        self.assertEqual(json.loads(stored["payload_json"]), event)
        for key in ("donor_id", "receiver_id", "reason", "requested_at"):
            self.assertEqual(stored[key], event[key])
        self.assertEqual(stored["cargo_id"], "G-1")
        self.assertEqual(stored["slot_id"], "ROBOT-6")
        self.assertEqual(self.driver.data["states"], {})
        self.assertIn(("run-1:event:v5:92", "T-12"), self.driver.data["event_tasks"])
        self.assertEqual(self.driver.data["tasks"], {"T-12"})

    def test_failed_connect_is_observable_and_retains_all_batches_in_order(self):
        self.driver.fail_connect = 2
        with self.assertRaises(gw.RuntimeGraphUnavailable):
            self.writer.record(4, robot(4), [handoff()])
        self.writer.enqueue(5, robot(5))
        with self.assertRaises(gw.RuntimeGraphUnavailable):
            self.writer.flush()
        self.assertEqual(self.writer.pending_count, 2)
        self.assertIsNone(self.writer.last_confirmed_tick)
        self.assertEqual(self.writer.failure_count, 2)
        self.assertIsInstance(self.writer.last_error, OSError)
        self.assertEqual(self.writer.flush(), 5)
        ticks = [p["robot"]["updated_tick"] for q, p in self.driver.calls if q == gw._WRITE_CURRENT]
        self.assertEqual(ticks, [4, 5])
        self.assertEqual(len(self.driver.data["events"]), 1)
        self.assertEqual(self.writer.pending_count, 0)
        self.assertIsNone(self.writer.last_error)

    def test_partial_batch_failure_rolls_back_current_history_events_and_links(self):
        self.driver.fail_events = 1
        before = deepcopy(self.driver.data)
        with self.assertRaises(gw.RuntimeGraphUnavailable) as caught:
            self.writer.record(4, robot(4), [handoff()])
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertEqual(self.driver.data, before)
        self.assertEqual(self.writer.pending_count, 1)
        self.writer.flush()
        self.assertEqual(len(self.driver.data["events"]), 1)
        self.assertEqual(len(self.driver.data["states"]), 1)
        self.assertEqual(self.writer.last_confirmed_tick, 4)

    def test_committed_but_unconfirmed_batch_retries_identically_without_duplicates(self):
        self.driver.lose_ack = 1
        with self.assertRaises(gw.RuntimeGraphUnavailable):
            self.writer.record(4, robot(4), [handoff()])
        committed = deepcopy(self.driver.data)
        self.assertEqual(self.writer.pending_count, 1)
        self.assertIsNone(self.writer.last_confirmed_tick)
        self.writer.record(4, robot(4), [handoff()])
        self.assertEqual(self.driver.data, committed)
        self.assertEqual(self.writer.pending_count, 0)
        self.assertEqual(self.driver.commits, 2)
        self.writer.record(4, robot(4), [handoff()])
        self.assertEqual(self.driver.commits, 2)

    def test_queued_input_is_a_deep_copy(self):
        state, event = robot(4), handoff()
        self.writer.enqueue(4, state, [event])
        state["x"] = -999
        event["location"]["x"] = -999
        self.writer.flush()
        self.assertEqual(self.driver.data["robots"]["run-1:robot:6"]["x"], 4.0)
        stored = self.driver.data["events"]["run-1:event:v5:92"]
        self.assertEqual(json.loads(stored["payload_json"])["location"]["x"], 4.0)

    def test_fresh_writer_replay_is_idempotent_and_old_batch_cannot_regress_current(self):
        self.writer.record(4, robot(4), [handoff()])
        self.writer.record(5, robot(5))
        self.make_writer().record(4, robot(4), [handoff()])
        self.assertEqual(self.driver.data["robots"]["run-1:robot:6"]["updated_tick"], 5)
        self.assertEqual(len(self.driver.data["events"]), 1)
        self.assertEqual(len(self.driver.data["states"]), 1)

    def test_persisted_same_tick_different_payload_fails_without_overwriting(self):
        self.writer.record(4, robot(4), [handoff()])
        before = deepcopy(self.driver.data)
        state = robot(4)
        state["x"] = 999
        other = self.make_writer()
        with self.assertRaises(gw.RuntimeGraphUnavailable):
            other.record(4, state, [handoff()])
        self.assertEqual(self.driver.data, before)
        self.assertEqual(other.pending_count, 1)
        self.assertIsInstance(other.last_error, ValueError)

    def test_conflicting_event_rolls_back_even_new_current_state(self):
        self.writer.record(4, robot(4), [handoff()])
        before = deepcopy(self.driver.data)
        event = handoff()
        event["reason"] = "changed"
        with self.assertRaises(gw.RuntimeGraphUnavailable):
            self.writer.record(5, robot(5), [event])
        self.assertEqual(self.driver.data, before)
        self.assertEqual(self.writer.last_confirmed_tick, 4)
        self.assertEqual(self.writer.pending_count, 1)

    def test_business_ids_are_scoped_by_run_and_two_events_same_tick_survive(self):
        event = {**handoff(), "event_seq": 93, "kind": "robot_power_isolated"}
        self.writer.record(4, robot(4), [handoff(), event, handoff()])
        self.make_writer("run-2").record(4, robot(4), [handoff()])
        self.assertEqual(len(self.driver.data["events"]), 3)
        self.assertEqual(len(self.driver.data["states"]), 2)

    def test_old_history_conflict_is_rejected_without_regressing_new_current(self):
        self.writer.record(4, robot(4))
        self.writer.record(5, robot(5))
        before = deepcopy(self.driver.data)
        state = {**robot(4), "battery": 1.0}
        replay = self.make_writer()
        with self.assertRaises(gw.RuntimeGraphUnavailable):
            replay.record(4, state)
        self.assertEqual(self.driver.data, before)
        self.assertEqual(replay.pending_count, 1)

    def test_unchanged_event_can_appear_in_a_later_batch_without_duplication(self):
        self.writer.record(4, robot(4), [handoff()])
        self.writer.record(5, robot(5), [handoff()])
        self.assertEqual(len(self.driver.data["events"]), 1)
        self.assertEqual(self.driver.data["robots"]["run-1:robot:6"]["updated_tick"], 5)

    def test_raw_local_robot_and_mapped_events_are_supported(self):
        raw = {"robot_id": 6, "robot_type_key": "X3", "stage": "placing",
               "task_id": "T-12", "cargo_id": "G-1", "x": 1, "y": 2, "battery": 80}
        event = {"id": "stable-12", "type": "task_completed", "tick": 1,
                 "robot_id": 6, "task_id": "T-12"}
        self.writer.record(1, raw, [event])
        current = self.driver.data["robots"]["run-1:robot:6"]
        self.assertEqual(current["stage"], "placing")
        self.assertEqual(json.loads(current["payload_json"]), raw)
        self.assertIn("run-1:event:stable-12", self.driver.data["events"])

    def test_missing_run_does_not_silently_confirm_or_create_orphans(self):
        writer = self.make_writer("missing")
        with self.assertRaises(gw.RuntimeGraphUnavailable):
            writer.record(1, robot())
        self.assertEqual(writer.pending_count, 1)
        self.assertIsNone(writer.last_confirmed_tick)
        self.assertEqual(self.driver.data["robots"], {})

    def test_invalid_or_conflicting_submission_never_discards_pending(self):
        self.writer.enqueue(4, robot(4), [handoff()])
        invalid = [
            (3, robot(3), []), (4, robot(9), [handoff()]),
            (5, {**robot(5), "id": 99}, []),
            (5, robot(5), [{"kind": "no_stable_id"}]),
            (5, robot(5), [{"event_seq": True}]),
            (5, robot(5), [handoff(), {**handoff(), "reason": "conflicting"}]),
            (5, robot(5), [{**handoff(), "tick": 6}]),
            (True, robot(), []),
        ]
        for args in invalid:
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.writer.enqueue(*args)
            self.assertEqual(self.writer.pending_count, 1)
        self.writer.flush()
        self.assertEqual(self.writer.last_confirmed_tick, 4)


if __name__ == "__main__":
    unittest.main()
