import copy
import json
import math
import unittest

from backend.scheduling_v3.energy import (
    EnergyProfile, HandoffError, assess_admission, required_soc,
    transfer_custody, validate_handoff,
)


def span(start, end, phase, **extra):
    return {"start": start, "end": end, "phase": phase, **extra}


class EnergyProfileTests(unittest.TestCase):
    def test_legacy_reproduces_prior_active_and_idle_model(self):
        p = EnergyProfile.legacy()
        self.assertAlmostEqual(p.rate("to_target", True), 60 / 14400)
        self.assertAlmostEqual(p.consume([span(0, 3600, "to_source")]), 15)
        self.assertAlmostEqual(p.consume([span(0, 3600, "traffic_wait")]), .9)
        self.assertEqual(p.charge_seconds(20, 80), 3600)
        self.assertFalse(p.hardware_calibrated)

    def test_loaded_markers_and_explicit_override(self):
        p = EnergyProfile()
        parts = [span(0, 10, "to_source"), span(10, 20, "picking", marker="picked"),
                 span(20, 30, "to_target"), span(30, 40, "placing", marker="delivered"),
                 span(40, 50, "returning")]
        expected = p.empty_soc_per_second * (10 + 12 + 12 + 12 + 10)
        self.assertAlmostEqual(p.consume(parts), expected)
        self.assertAlmostEqual(p.consume([span(0, 10, "to_target", loaded=True)]), p.empty_soc_per_second * 12)

    def test_charging_never_offsets_mission_energy(self):
        p = EnergyProfile()
        parts = [span(0, 100, "to_source"), span(100, 3700, "charging")]
        self.assertAlmostEqual(p.consume(parts), 100 * p.empty_soc_per_second)

    def test_taper_and_gain_inverse_to_integer_time(self):
        p = EnergyProfile()
        self.assertEqual(p.charge_seconds(20, 80), 3600)
        self.assertEqual(p.charge_seconds(80, 90), 1200)
        self.assertAlmostEqual(p.charge_gain(75, 900), 10)
        for start, target in [(0, 20), (20, 80), (79.99, 80.01), (79, 99), (99, 100)]:
            seconds = p.charge_seconds(start, target)
            self.assertGreaterEqual(start + p.charge_gain(start, seconds), target - 1e-9)
            if seconds:
                self.assertLess(start + p.charge_gain(start, seconds - 1), target + 1e-9)
        self.assertEqual(p.charge_seconds(90, 80), 0)
        self.assertEqual(p.charge_gain(100, 300), 0)

    def test_rejects_unknown_phase_negative_or_unaccounted_time(self):
        p = EnergyProfile()
        bad = [[span(0, 0, "idle")], [span(-1, 1, "idle")], [span(0, math.inf, "idle")],
               [span(0, 1, "unmodelled")], [span(0, 1, "idle"), span(2, 3, "idle")]]
        for parts in bad:
            with self.assertRaises(ValueError):
                p.consume(parts)
        for args in [{"charge_soc_per_second": 0}, {"loaded_factor": -1}, {"taper_factor": 2}, {"taper_start_soc": 101}]:
            with self.assertRaises(ValueError):
                EnergyProfile(**args)
        for start, target in [(101, 80), (20, -1), (math.nan, 80)]:
            with self.assertRaises(ValueError):
                p.charge_seconds(start, target)

    def test_conservative_profile_really_increases_drain_and_charge_time(self):
        base, stress = EnergyProfile(), EnergyProfile.conservative()
        for phase in ["to_source", "to_target", "picking", "placing", "traffic_wait"]:
            self.assertGreater(stress.rate(phase, True), base.rate(phase, True))
        self.assertGreater(stress.charge_seconds(20, 80), base.charge_seconds(20, 80))


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.profile = EnergyProfile()
        self.mission = [span(0, 300, "to_source"), span(300, 310, "picking", marker="picked"),
                        span(310, 610, "to_target"), span(610, 622, "placing", marker="delivered")]
        self.escape = [span(622, 922, "to_charge")]

    def test_actual_task_escape_queue_and_reserve_are_all_accounted(self):
        p = self.profile
        need = p.consume(self.mission) + p.consume(self.escape) + 200 * p.idle_soc_per_second + 20
        self.assertAlmostEqual(required_soc(self.mission, self.escape, p, 20, 200), need)
        options = [{"dock_id": "C1", "parts": self.escape, "queue_seconds": 200}]
        yes = assess_admission(need, self.mission, options, p)
        no = assess_admission(need - .01, self.mission, options, p)
        self.assertTrue(yes.admitted)
        self.assertFalse(no.admitted)
        self.assertAlmostEqual(yes.remaining_soc, 20)
        self.assertEqual(no.reason, "insufficient_energy")

    def test_available_at_wait_not_double_counted(self):
        a = assess_admission(50, self.mission, [{"dock_id": "C1", "parts": self.escape, "available_at": 1022}], self.profile)
        waited = self.escape + [span(922, 1022, "traffic_wait")]
        b = assess_admission(50, self.mission, [{"dock_id": "C1", "parts": waited, "available_at": 1022}], self.profile)
        self.assertAlmostEqual(a.required_soc, b.required_soc)
        self.assertGreater(a.queue_soc, 0)
        self.assertEqual(b.queue_soc, 0)

    def test_excessive_queue_or_no_reachable_station_is_rejected(self):
        missing = assess_admission(80, self.mission, [], self.profile)
        self.assertEqual(missing.reason, "no_reachable_charger")
        self.assertIsNone(missing.as_dict()["required_soc"])
        json.dumps(missing.as_dict(), allow_nan=False)
        self.assertFalse(assess_admission(80, self.mission, [{"dock_id": "bad", "reachable": False}], self.profile).admitted)
        waiting = self.escape + [span(922, 1922, "traffic_wait")]
        result = assess_admission(80, self.mission, [{"dock_id": "C1", "parts": waiting}], self.profile, max_queue_seconds=500)
        self.assertFalse(result.admitted)
        self.assertEqual(result.reason, "queue_bound_exceeded")

    def test_options_are_compared_by_required_energy_and_ties_deterministic(self):
        options = [{"dock_id": "far", "parts": [span(622, 1222, "to_charge")]},
                   {"dock_id": "near", "parts": self.escape}, {"dock_id": "A", "parts": self.escape}]
        self.assertEqual(assess_admission(70, self.mission, options, self.profile).dock_id, "A")
        with self.assertRaises(ValueError):
            assess_admission(70, self.mission, [{"dock_id": "bad", "parts": [span(700, 800, "to_charge")]}], self.profile)


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.donor = {"robot_id": 1, "carrying": True, "cargo_id": "G1", "task_id": "T1", "stage": "handoff_give", "x": 0, "y": 0}
        self.receiver = {"robot_id": 2, "carrying": False, "cargo_id": None, "task_id": None, "stage": "handoff_receive", "x": 1.2, "y": 0}
        self.task = {"task_id": "T1", "goods_id": "G1", "cargo_id": "G1", "robot_id": 1, "status": "carrying"}
        self.goods = {"G1": "ROBOT-1"}

    def test_transfer_is_atomic_copied_unique_and_not_duplicate_completion(self):
        before = copy.deepcopy((self.donor, self.receiver, self.task, self.goods))
        d, r, t, g, event = transfer_custody(*before, now=30)
        self.assertEqual((self.donor, self.receiver, self.task, self.goods), before)
        self.assertFalse(d["carrying"])
        self.assertTrue(r["carrying"])
        self.assertEqual(g, {"G1": "ROBOT-2"})
        self.assertEqual(t["status"], "carrying")
        self.assertNotIn("completed_at", t)
        self.assertEqual(event["kind"], "cargo_handoff")
        self.assertEqual(event["from_location"], "ROBOT-1")
        with self.assertRaises(HandoffError):
            transfer_custody(d, r, t, g, now=31)

    def test_collision_out_of_reach_wrong_owner_and_busy_receiver_rejected(self):
        for changed in [{"x": 0.5}, {"x": 2}, {"carrying": True, "cargo_id": "G2"},
                        {"task_id": "T2"}, {"stage": "charging"}, {"x": math.nan}]:
            receiver = dict(self.receiver, **changed)
            with self.assertRaises(HandoffError):
                validate_handoff(self.donor, receiver, self.task, self.goods)
        for changed in [{"robot_id": 2}, {"status": "completed"}, {"goods_id": "G2"}]:
            with self.assertRaises(HandoffError):
                validate_handoff(self.donor, self.receiver, dict(self.task, **changed), self.goods)
        with self.assertRaises(HandoffError):
            validate_handoff(self.donor, self.receiver, self.task, {"G1": "ROBOT-2"})
        with self.assertRaises(HandoffError):
            validate_handoff(self.donor, self.receiver, self.task, {"G1": "ROBOT-1", "G2": "ROBOT-2"})


if __name__ == "__main__":
    unittest.main()
