"""Energy/custody integration regressions, separate from pure-function tests."""
from pathlib import Path
import tempfile
import unittest

from backend.scheduling_v3.scenarios import generate_scenario
from backend.scheduling_v3.sim import Config, Simulation


class EnergyRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]
        cls.temp = tempfile.TemporaryDirectory(prefix="v3-energy-runtime-", dir=cls.root / "tests")
        assert Path(cls.temp.name).resolve().is_relative_to(cls.root)
        cls.scene = cls.root / "data/layouts/warehouse100-v1.json"
        cls.dataset = Path(cls.temp.name) / "input"
        generate_scenario(cls.scene, cls.dataset, active_slots=1, duration_seconds=1,
                          slot_interval_seconds=3600)
        cls.inbound_dataset = Path(cls.temp.name) / "inbound-input"
        generate_scenario(cls.scene, cls.inbound_dataset, active_slots=12,
                          duration_seconds=1800, slot_interval_seconds=1800)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def make(self, target=80):
        sim = Simulation(self.scene, self.dataset, "energy-runtime", Config(
            energy_profile="conservative", charge_target_soc=target, charging_policy="none"))
        for robot in sim.robots.values():
            robot["battery"] = 90.
        return sim

    def test_lowest_soc_high_id_robot_gets_charge_before_nonurgent_robots(self):
        sim = self.make()
        for robot in sim.robots.values():
            robot["battery"] = 34.9
        sim.robots[100]["battery"] = 14.
        sim.release_and_dispatch()
        self.assertIn(100, sim.jobs, "Critical charge dispatch must not prioritise robot ID over energy margin")
        self.assertIsNotNone(sim.jobs[100].get("dock_id"))

    def test_configured_above_taper_target_gets_full_charging_duration(self):
        sim = self.make(target=90)
        robot = sim.robots[1]
        robot["battery"] = 25.
        self.assertTrue(sim.schedule_charge(robot))
        parts = sim._suffix(1)
        index = next(i for i, part in enumerate(parts) if part["phase"] == "charging")
        arrival_soc = robot["battery"] - sim.energy.consume(parts[:index])
        expected = sim.energy.charge_seconds(arrival_soc, 90)
        self.assertGreaterEqual(parts[index]["end"] - parts[index]["start"], expected,
            "Planner must use the same taper curve as actual execution")

    def test_conservative_long_charge_reaches_target_in_actual_steps(self):
        sim = self.make()
        robot = sim.robots[1]
        robot["battery"] = 25.
        self.assertTrue(sim.schedule_charge(robot))
        # Isolate actual execution from unrelated opportunity charging. All 100
        # physical robots are still advanced and included in the safety audit.
        sim.release_and_dispatch = lambda: None
        parts = sim._suffix(1)
        charging = next(part for part in parts if part["phase"] == "charging")
        self.assertGreater(charging["end"] - charging["start"], 3600)
        until = parts[-1]["end"] + 1
        while sim.elapsed < until:
            sim.step()
        events = [event for event in sim.pending_events if event.get("robot_id") == 1]
        completed = [event for event in events if event["kind"] == "charge_complete"]
        self.assertEqual(len(completed), 1)
        self.assertGreaterEqual(completed[0]["battery"], 80 - 1e-6)
        self.assertGreaterEqual(robot["battery"], 10)
        self.assertEqual(robot["stage"], "idle")
        self.assertEqual(robot["location_id"], robot["home_id"])
        self.assertGreaterEqual(sim.metrics["min_separation_m"], 1.1 - 1e-6)

    def test_unpicked_inbound_energy_diversion_keeps_source_custody_and_slot_reservation(self):
        sim = Simulation(self.scene, self.inbound_dataset, "inbound-energy-runtime",
                         Config(energy_profile="conservative", charging_policy="none"))
        selected = None
        for _ in range(1800):
            sim.step()
            for task in sim.tasks.values():
                if task["task_type"] != "INBOUND" or task["status"] != "assigned":
                    continue
                owner = task["robot_id"]
                robot = sim.robots[owner]
                # Trigger only when a real low-SOC reading makes the old
                # business suffix unsafe but an actual charger route is safe.
                for soc in (11.5, 12.0, 12.5, 13.0):
                    if sim.energy.consume(sim._suffix(owner)) + 10 <= soc:
                        continue
                    charge = sim._charge_tail(robot, sim.elapsed, battery=soc)
                    if charge:
                        selected = (task, robot, soc)
                        break
                if selected:
                    break
            if selected:
                break
        self.assertIsNotNone(selected, "fixture must expose an energy-feasible unpicked diversion")
        task, robot, soc = selected
        goods = task["goods_id"]
        source, target, owner = task["source_id"], task["target_id"], robot["robot_id"]
        self.assertEqual(sim.goods[goods], source)
        self.assertTrue(sim.report_soc(owner, soc))
        self.assertEqual(task["status"], "accepted")
        self.assertIsNone(task["robot_id"])
        self.assertEqual(sim.goods[goods], source)
        self.assertEqual(sim.slots[target]["reservedBy"], task["task_id"])
        self.assertFalse(robot["carrying"])
        self.assertIsNotNone(sim.jobs[owner]["dock_id"])
        for _ in range(900):
            sim.step()
            if task["status"] == "completed":
                break
        self.assertEqual(task["status"], "completed")
        self.assertEqual(sim.goods[goods], target)
        events = [e for e in sim.pending_events if e.get("task_id") == task["task_id"]]
        self.assertEqual(sum(e["kind"] == "received" for e in events), 1)
        self.assertEqual(sum(e["kind"] == "picked" for e in events), 1)
        self.assertEqual(sum(e["kind"] == "placed" for e in events), 1)
        sim.assert_inventory()

    def test_unrecoverable_measured_energy_without_helper_pauses_before_any_motion(self):
        sim = Simulation(self.scene, self.inbound_dataset, "unresolved-energy-runtime",
                         Config(energy_profile="conservative", charging_policy="none"))
        donor = None
        for _ in range(1800):
            sim.step()
            donor = next((r for r in sim.robots.values() if r["carrying"]), None)
            if donor:
                break
        self.assertIsNotNone(donor)
        task_id, cargo_id = donor["task_id"], donor["cargo_id"]
        # Fixture deliberately makes every other empty robot ineligible for
        # helping. This is an EXPECTED UNRESOLVED fault, not a successful
        # automatic-recovery scenario to count among passed recoveries.
        for robot in sim.robots.values():
            if robot["robot_id"] != donor["robot_id"]:
                robot["battery"] = 34.9
        self.assertFalse(sim.report_soc(donor["robot_id"], 11.0))
        self.assertIsNotNone(sim.energy_unresolved)
        elapsed = sim.elapsed
        positions = {i: (r["x"], r["y"]) for i, r in sim.robots.items()}
        with self.assertRaisesRegex(RuntimeError, "unresolved measured energy"):
            sim.step()
        self.assertEqual(sim.elapsed, elapsed)
        self.assertEqual({i: (r["x"], r["y"]) for i, r in sim.robots.items()}, positions)
        self.assertEqual(sim.goods[cargo_id], f"ROBOT-{donor['robot_id']}")
        self.assertEqual(sim.tasks[task_id]["status"], "carrying")
        self.assertEqual(sim.metrics["handoffs"], 0)
        self.assertTrue(donor["carrying"])
        sim.assert_inventory()


if __name__ == "__main__":
    unittest.main()
