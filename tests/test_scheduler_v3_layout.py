import copy
import math
from pathlib import Path
from types import SimpleNamespace
import unittest

from backend.day1.data import load_scene
from backend.day1.nav import Navigator
from backend.scheduling_v2.motion import Roadmap
from backend.scheduling_v3.layout import audit_layout, audit_roadmap, audit_handover_geometry, handover_candidates, topology_report
from backend.routing import segment_distance


class TopologyTests(unittest.TestCase):
    def test_path_reports_real_articulations_and_bridges(self):
        report = topology_report({0: {}, 1: {}, 2: {}, 3: {}},
                                 {0: [1], 1: [0, 2], 2: [1, 3], 3: [2]})
        self.assertEqual(report["articulation_nodes"], [1, 2])
        self.assertEqual(len(report["bridges"]), 3)
        self.assertFalse(report["single_vertex_failure_preserves_connectivity"])

    def test_cycle_and_disconnected_components_are_not_confused(self):
        edges = {0: [1, 3], 1: [0, 2], 2: [1, 3], 3: [0, 2]}
        self.assertTrue(topology_report({k: {} for k in edges}, edges)["single_vertex_failure_preserves_connectivity"])
        edges[4] = []
        result = topology_report({k: {} for k in edges}, edges)
        self.assertFalse(result["connected"])
        self.assertEqual(result["component_sizes"], [4, 1])

    def test_circle_probe_detects_isolation_in_narrow_chain(self):
        nodes = {0: {"x": 2., "y": 2.}, 1: {"x": 4., "y": 2.}, 2: {"x": 6., "y": 2.}}
        graph = SimpleNamespace(nodes=nodes, edges={0: [1], 1: [0, 2], 2: [1]},
                                locations={"A": {"x": 1., "y": 2.}, "B": {"x": 7., "y": 2.}},
                                base=SimpleNamespace(private={}, clear=lambda a, b, allowed: True))
        graph.connections = lambda name: [0] if name == "A" else [2]
        result = audit_roadmap(graph)
        self.assertTrue(result["initial_roadmap_usable"])
        self.assertGreater(result["stopped_body_probes"]["graph_split_cases"], 0)
        self.assertFalse(result["stopped_body_probes"]["all_probes_preserve_unaffected_terminal_access"])


class ActualLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scene_path = Path(__file__).resolve().parents[1] / "data/layouts/warehouse100-v1.json"
        scene, slots = load_scene(cls.scene_path)
        cls.graph = Roadmap(Navigator(scene, slots))
        cls.audit = audit_layout(cls.scene_path, circle_probes=False)

    def test_actual_loaded_geometry_and_all_1868_endpoints(self):
        audit = self.audit
        self.assertTrue(audit["initial_layout_usable"])
        self.assertEqual(audit["terminals"]["location_ids"], 1868)
        self.assertEqual(audit["counts"], {"homes": 100, "docks": 24, "slots": 1728, "boundary_stations": 16})
        self.assertEqual(audit["terminals"]["unreachable_ids"], [])
        self.assertEqual(audit["terminals"]["unsafe_connectors"], [])
        self.assertEqual(audit["unsafe_public_edges"], [])
        self.assertTrue(audit["geometry"]["planning_envelope_contains_loaded_rectangle"])
        self.assertTrue(audit["geometry"]["parked_bodies_keep_required_separation"])
        self.assertEqual(audit["geometry"]["parked_body_public_edge_intrusions"], [])

    def test_actual_roadmap_has_no_single_vertex_cut(self):
        self.assertTrue(self.audit["topology"]["single_vertex_failure_preserves_connectivity"])
        self.assertEqual(self.audit["topology"]["articulation_nodes"], [])
        self.assertEqual(self.audit["topology"]["bridges"], [])

    def test_business_handover_candidates_keep_both_bodies_clear(self):
        for location in ["R01-L1-C1-F1", "R18-L3-C8-F2", "R36-L2-C8-F2", "IN-1", "OUT-8"]:
            donor = self.graph.locations[location]
            before = copy.deepcopy(donor)
            candidates = handover_candidates(self.graph, donor)
            self.assertTrue(candidates, location)
            self.assertEqual(donor, before)
            for candidate in candidates:
                point = candidate["point"]
                self.assertGreaterEqual(math.dist((point["x"], point["y"]), (donor["x"], donor["y"])), 1.1 - 1e-7)
                self.assertTrue(self.graph.base.clear(point, point, set()))
                for node in candidate["connections"]:
                    other = self.graph.nodes[node]
                    self.assertTrue(self.graph.base.clear(point, other, set()))
                    self.assertGreaterEqual(segment_distance((point["x"], point["y"]), (other["x"], other["y"]),
                                                             (donor["x"], donor["y"])), 1.1 - 1e-7)

    def test_handover_rejects_overlap_and_cannot_manufacture_free_space(self):
        with self.assertRaises(ValueError):
            handover_candidates(self.graph, {"x": 30., "y": 26.}, distances=(.5,))
        with self.assertRaises(ValueError):
            handover_candidates(self.graph, {"x": 30., "y": 26.}, distances=(math.nan,))
        blocked = SimpleNamespace(nodes=self.graph.nodes, base=SimpleNamespace(scene=self.graph.base.scene,
                                   radius=.55, clear=lambda a, b, allowed: False))
        self.assertEqual(handover_candidates(blocked, {"x": 30., "y": 26.}), [])

    def test_handover_audit_deduplicates_rack_levels_not_physical_robots(self):
        names = ["R01-L1-C1-F1", "R01-L2-C1-F1", "OUT-8", "P001"]
        subset = SimpleNamespace(nodes=self.graph.nodes, base=self.graph.base,
                                 locations={name: self.graph.locations[name] for name in names})
        result = audit_handover_geometry(subset)
        self.assertEqual(result["counts"], {"tested": 2, "with_candidate": 2})
        self.assertEqual(result["missing"], [])


if __name__ == "__main__":
    unittest.main()
