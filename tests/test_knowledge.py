import asyncio
import unittest

from backend.knowledge import InMemoryKnowledgeStore
from backend.runtime import LocalRobotRuntime
from backend.server import ServerSettings, create_app
from backend.simulation import SimulationConfig, SimulationCoordinator


class KnowledgeStoreTest(unittest.TestCase):
    def setUp(self):
        self.simulation = SimulationCoordinator(
            LocalRobotRuntime(), SimulationConfig(robot_count=4, max_active_robots=4)
        )
        self.store = InMemoryKnowledgeStore(history_steps=2)

    def tearDown(self):
        self.simulation.close()

    def test_summary_and_robot_subgraph_are_derived_from_snapshot(self):
        self.store.refresh(self.simulation.snapshot())
        summary = self.store.summary()
        graph = self.store.robot_subgraph(1)
        self.assertEqual(summary["backend"], "memory-test-data")
        self.assertEqual(summary["robotCount"], 4)
        self.assertGreater(summary["nodeCount"], 4)
        self.assertIsNotNone(graph)
        self.assertEqual(graph["robotId"], 1)
        self.assertTrue(any(node["id"] == "robot:1" for node in graph["nodes"]))
        self.assertTrue(any(edge["label"] == "IN_RUN" for edge in graph["edges"]))

    def test_state_history_is_bounded(self):
        for _ in range(4):
            self.simulation.step_once()
            self.store.refresh(self.simulation.snapshot())
        self.assertLessEqual(self.store.summary()["stateCount"], 8)

    def test_knowledge_endpoints_return_test_graph_without_network_io(self):
        app = create_app(ServerSettings(backend="local", robot_count=4, max_active_robots=4))
        endpoints = {route.path: route.endpoint for route in app.routes if hasattr(route, "endpoint")}
        service = app.state.simulation_service
        try:
            summary = asyncio.run(endpoints["/api/kg/summary"]())
            graph = asyncio.run(endpoints["/api/kg/robots/{robot_id}/subgraph"](1))
            self.assertEqual(summary["backend"], "memory-test-data")
            self.assertEqual(summary["robotCount"], 4)
            self.assertEqual(graph["robotId"], 1)
        finally:
            service.coordinator.close()


if __name__ == "__main__":
    unittest.main()
