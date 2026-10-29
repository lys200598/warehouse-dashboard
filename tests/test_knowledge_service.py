import importlib.util
from pathlib import Path
import tempfile
import unittest


@unittest.skipUnless(importlib.util.find_spec("fastapi"), "HTTP dependencies not installed")
class ServiceKnowledgeTest(unittest.TestCase):
    def test_resume_uses_journal_without_creating_extra_run(self):
        from backend.server import ServerSettings, SimulationService
        from backend.checkpoint import write_checkpoint
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            settings = ServerSettings(backend="local", robot_count=3, max_active_robots=3,
                checkpoint_path=path/"checkpoint.json", journal_path=path/"journal.sqlite",
                replay_record=False)
            service = SimulationService(settings)
            original_run = service.coordinator.run_id
            write_checkpoint(settings.checkpoint_path, service.coordinator.checkpoint_payload())
            service.coordinator.step_once()  # Durable source is ahead of external checkpoint.
            service.coordinator.close()
            service.knowledge.close()
            settings.resume = True
            recovered = SimulationService(settings)
            try:
                self.assertEqual(recovered.coordinator.run_id, original_run)
                self.assertEqual(recovered.coordinator.tick, 1)
                self.assertEqual(recovered.knowledge.db.execute("SELECT count(*) FROM heads").fetchone()[0], 1)
            finally:
                recovered.coordinator.close()
                recovered.knowledge.close()

    def test_websocket_annotation_still_resolves(self):
        from backend.server import create_app, ServerSettings
        with tempfile.TemporaryDirectory() as tmp:
            app = create_app(ServerSettings(backend="local", robot_count=1, knowledge_backend="none",
                                           checkpoint_path=Path(tmp)/"checkpoint.json",
                                           replay_record=False))
            service = app.state.simulation_service
            try:
                route = next(r for r in app.routes if getattr(r,"path",None)=="/ws/simulation")
                self.assertEqual(route.dependant.websocket_param_name,"websocket")
                self.assertEqual(route.dependant.query_params,[])
            finally:
                service.coordinator.close()
