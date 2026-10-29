import asyncio
import importlib.util
from pathlib import Path
import tempfile
import time
import unittest


@unittest.skipUnless(importlib.util.find_spec('fastapi'),'HTTP dependencies not installed')
class AsyncServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_blocking_worker_keeps_loop_responsive_and_drains_cancellation(self):
        from backend.server import ServerSettings,SimulationService
        with tempfile.TemporaryDirectory() as tmp:
            service=SimulationService(ServerSettings(backend='local',robot_count=1,knowledge_backend='none',
                checkpoint_path=Path(tmp)/'checkpoint.json', replay_record=False))
            completed=[]
            def slow():
                time.sleep(.15);completed.append(True)
            task=asyncio.create_task(service._blocking(slow))
            await asyncio.sleep(.02)
            self.assertFalse(task.done())
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            self.assertEqual(completed,[True])
            await service.stop()
