from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import Any

from starlette.websockets import WebSocket

from .checkpoint import read_checkpoint, write_checkpoint
from .runtime import build_runtime
from .simulation import SimulationConfig, SimulationCoordinator
from .knowledge import build_knowledge_store, KnowledgeError, source_fingerprint
from .knowledge_queries import WarehouseQueries, CATALOG, CATALOG_VERSION


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHECKPOINT = PROJECT_ROOT / "data" / "checkpoints" / "warehouse-latest.json"


@dataclass
class ServerSettings:
    backend: str = "ray"
    robot_count: int = 100
    max_active_robots: int = 20
    seed: int = 20261013
    step_seconds: float = 0.25
    wall_interval: float = 0.25
    flow_rate: float = 48.0
    traffic_safety_radius: float = 0.12
    ray_address: str | None = None
    ray_num_cpus: float | None = None
    ray_actor_cpus: float = 0.1
    checkpoint_path: Path = DEFAULT_CHECKPOINT
    checkpoint_every_ticks: int = 20
    resume: bool = False
    ray_recovery_attempts: int = 3
    ray_recovery_delay: float = 1.0
    knowledge_backend: str = "journal"
    journal_path: Path = PROJECT_ROOT / "data/knowledge/warehouse.sqlite"
    allow_code_change: bool = False
    inbound_layout: str = 'edge'
    start_paused: bool = False
    max_run_ticks: int = 0
    initial_occupancy: float = .52


class SimulationService:
    def __init__(self, settings: ServerSettings):
        self.settings = settings
        self.knowledge = build_knowledge_store(settings.knowledge_backend, settings.journal_path)
        runtime = build_runtime(
            settings.backend,
            ray_address=settings.ray_address,
            ray_num_cpus=settings.ray_num_cpus,
            ray_actor_cpus=settings.ray_actor_cpus,
        )
        self.coordinator = SimulationCoordinator(
            runtime,
            SimulationConfig(
                robot_count=settings.robot_count,
                max_active_robots=min(settings.max_active_robots, settings.robot_count),
                seed=settings.seed,
                step_seconds=settings.step_seconds,
                flow_rate=settings.flow_rate,
                traffic_cell_size=settings.traffic_safety_radius,
                inbound_layout=settings.inbound_layout,
                initial_occupancy=settings.initial_occupancy,
            ),
            knowledge_store=self.knowledge,
            initialize=not settings.resume,
        )
        if settings.resume:
            self.coordinator.restore_checkpoint(self._recovery_payload())
            origin=self.coordinator.run_metadata.get('initial_backend')
            if origin and origin != settings.backend:
                phases=self.coordinator.run_metadata.setdefault('runtime_transitions',[])
                phase={'tick':self.coordinator.tick,'backend':settings.backend,'source_sha256':source_fingerprint()}
                if not phases or phases[-1].get('backend') != settings.backend:
                    phases.append(phase)
                    self.coordinator.record_system_event(f'同一 run 切换为 {settings.backend} 执行；历史数据来源保留')
                    self.coordinator.persist_knowledge()
        elif settings.start_paused:
            self.coordinator.control('pause')
        self.lock = asyncio.Lock()
        self.subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self.runner: asyncio.Task[None] | None = None
        self.stopping = False
        self.latest_snapshot = self.coordinator.snapshot()
        self.last_error: str | None = None
        self.recovery_count = 0
        self.consecutive_recovery_attempts = 0
        self.simulation_executor = ThreadPoolExecutor(max_workers=1,thread_name_prefix='warehouse-coordinator')
        # Accept 100 concurrent HTTP clients without 100 native threads competing
        # for the Python GIL. Executor queueing is INCLUDED in HTTP latency tests.
        self.query_executor = ThreadPoolExecutor(max_workers=16,thread_name_prefix='warehouse-query')

    async def _blocking(self, function, *args):
        future = asyncio.get_running_loop().run_in_executor(self.simulation_executor,function,*args)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            # Cancelling an asyncio task does not stop its native worker thread.
            # Drain the barrier before stop() closes actors or the journal.
            await future
            raise

    async def start(self) -> None:
        await self._blocking(self._save_checkpoint_locked)
        self.stopping = False
        self.runner = asyncio.create_task(self._run_loop(), name="warehouse-simulation-loop")

    async def stop(self) -> None:
        self.stopping = True
        if self.runner:
            self.runner.cancel()
            try:
                await self.runner
            except asyncio.CancelledError:
                pass
        async with self.lock:
            await self._blocking(self._save_checkpoint_locked)
            await self._blocking(self.coordinator.close)
            if self.knowledge:
                await self._blocking(self.knowledge.close)
        self.simulation_executor.shutdown(wait=True)
        self.query_executor.shutdown(wait=True)

    async def control(self, action: str, value: float | None = None) -> dict[str, Any]:
        async with self.lock:
            snapshot = await self._blocking(self.coordinator.control,action,value)
            self.latest_snapshot = snapshot
            if action == "reset":
                await self._blocking(self._save_checkpoint_locked)
        await self.publish(snapshot)
        return snapshot

    async def inject_priority_task(self) -> dict[str, Any]:
        async with self.lock:
            result = await self._blocking(self.coordinator.inject_priority_task)
            self.latest_snapshot = self.coordinator.snapshot()
        await self.publish(self.latest_snapshot)
        return result

    async def inject_task_batch(self,count=100):
        async with self.lock:
            result=await self._blocking(self.coordinator.inject_task_batch,count)
            self.latest_snapshot=self.coordinator.snapshot()
        await self.publish(self.latest_snapshot)
        return result

    async def save_checkpoint(self) -> dict[str, Any]:
        async with self.lock:
            await self._blocking(self._save_checkpoint_locked)
            return {
                "status": "saved",
                "path": str(self.settings.checkpoint_path),
                "tick": self.coordinator.tick,
            }

    async def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        self.subscribers.add(queue)
        await queue.put(self.latest_snapshot)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self.subscribers.discard(queue)

    async def publish(self, snapshot: dict[str, Any]) -> None:
        for queue in list(self.subscribers):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            try:
                queue.put_nowait(snapshot)
            except asyncio.QueueFull:
                pass

    async def _run_loop(self) -> None:
        while not self.stopping:
            started = time.monotonic()
            if self.coordinator.running:
                try:
                    async with self.lock:
                        snapshot = await self._blocking(self.coordinator.step_once)
                        if self.settings.max_run_ticks > 0 and self.coordinator.tick >= self.settings.max_run_ticks:
                            self.coordinator.record_system_event('已达到本场景有界运行步数；重置可开始新 run')
                            snapshot = await self._blocking(self.coordinator.control,'pause',None)
                        self.latest_snapshot = snapshot
                        every = self.settings.checkpoint_every_ticks
                        if every > 0 and self.coordinator.tick % every == 0:
                            await self._blocking(self._save_checkpoint_locked)
                    self.last_error = None
                    self.consecutive_recovery_attempts = 0
                    await self.publish(snapshot)
                except Exception as exc:  # Ray wraps actor failures in several exception types.
                    recovered = await self._recover_after_step_failure(exc)
                    if not recovered:
                        self.coordinator.running = False
                        self.latest_snapshot = self.coordinator.snapshot()
                        await self.publish(self.latest_snapshot)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.01, self.settings.wall_interval - elapsed))

    async def _recover_after_step_failure(self, exc: Exception) -> bool:
        self.last_error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, KnowledgeError):
            return False
        if self.settings.backend != "ray":
            return False
        self.consecutive_recovery_attempts += 1
        if self.consecutive_recovery_attempts > self.settings.ray_recovery_attempts:
            return False
        await asyncio.sleep(max(0.0, self.settings.ray_recovery_delay))
        try:
            async with self.lock:
                payload = self._recovery_payload()
                self.coordinator.restore_checkpoint(payload)
                self.coordinator.record_system_event(
                    f"Ray Actor 异常，已从 tick {self.coordinator.tick} checkpoint 恢复"
                )
                self.latest_snapshot = self.coordinator.snapshot()
            self.recovery_count += 1
            await self.publish(self.latest_snapshot)
            return True
        except Exception as recovery_exc:
            self.last_error = f"{type(recovery_exc).__name__}: {recovery_exc}"
            return False

    def _save_checkpoint_locked(self) -> None:
        payload = self.coordinator.checkpoint_payload()
        write_checkpoint(self.settings.checkpoint_path, payload)

    def _recovery_payload(self):
        payload = read_checkpoint(self.settings.checkpoint_path)
        if self.knowledge:
            payload = self.knowledge.recovery_payload(payload)
        previous_hash = payload.get("run_metadata", {}).get("source_sha256")
        if previous_hash and previous_hash != source_fingerprint() and not self.settings.allow_code_change:
            raise ValueError("checkpoint source fingerprint differs; explicitly allow code change or use matching source")
        return payload


def create_app(settings: ServerSettings | None = None) -> Any:
    try:
        from fastapi import FastAPI, HTTPException, WebSocketDisconnect
        from fastapi.staticfiles import StaticFiles
        from fastapi.responses import JSONResponse
    except ImportError as exc:
        raise RuntimeError(
            "HTTP server requires FastAPI and Uvicorn; install requirements.txt first"
        ) from exc

    resolved = settings or ServerSettings()
    service = SimulationService(resolved)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    app = FastAPI(title="Warehouse Humanoid Robot Simulation", lifespan=lifespan)
    app.state.simulation_service = service

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        snapshot = service.latest_snapshot
        return {
            "status": "degraded" if service.last_error or snapshot["knowledge"].get("last_error") else "ok",
            "runId": snapshot["runId"],
            "backend": snapshot["backend"],
            "tick": snapshot["tick"],
            "running": snapshot["running"],
            "lastError": service.last_error,
            "recoveryCount": service.recovery_count,
            "knowledge": snapshot["knowledge"],
            **snapshot["runtime"],
        }

    @app.get("/api/snapshot")
    async def snapshot() -> dict[str, Any]:
        return service.latest_snapshot

    @app.get("/api/knowledge/status")
    async def knowledge_status() -> dict:
        return service.latest_snapshot['knowledge']

    @app.get("/api/knowledge/templates")
    async def knowledge_templates() -> dict:
        return {"catalog_version": CATALOG_VERSION, "templates": CATALOG}

    @app.post("/api/knowledge/query")
    async def knowledge_query(request: dict[str, Any]) -> dict:
        if not service.knowledge or not service.knowledge.mirror:
            raise HTTPException(status_code=503, detail="Neo4j read model is not enabled")
        try:
            result = await asyncio.get_running_loop().run_in_executor(
                service.query_executor,WarehouseQueries(service.knowledge.mirror).execute,request)
            # Catalog values have already been decoded into plain JSON types.
            # Avoid a second recursive jsonable_encoder walk on the event loop.
            return JSONResponse(result)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception:
            import logging
            logging.getLogger(__name__).exception('warehouse knowledge query failed')
            raise HTTPException(status_code=503, detail="knowledge query failed; inspect database status") from None

    @app.post("/api/control")
    async def control(request: dict[str, Any]) -> dict[str, Any]:
        try:
            return await service.control(str(request.get("action", "")), request.get("value"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/tasks/priority")
    async def priority_task() -> dict[str, Any]:
        return await service.inject_priority_task()

    @app.post('/api/tasks/batch')
    async def task_batch(request: dict[str,Any]):
        try:
            return await service.inject_task_batch(request.get('count',100))
        except ValueError as exc:
            raise HTTPException(status_code=400,detail=str(exc)) from exc

    @app.post("/api/checkpoint")
    async def checkpoint() -> dict[str, Any]:
        return await service.save_checkpoint()

    @app.websocket("/ws/simulation")
    async def simulation_socket(websocket: WebSocket) -> None:
        await websocket.accept()
        queue = await service.subscribe()
        try:
            while True:
                await websocket.send_json(await queue.get())
        except WebSocketDisconnect:
            pass
        finally:
            service.unsubscribe(queue)

    app.mount("/", StaticFiles(directory=PROJECT_ROOT / "dist", html=True), name="frontend")
    return app


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the warehouse robot simulation server")
    parser.add_argument("--host", default=os.environ.get("WAREHOUSE_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("WAREHOUSE_PORT", "4173")))
    parser.add_argument("--backend", choices=["local", "ray"], default=os.environ.get("SIM_BACKEND", "ray"))
    parser.add_argument("--robot-count", type=int, default=int(os.environ.get("ROBOT_COUNT", "100")))
    parser.add_argument(
        "--max-active-robots",
        type=int,
        default=int(os.environ.get("MAX_ACTIVE_ROBOTS", "20")),
    )
    parser.add_argument("--seed", type=int, default=int(os.environ.get("SIM_SEED", "20261013")))
    parser.add_argument("--step-seconds", type=float, default=float(os.environ.get("STEP_SECONDS", "0.25")))
    parser.add_argument("--wall-interval", type=float, default=float(os.environ.get("WALL_INTERVAL", "0.25")))
    parser.add_argument("--flow-rate", type=float, default=float(os.environ.get("FLOW_RATE", "48")))
    parser.add_argument('--inbound-layout', choices=['edge','corner'],default='edge')
    parser.add_argument('--start-paused',action='store_true')
    parser.add_argument('--max-run-ticks',type=int,default=0)
    parser.add_argument('--initial-occupancy',type=float,default=.52)
    parser.add_argument(
        "--traffic-safety-radius",
        type=float,
        default=float(os.environ.get("TRAFFIC_SAFETY_RADIUS", "0.12")),
    )
    parser.add_argument("--ray-address", default=os.environ.get("RAY_ADDRESS") or None)
    parser.add_argument(
        "--ray-num-cpus",
        type=float,
        default=float(os.environ["RAY_NUM_CPUS"]) if os.environ.get("RAY_NUM_CPUS") else None,
    )
    parser.add_argument(
        "--ray-actor-cpus",
        type=float,
        default=float(os.environ.get("RAY_ACTOR_CPUS", "0.1")),
    )
    parser.add_argument(
        "--checkpoint-path",
        type=Path,
        default=Path(os.environ.get("CHECKPOINT_PATH", DEFAULT_CHECKPOINT)),
    )
    parser.add_argument(
        "--checkpoint-every-ticks",
        type=int,
        default=int(os.environ.get("CHECKPOINT_EVERY_TICKS", "20")),
    )
    parser.add_argument("--resume", action="store_true", default=os.environ.get("RESUME", "0") == "1")
    parser.add_argument("--knowledge-backend", choices=["none", "journal", "neo4j"], default=os.environ.get("KNOWLEDGE_BACKEND", "journal"))
    parser.add_argument("--journal-path", type=Path, default=PROJECT_ROOT / "data/knowledge/warehouse.sqlite")
    parser.add_argument("--allow-code-change", action="store_true")
    parser.add_argument(
        "--ray-recovery-attempts",
        type=int,
        default=int(os.environ.get("RAY_RECOVERY_ATTEMPTS", "3")),
    )
    parser.add_argument(
        "--ray-recovery-delay",
        type=float,
        default=float(os.environ.get("RAY_RECOVERY_DELAY", "1")),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.checkpoint_path.exists() and not args.resume:
        raise SystemExit("checkpoint already exists; use --resume or a fresh --checkpoint-path")
    if args.wall_interval <= 0:
        raise SystemExit("--wall-interval must be positive")
    settings = ServerSettings(
        backend=args.backend,
        robot_count=args.robot_count,
        max_active_robots=args.max_active_robots,
        seed=args.seed,
        step_seconds=args.step_seconds,
        wall_interval=args.wall_interval,
        flow_rate=args.flow_rate,
        inbound_layout=args.inbound_layout,
        start_paused=args.start_paused,
        max_run_ticks=args.max_run_ticks,
        initial_occupancy=args.initial_occupancy,
        traffic_safety_radius=args.traffic_safety_radius,
        ray_address=args.ray_address,
        ray_num_cpus=args.ray_num_cpus,
        ray_actor_cpus=args.ray_actor_cpus,
        checkpoint_path=args.checkpoint_path,
        checkpoint_every_ticks=args.checkpoint_every_ticks,
        resume=args.resume,
        ray_recovery_attempts=args.ray_recovery_attempts,
        ray_recovery_delay=args.ray_recovery_delay,
        knowledge_backend=args.knowledge_backend,
        journal_path=args.journal_path,
        allow_code_change=args.allow_code_change,
    )
    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit("Uvicorn is not installed; install requirements.txt first") from exc
    uvicorn.run(create_app(settings), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
