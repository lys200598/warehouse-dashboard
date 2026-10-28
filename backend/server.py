from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import os
from pathlib import Path
import time
from typing import Any

from .checkpoint import read_checkpoint, write_checkpoint
from .knowledge import InMemoryKnowledgeStore
from .runtime import build_runtime
from .simulation import SimulationConfig, SimulationCoordinator
from .warehouse_graph_query import Neo4jWarehouseGraph, WarehouseGraphUnavailable
from .warehouse_runtime_graph import Neo4jRuntimeGraphWriter, RuntimeGraphUnavailable


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHECKPOINT = PROJECT_ROOT / "data" / "checkpoints" / "warehouse-latest.json"
DEFAULT_TEST_DATASET = PROJECT_ROOT / "data" / "generated" / "current-layout-day1" / "tasks.csv"


def dataset_id_for_task_file(path: Path | None) -> str | None:
    """Derive the immutable graph dataset ID from a generated package manifest."""
    if path is None:
        return None
    manifest_path = path.parent / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return f"dataset:{manifest['version']}:{str(manifest['start'])[:10]}"
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        return None


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
    dataset_path: Path | None = DEFAULT_TEST_DATASET if DEFAULT_TEST_DATASET.exists() else None
    warehouse_neo4j_uri: str = "bolt://127.0.0.1:47687"
    warehouse_neo4j_user: str = "neo4j"
    warehouse_neo4j_password: str | None = None
    warehouse_neo4j_database: str = "neo4j"
    warehouse_neo4j_dataset_id: str | None = None
    warehouse_neo4j_write_enabled: bool = False
    warehouse_neo4j_state_interval_ticks: int = 4


class SimulationService:
    def __init__(self, settings: ServerSettings):
        self.settings = settings
        self.dataset_id = settings.warehouse_neo4j_dataset_id or dataset_id_for_task_file(settings.dataset_path)
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
                dataset_path=str(settings.dataset_path) if settings.dataset_path else None,
            ),
        )
        self.lock = asyncio.Lock()
        self.subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self.runner: asyncio.Task[None] | None = None
        self.stopping = False
        self.latest_snapshot = self.coordinator.snapshot()
        self.knowledge = InMemoryKnowledgeStore()
        self.knowledge.refresh(self.latest_snapshot)
        self.warehouse_graph = Neo4jWarehouseGraph(
            uri=settings.warehouse_neo4j_uri,
            user=settings.warehouse_neo4j_user,
            password=settings.warehouse_neo4j_password,
            database=settings.warehouse_neo4j_database,
            dataset_id=self.dataset_id,
        )
        self.runtime_graph = Neo4jRuntimeGraphWriter(
            uri=settings.warehouse_neo4j_uri, user=settings.warehouse_neo4j_user,
            password=settings.warehouse_neo4j_password, database=settings.warehouse_neo4j_database,
            dataset_id=self.dataset_id,
            state_interval_ticks=settings.warehouse_neo4j_state_interval_ticks,
        )
        self.runtime_graph_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        self.runtime_graph_runner: asyncio.Task[None] | None = None
        self.runtime_graph_last_error: str | None = None
        self.last_error: str | None = None
        self.recovery_count = 0
        self.consecutive_recovery_attempts = 0

    async def start(self) -> None:
        if self.settings.resume:
            if not self.settings.checkpoint_path.exists():
                raise FileNotFoundError(
                    f"resume requested but checkpoint does not exist: {self.settings.checkpoint_path}"
                )
            payload = read_checkpoint(self.settings.checkpoint_path)
            self.coordinator.restore_checkpoint(payload)
            self.latest_snapshot = self.coordinator.snapshot()
            self.knowledge.refresh(self.latest_snapshot)
        else:
            self._save_checkpoint_locked()
        self.stopping = False
        self.runner = asyncio.create_task(self._run_loop(), name="warehouse-simulation-loop")
        if self.settings.warehouse_neo4j_write_enabled:
            self.runtime_graph_runner = asyncio.create_task(self._runtime_graph_loop(), name="warehouse-neo4j-writer")
            self._queue_runtime_graph_snapshot(self.latest_snapshot)

    async def stop(self) -> None:
        self.stopping = True
        if self.runner:
            self.runner.cancel()
            try:
                await self.runner
            except asyncio.CancelledError:
                pass
        if self.runtime_graph_runner:
            self.runtime_graph_runner.cancel()
            try:
                await self.runtime_graph_runner
            except asyncio.CancelledError:
                pass
        async with self.lock:
            self._save_checkpoint_locked()
            self.coordinator.close()
            self.warehouse_graph.close()
            self.runtime_graph.close()

    async def control(self, action: str, value: float | None = None) -> dict[str, Any]:
        async with self.lock:
            snapshot = self.coordinator.control(action, value)
            self.latest_snapshot = snapshot
            self.knowledge.refresh(snapshot)
            if action == "reset":
                self._save_checkpoint_locked()
        await self.publish(snapshot)
        return snapshot

    async def inject_priority_task(self) -> dict[str, Any]:
        async with self.lock:
            result = self.coordinator.inject_priority_task()
            self.latest_snapshot = self.coordinator.snapshot()
            self.knowledge.refresh(self.latest_snapshot)
        await self.publish(self.latest_snapshot)
        return result

    async def save_checkpoint(self) -> dict[str, Any]:
        async with self.lock:
            self._save_checkpoint_locked()
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

    def _queue_runtime_graph_snapshot(self, snapshot: dict[str, Any]) -> None:
        if not self.settings.warehouse_neo4j_write_enabled:
            return
        if self.runtime_graph_queue.full():
            try:
                self.runtime_graph_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            self.runtime_graph_queue.put_nowait(snapshot)
        except asyncio.QueueFull:
            pass

    async def _runtime_graph_loop(self) -> None:
        while not self.stopping:
            snapshot = await self.runtime_graph_queue.get()
            try:
                await asyncio.to_thread(self.runtime_graph.record_snapshot, snapshot)
                self.runtime_graph_last_error = None
            except RuntimeGraphUnavailable as exc:
                self.runtime_graph_last_error = str(exc)

    async def _run_loop(self) -> None:
        while not self.stopping:
            started = time.monotonic()
            if self.coordinator.running:
                try:
                    async with self.lock:
                        snapshot = self.coordinator.step_once()
                        self.latest_snapshot = snapshot
                        self.knowledge.refresh(snapshot)
                        every = self.settings.checkpoint_every_ticks
                        if every > 0 and self.coordinator.tick % every == 0:
                            self._save_checkpoint_locked()
                    self.last_error = None
                    self.consecutive_recovery_attempts = 0
                    self._queue_runtime_graph_snapshot(snapshot)
                    await self.publish(snapshot)
                except Exception as exc:  # Ray wraps actor failures in several exception types.
                    recovered = await self._recover_after_step_failure(exc)
                    if not recovered:
                        self.coordinator.running = False
                        self.latest_snapshot = self.coordinator.snapshot()
                        self.knowledge.refresh(self.latest_snapshot)
                        await self.publish(self.latest_snapshot)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.01, self.settings.wall_interval - elapsed))

    async def _recover_after_step_failure(self, exc: Exception) -> bool:
        self.last_error = f"{type(exc).__name__}: {exc}"
        if self.settings.backend != "ray":
            return False
        self.consecutive_recovery_attempts += 1
        if self.consecutive_recovery_attempts > self.settings.ray_recovery_attempts:
            return False
        await asyncio.sleep(max(0.0, self.settings.ray_recovery_delay))
        try:
            async with self.lock:
                payload = read_checkpoint(self.settings.checkpoint_path)
                self.coordinator.restore_checkpoint(payload)
                self.coordinator.record_system_event(
                    f"Ray Actor 异常，已从 tick {self.coordinator.tick} checkpoint 恢复"
                )
                self.latest_snapshot = self.coordinator.snapshot()
                self.knowledge.refresh(self.latest_snapshot)
            self.recovery_count += 1
            await self.publish(self.latest_snapshot)
            return True
        except Exception as recovery_exc:
            self.last_error = f"{type(recovery_exc).__name__}: {recovery_exc}"
            return False

    def _save_checkpoint_locked(self) -> None:
        payload = self.coordinator.checkpoint_payload()
        write_checkpoint(self.settings.checkpoint_path, payload)


def create_app(settings: ServerSettings | None = None) -> Any:
    try:
        from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
        from fastapi.staticfiles import StaticFiles
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
            "status": "degraded" if service.last_error else "ok",
            "runId": snapshot["runId"],
            "backend": snapshot["backend"],
            "tick": snapshot["tick"],
            "running": snapshot["running"],
            "lastError": service.last_error,
            "recoveryCount": service.recovery_count,
            "runtimeGraph": {
                "enabled": service.settings.warehouse_neo4j_write_enabled,
                "stateIntervalTicks": service.settings.warehouse_neo4j_state_interval_ticks,
                "lastError": service.runtime_graph_last_error,
            },
            **snapshot["runtime"],
        }

    @app.get("/api/snapshot")
    async def snapshot() -> dict[str, Any]:
        return service.latest_snapshot

    @app.get("/api/kg/summary")
    async def knowledge_summary() -> dict[str, Any]:
        async with service.lock:
            service.knowledge.refresh(service.latest_snapshot)
            return service.knowledge.summary()

    @app.get("/api/kg/robots/{robot_id}/subgraph")
    async def robot_knowledge_subgraph(robot_id: int) -> dict[str, Any]:
        async with service.lock:
            service.knowledge.refresh(service.latest_snapshot)
            graph = service.knowledge.robot_subgraph(robot_id)
        if graph is None:
            raise HTTPException(status_code=404, detail=f"robot {robot_id} does not exist")
        return graph

    @app.get("/api/kg/warehouse/health")
    async def warehouse_graph_health() -> dict[str, Any]:
        return await asyncio.to_thread(service.warehouse_graph.health)

    @app.get("/api/kg/warehouse/summary")
    async def warehouse_graph_summary(dataset_id: str | None = None) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.summary, dataset_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/dashboard")
    async def warehouse_graph_dashboard(dataset_id: str | None = None) -> dict[str, Any]:
        """Small, UI-safe graph overview; never returns graph entities in bulk."""
        try:
            summary = await asyncio.to_thread(service.warehouse_graph.summary, dataset_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        health = await asyncio.to_thread(service.warehouse_graph.health)
        return {"source": "neo4j-knowledge-graph", "health": health, "summary": summary}

    @app.get("/api/kg/warehouse/runs/{run_id}/progress")
    async def warehouse_run_progress(run_id: str) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.run_write_progress, run_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/tasks/{task_id}/subgraph")
    async def warehouse_task_subgraph(task_id: str, dataset_id: str | None = None) -> dict[str, Any]:
        try:
            graph = await asyncio.to_thread(service.warehouse_graph.task_subgraph, task_id, dataset_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if graph is None:
            raise HTTPException(status_code=404, detail=f"warehouse task {task_id} does not exist")
        return graph

    @app.get("/api/kg/warehouse/tasks/{task_id}/trace")
    async def warehouse_task_trace(task_id: str, dataset_id: str | None = None) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.task_execution_trace, task_id, dataset_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/tasks")
    async def warehouse_task_list(limit: int = 8, dataset_id: str | None = None) -> list[dict[str, Any]]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.task_list, limit, dataset_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/query-presets")
    async def warehouse_graph_query_presets() -> dict[str, Any]:
        return {"presets": list(service.warehouse_graph.QUERY_PRESETS), "readOnly": True, "maxRows": 200}

    @app.post("/api/kg/warehouse/query-presets/{preset}")
    async def warehouse_graph_query_preset(preset: str, request: dict[str, Any]) -> dict[str, Any]:
        """Bounded named reads for the UI; arbitrary Cypher is intentionally unsupported."""
        try:
            return await asyncio.to_thread(
                service.warehouse_graph.query_preset,
                preset,
                task_id=str(request.get("taskId", ""))[:160],
                run_id=str(request.get("runId", ""))[:160],
                robot_id=int(request.get("robotId", 1)),
                tick=int(request.get("tick", 0)),
            )
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/runs/{run_id}/robots/{robot_id}/subgraph")
    async def warehouse_robot_execution_subgraph(run_id: str, robot_id: int) -> dict[str, Any]:
        try:
            graph = await asyncio.to_thread(service.warehouse_graph.robot_execution_subgraph, run_id, robot_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if graph is None:
            raise HTTPException(status_code=404, detail=f"robot {robot_id} does not exist in run {run_id}")
        return graph

    @app.get("/api/kg/warehouse/runs/{run_id}/robots/{robot_id}/timeline")
    async def warehouse_robot_timeline(run_id: str, robot_id: int, start_tick: int = 0, end_tick: int = 10000, limit: int = 200) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.robot_timeline, run_id, robot_id, start_tick, end_tick, limit)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/runs/{run_id}/robots/{robot_id}/trace")
    async def warehouse_robot_trace(run_id: str, robot_id: int, tick: int | None = None, history_limit: int = 20) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.robot_trace_context, run_id, robot_id, tick, history_limit)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/runs/{run_id}/replay")
    async def warehouse_replay(run_id: str, tick: int) -> dict[str, Any]:
        try:
            states = await asyncio.to_thread(service.warehouse_graph.replay_states, run_id, tick)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if not states:
            raise HTTPException(status_code=404, detail="no graph states at this tick")
        current = service.latest_snapshot
        by_id = {robot["id"]: robot for robot in current["robots"]}
        robots = []
        for item in states:
            base = dict(by_id.get(item["robot_id"], {})); base.update({"id": item["robot_id"], "stage": item["stage"], "battery": item["battery"], "x": item["x"], "y": item["y"], "taskId": item.get("task_id"), "cargoId": item.get("cargo_id")}); robots.append(base)
        return {**current, "backend": "neo4j-replay", "runId": run_id, "tick": tick, "robots": robots, "events": [], "generatedAtMs": int(time.time() * 1000)}

    @app.get("/api/kg/warehouse/runs/{run_id}/runtime-trace")
    async def warehouse_runtime_trace(run_id: str, tick: int | None = None, robot_id: int | None = None) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.runtime_state_trace, run_id, tick, robot_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.post("/api/control")
    async def control(request: dict[str, Any]) -> dict[str, Any]:
        try:
            return await service.control(str(request.get("action", "")), request.get("value"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/tasks/priority")
    async def priority_task() -> dict[str, Any]:
        return await service.inject_priority_task()

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
    parser.add_argument("--dataset-path", type=Path, default=Path(os.environ["DATASET_PATH"]) if os.environ.get("DATASET_PATH") else (DEFAULT_TEST_DATASET if DEFAULT_TEST_DATASET.exists() else None))
    parser.add_argument("--warehouse-neo4j-uri", default=os.environ.get("WAREHOUSE_NEO4J_URI", "bolt://127.0.0.1:47687"))
    parser.add_argument("--warehouse-neo4j-user", default=os.environ.get("WAREHOUSE_NEO4J_USER", "neo4j"))
    parser.add_argument("--warehouse-neo4j-database", default=os.environ.get("WAREHOUSE_NEO4J_DATABASE", "neo4j"))
    parser.add_argument("--warehouse-neo4j-dataset-id", default=os.environ.get("WAREHOUSE_NEO4J_DATASET_ID") or None)
    parser.add_argument("--warehouse-neo4j-state-interval-ticks", type=int, default=int(os.environ.get("WAREHOUSE_NEO4J_STATE_INTERVAL_TICKS", "4")))
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
        traffic_safety_radius=args.traffic_safety_radius,
        ray_address=args.ray_address,
        ray_num_cpus=args.ray_num_cpus,
        ray_actor_cpus=args.ray_actor_cpus,
        checkpoint_path=args.checkpoint_path,
        checkpoint_every_ticks=args.checkpoint_every_ticks,
        resume=args.resume,
        ray_recovery_attempts=args.ray_recovery_attempts,
        ray_recovery_delay=args.ray_recovery_delay,
        dataset_path=args.dataset_path,
        warehouse_neo4j_uri=args.warehouse_neo4j_uri,
        warehouse_neo4j_user=args.warehouse_neo4j_user,
        warehouse_neo4j_password=os.environ.get("WAREHOUSE_NEO4J_PASSWORD"),
        warehouse_neo4j_database=args.warehouse_neo4j_database,
        warehouse_neo4j_dataset_id=args.warehouse_neo4j_dataset_id,
        warehouse_neo4j_write_enabled=os.environ.get("WAREHOUSE_NEO4J_WRITE_ENABLED", "0") == "1",
        warehouse_neo4j_state_interval_ticks=args.warehouse_neo4j_state_interval_ticks,
    )
    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit("Uvicorn is not installed; install requirements.txt first") from exc
    uvicorn.run(create_app(settings), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
