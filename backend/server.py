from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import os
from pathlib import Path
import time
from typing import Any

from starlette.websockets import WebSocket

from .checkpoint import read_checkpoint, write_checkpoint
from .console_replay import ConsoleFrameRecorder
from .runtime import build_runtime
from .simulation import SimulationConfig, SimulationCoordinator
from .knowledge import build_knowledge_store, KnowledgeError, source_fingerprint
from .knowledge_queries import WarehouseQueries, CATALOG, CATALOG_VERSION
from .warehouse_graph_query import Neo4jWarehouseGraph, WarehouseGraphUnavailable
from .warehouse_runtime_graph import Neo4jRuntimeGraphWriter, RuntimeGraphUnavailable
from .dataset import load_initial_inventory, load_warehouse_tasks


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHECKPOINT = PROJECT_ROOT / "data" / "checkpoints" / "warehouse-latest.json"
DEFAULT_DASHBOARD_LAYOUT = PROJECT_ROOT / "data" / "layouts" / "expansion-candidates" / "balanced" / "layout.json"


def dataset_id_for_package(path: Path | None) -> str | None:
    """Resolve a static graph dataset ID without coupling V5 task ingestion to it."""
    if path is None:
        return None
    try:
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        return f"dataset:{manifest['version']}:{str(manifest['start'])[:10]}"
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        return None


def dataset_id_for_task_file(path: Path | None) -> str | None:
    """Compatibility helper for callers that still provide ``tasks.csv``.

    The V5 contract identifies a dataset through the package-level manifest,
    rather than the task file itself.  Retaining this narrow adapter keeps old
    callers working while making the package directory the canonical input.
    """
    return dataset_id_for_package(path.parent if path is not None else None)


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
    ray_node_ip_address: str | None = "127.0.0.1"
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
    warehouse_neo4j_uri: str = "bolt://127.0.0.1:47687"
    warehouse_neo4j_user: str = "neo4j"
    warehouse_neo4j_password: str | None = None
    warehouse_neo4j_database: str = "neo4j"
    warehouse_neo4j_dataset_id: str | None = None
    warehouse_neo4j_package_dir: Path | None = None
    simulation_dataset_dir: Path | None = None
    warehouse_neo4j_write_enabled: bool = False
    warehouse_neo4j_write_mode: str = "robot-actors"
    warehouse_neo4j_state_interval_ticks: int = 4
    replay_record: bool = True
    replay_record_seconds: float = 5.0
    replay_record_dir: Path = PROJECT_ROOT / "data/local-runs"


class SimulationService:
    def __init__(self, settings: ServerSettings):
        self.settings = settings
        self.knowledge = build_knowledge_store(settings.knowledge_backend, settings.journal_path)
        package_dir = settings.simulation_dataset_dir or settings.warehouse_neo4j_package_dir
        self.dataset_id = settings.warehouse_neo4j_dataset_id or dataset_id_for_package(package_dir)
        dataset_tasks: list[dict[str, Any]] = []
        dataset_inventory: list[dict[str, str]] = []
        dataset_metadata: dict[str, Any] = {}
        if settings.simulation_dataset_dir is not None:
            task_file = settings.simulation_dataset_dir / "tasks.csv"
            if not task_file.is_file():
                raise ValueError(f"simulation dataset needs tasks.csv: {settings.simulation_dataset_dir}")
            dataset_tasks = load_warehouse_tasks(task_file)
            dataset_inventory = load_initial_inventory(task_file)
            dataset_metadata = {
                "dataset_origin": "csv-package",
                "dataset_package": str(settings.simulation_dataset_dir),
                "dataset_task_count": len(dataset_tasks),
            }
        runtime = build_runtime(
            settings.backend,
            ray_address=settings.ray_address,
            ray_num_cpus=settings.ray_num_cpus,
            ray_actor_cpus=settings.ray_actor_cpus,
            ray_node_ip_address=settings.ray_node_ip_address,
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
            metadata=dataset_metadata,
            dataset_tasks=dataset_tasks,
            dataset_inventory=dataset_inventory,
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
        if settings.warehouse_neo4j_write_mode not in {"robot-actors", "central"}:
            raise ValueError("warehouse_neo4j_write_mode must be robot-actors or central")
        if (settings.warehouse_neo4j_write_enabled
                and settings.warehouse_neo4j_write_mode == "robot-actors"
                and settings.backend != "ray"):
            raise ValueError("robot-actors Neo4j writing requires --backend ray")
        self.lock = asyncio.Lock()
        self.subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self.runner: asyncio.Task[None] | None = None
        self.stopping = False
        self.latest_snapshot = self.coordinator.snapshot()
        self.replay_recorder: ConsoleFrameRecorder | None = None
        self.replay_record_error: str | None = None
        self._reset_replay_recorder()
        self.warehouse_graph = Neo4jWarehouseGraph(
            uri=settings.warehouse_neo4j_uri, user=settings.warehouse_neo4j_user,
            password=settings.warehouse_neo4j_password, database=settings.warehouse_neo4j_database,
            dataset_id=self.dataset_id,
        )
        # Legacy centralized writer remains opt-in for compatibility.  The
        # realtime 4173 default is one writer per Ray robot Actor.
        self.runtime_graph = (
            Neo4jRuntimeGraphWriter(
                uri=settings.warehouse_neo4j_uri, user=settings.warehouse_neo4j_user,
                password=settings.warehouse_neo4j_password, database=settings.warehouse_neo4j_database,
                dataset_id=self.dataset_id, state_interval_ticks=settings.warehouse_neo4j_state_interval_ticks,
            ) if settings.warehouse_neo4j_write_mode == "central" else None
        )
        self.runtime_graph_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        self.runtime_graph_runner: asyncio.Task[None] | None = None
        self.runtime_graph_last_error: str | None = None
        self.last_error: str | None = None
        self.recovery_count = 0
        self.consecutive_recovery_attempts = 0
        self.simulation_executor = ThreadPoolExecutor(max_workers=1,thread_name_prefix='warehouse-coordinator')
        # Accept 100 concurrent HTTP clients without 100 native threads competing
        # for the Python GIL. Executor queueing is INCLUDED in HTTP latency tests.
        self.query_executor = ThreadPoolExecutor(max_workers=16,thread_name_prefix='warehouse-query')
        if settings.warehouse_neo4j_write_enabled and settings.warehouse_neo4j_write_mode == "robot-actors":
            self._configure_robot_graph_writers()

    def _reset_replay_recorder(self) -> None:
        if self.replay_recorder is not None:
            try:
                self.replay_recorder.close()
            except Exception as exc:
                self.replay_record_error = f"{type(exc).__name__}: {exc}"
            self.replay_recorder = None
        if not self.settings.replay_record:
            return
        try:
            self.replay_recorder = ConsoleFrameRecorder(
                self.settings.replay_record_dir,
                self.coordinator.run_id,
                self.settings.replay_record_seconds,
            )
            self.replay_record_error = None
        except Exception as exc:
            self.replay_recorder = None
            self.replay_record_error = f"{type(exc).__name__}: {exc}"

    def _configure_robot_graph_writers(self) -> None:
        if not self.settings.warehouse_neo4j_password:
            raise ValueError("WAREHOUSE_NEO4J_PASSWORD is required when graph writing is enabled")
        # This is schema-only bootstrap, not runtime graph persistence.  It
        # avoids 100 actors racing to create the same constraints at startup.
        schema_writer = Neo4jRuntimeGraphWriter(
            uri=self.settings.warehouse_neo4j_uri, user=self.settings.warehouse_neo4j_user,
            password=self.settings.warehouse_neo4j_password,
            database=self.settings.warehouse_neo4j_database,
        )
        try:
            schema_writer.ensure_schema()
        finally:
            schema_writer.close()
        self.coordinator.configure_worker_graph({
            "uri": self.settings.warehouse_neo4j_uri,
            "user": self.settings.warehouse_neo4j_user,
            "password": self.settings.warehouse_neo4j_password,
            "database": self.settings.warehouse_neo4j_database,
            "dataset_id": self.dataset_id,
            "state_interval_ticks": self.settings.warehouse_neo4j_state_interval_ticks,
            "schema_ready": True,
        })

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
        if self.replay_recorder is not None:
            try:
                await self._blocking(self.replay_recorder.record, self.latest_snapshot)
            except Exception as exc:
                self.replay_record_error = f"{type(exc).__name__}: {exc}"
        self.stopping = False
        self.runner = asyncio.create_task(self._run_loop(), name="warehouse-simulation-loop")
        if (self.settings.warehouse_neo4j_write_enabled
                and self.settings.warehouse_neo4j_write_mode == "central"):
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
            await self._blocking(self._save_checkpoint_locked)
            if self.replay_recorder is not None:
                try:
                    await self._blocking(self.replay_recorder.close)
                except Exception as exc:
                    self.replay_record_error = f"{type(exc).__name__}: {exc}"
                self.replay_recorder = None
            await self._blocking(self.coordinator.close)
            if self.knowledge:
                await self._blocking(self.knowledge.close)
            self.warehouse_graph.close()
            if self.runtime_graph is not None:
                self.runtime_graph.close()
        self.simulation_executor.shutdown(wait=True)
        self.query_executor.shutdown(wait=True)

    async def control(self, action: str, value: float | None = None) -> dict[str, Any]:
        async with self.lock:
            snapshot = await self._blocking(self.coordinator.control,action,value)
            self.latest_snapshot = snapshot
            if action == "reset":
                self._reset_replay_recorder()
                if self.replay_recorder is not None:
                    try:
                        await self._blocking(self.replay_recorder.record, snapshot)
                    except Exception as exc:
                        self.replay_record_error = f"{type(exc).__name__}: {exc}"
                await self._blocking(self._save_checkpoint_locked)
                if (self.settings.warehouse_neo4j_write_enabled
                        and self.settings.warehouse_neo4j_write_mode == "robot-actors"):
                    await self._blocking(self._configure_robot_graph_writers)
        await self.publish(snapshot)
        self._queue_runtime_graph_snapshot(snapshot)
        return snapshot

    async def inject_priority_task(self) -> dict[str, Any]:
        async with self.lock:
            result = await self._blocking(self.coordinator.inject_priority_task)
            self.latest_snapshot = self.coordinator.snapshot()
        await self.publish(self.latest_snapshot)
        self._queue_runtime_graph_snapshot(self.latest_snapshot)
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

    def _queue_runtime_graph_snapshot(self, snapshot: dict[str, Any]) -> None:
        if (not self.settings.warehouse_neo4j_write_enabled
                or self.settings.warehouse_neo4j_write_mode != "central"):
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
                if self.runtime_graph is None:
                    return
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
                        snapshot = await self._blocking(self.coordinator.step_once)
                        if self.settings.max_run_ticks > 0 and self.coordinator.tick >= self.settings.max_run_ticks:
                            self.coordinator.record_system_event('已达到本场景有界运行步数；重置可开始新 run')
                            snapshot = await self._blocking(self.coordinator.control,'pause',None)
                        self.latest_snapshot = snapshot
                        if self.replay_recorder is not None and self.replay_recorder.due(self.coordinator.elapsed):
                            try:
                                await self._blocking(self.replay_recorder.record, snapshot)
                            except Exception as exc:
                                self.replay_record_error = f"{type(exc).__name__}: {exc}"
                        every = self.settings.checkpoint_every_ticks
                        if every > 0 and self.coordinator.tick % every == 0:
                            await self._blocking(self._save_checkpoint_locked)
                    self.last_error = None
                    self.consecutive_recovery_attempts = 0
                    self._queue_runtime_graph_snapshot(snapshot)
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
                if (self.settings.warehouse_neo4j_write_enabled
                        and self.settings.warehouse_neo4j_write_mode == "robot-actors"):
                    self._configure_robot_graph_writers()
                self.coordinator.record_system_event(
                    f"Ray Actor 异常，已从 tick {self.coordinator.tick} checkpoint 恢复"
                )
                self.latest_snapshot = self.coordinator.snapshot()
            self.recovery_count += 1
            self._queue_runtime_graph_snapshot(self.latest_snapshot)
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
        worker_errors = snapshot.get("runtime", {}).get("graphWriterErrorSamples", [])
        return {
            "status": "degraded" if service.last_error or snapshot["knowledge"].get("last_error") else "ok",
            "runId": snapshot["runId"],
            "backend": snapshot["backend"],
            "tick": snapshot["tick"],
            "running": snapshot["running"],
            "lastError": service.last_error,
            "recoveryCount": service.recovery_count,
            "knowledge": snapshot["knowledge"],
            "runtimeGraph": {
                "enabled": service.settings.warehouse_neo4j_write_enabled,
                "writeMode": service.settings.warehouse_neo4j_write_mode,
                "writerOwner": (
                    "ray-robot-actors" if service.settings.warehouse_neo4j_write_mode == "robot-actors"
                    else "server-background-worker"
                ),
                "stateIntervalTicks": service.settings.warehouse_neo4j_state_interval_ticks,
                "lastError": service.runtime_graph_last_error or (worker_errors[0]["error"] if worker_errors else None),
            },
            "replayRecord": {
                "enabled": service.settings.replay_record,
                "directory": str(service.replay_recorder.directory) if service.replay_recorder is not None else None,
                "intervalSeconds": service.settings.replay_record_seconds,
                "error": service.replay_record_error,
            },
            **snapshot["runtime"],
        }

    @app.get("/api/snapshot")
    async def snapshot() -> dict[str, Any]:
        return service.latest_snapshot

    @app.get("/api/layout")
    async def dashboard_layout() -> dict[str, Any]:
        """Return the visual warehouse authority used by the 4173 canvas.

        The legacy coordinator still owns its local control-demo coordinates.
        Returning the layout separately lets the browser render the same
        physical 1920-slot warehouse used by the V5/data-package workflow,
        without claiming that legacy task generation is a V5 execution.
        """
        try:
            layout = json.loads(DEFAULT_DASHBOARD_LAYOUT.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=503, detail="dashboard layout is unavailable") from exc
        return {
            "source": "warehouse100-expanded-balanced-v1",
            "coordinateMode": "legacy-console-mapped",
            "layout": layout,
        }

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

    @app.get("/api/kg/warehouse/health")
    async def warehouse_graph_health() -> dict[str, Any]:
        return await asyncio.to_thread(service.warehouse_graph.health)

    @app.get("/api/kg/warehouse/dashboard")
    async def warehouse_graph_dashboard(dataset_id: str | None = None) -> dict[str, Any]:
        try:
            summary = await asyncio.to_thread(service.warehouse_graph.summary, dataset_id)
            health = await asyncio.to_thread(service.warehouse_graph.health)
            return {"source": "neo4j-knowledge-graph", "health": health, "summary": summary}
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/tasks")
    async def warehouse_task_list(limit: int = 8, dataset_id: str | None = None) -> list[dict[str, Any]]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.task_list, limit, dataset_id)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/tasks/{task_id}/trace")
    async def warehouse_task_trace(task_id: str, dataset_id: str | None = None,
                                   run_id: str | None = None, tick: int | None = None) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.task_execution_trace, task_id, dataset_id, run_id, tick)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/query-presets")
    async def warehouse_graph_query_presets() -> dict[str, Any]:
        return {"presets": list(service.warehouse_graph.QUERY_PRESETS), "readOnly": True, "maxRows": 200}

    @app.post("/api/kg/warehouse/query-presets/{preset}")
    async def warehouse_graph_query_preset(preset: str, request: dict[str, Any]) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                service.warehouse_graph.query_preset, preset,
                task_id=str(request.get("taskId", ""))[:160],
                run_id=str(request.get("runId", ""))[:160],
                robot_id=int(request.get("robotId", 1)), tick=int(request.get("tick", 0)),
            )
        except (ValueError, TypeError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/runs/{run_id}/robots/{robot_id}/trace")
    async def warehouse_robot_trace(run_id: str, robot_id: int, tick: int | None = None, history_limit: int = 20) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.robot_trace_context, run_id, robot_id, tick, history_limit)
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/runs/{run_id}/runtime-trace")
    async def warehouse_runtime_trace(run_id: str, tick: int | None = None, robot_id: int | None = None) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.runtime_state_trace, run_id, tick, robot_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except WarehouseGraphUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/api/kg/warehouse/runs/{run_id}/steps")
    async def warehouse_complete_steps(run_id: str, limit: int = 500) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(service.warehouse_graph.complete_steps, run_id, limit=limit)
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
        "--ray-node-ip-address",
        default=os.environ.get("RAY_NODE_IP_ADDRESS", "127.0.0.1"),
        help="single-machine Ray bind address; ignored when --ray-address is supplied",
    )
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
    parser.add_argument("--warehouse-neo4j-uri", default=os.environ.get("WAREHOUSE_NEO4J_URI", "bolt://127.0.0.1:47687"))
    parser.add_argument("--warehouse-neo4j-user", default=os.environ.get("WAREHOUSE_NEO4J_USER", "neo4j"))
    parser.add_argument("--warehouse-neo4j-database", default=os.environ.get("WAREHOUSE_NEO4J_DATABASE", "neo4j"))
    parser.add_argument("--warehouse-neo4j-dataset-id", default=os.environ.get("WAREHOUSE_NEO4J_DATASET_ID") or None)
    parser.add_argument("--warehouse-neo4j-package-dir", type=Path, default=Path(os.environ["WAREHOUSE_NEO4J_PACKAGE_DIR"]) if os.environ.get("WAREHOUSE_NEO4J_PACKAGE_DIR") else None)
    parser.add_argument(
        "--simulation-dataset-dir", type=Path,
        default=Path(os.environ["WAREHOUSE_SIMULATION_DATASET_DIR"]) if os.environ.get("WAREHOUSE_SIMULATION_DATASET_DIR") else None,
        help="CSV package directory whose tasks.csv drives 4173 dispatch",
    )
    parser.add_argument("--warehouse-neo4j-state-interval-ticks", type=int, default=int(os.environ.get("WAREHOUSE_NEO4J_STATE_INTERVAL_TICKS", "4")))
    parser.add_argument(
        "--replay-record",
        dest="replay_record",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("REPLAY_RECORD", "1") == "1",
        help="write 4173 snapshots into data/local-runs for the replay service",
    )
    parser.add_argument(
        "--replay-record-seconds",
        type=float,
        default=float(os.environ.get("REPLAY_RECORD_SECONDS", "5")),
    )
    parser.add_argument(
        "--replay-record-dir",
        type=Path,
        default=Path(os.environ.get("REPLAY_RECORD_DIR", str(PROJECT_ROOT / "data/local-runs"))),
    )
    parser.add_argument(
        "--warehouse-neo4j-write-mode",
        choices=["robot-actors", "central"],
        default=os.environ.get("WAREHOUSE_NEO4J_WRITE_MODE", "robot-actors"),
        help="robot-actors: each Ray robot writes its own graph facts; central: legacy server batch writer",
    )
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
    if args.replay_record_seconds <= 0:
        raise SystemExit("--replay-record-seconds must be positive")
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
        ray_node_ip_address=args.ray_node_ip_address,
        checkpoint_path=args.checkpoint_path,
        checkpoint_every_ticks=args.checkpoint_every_ticks,
        resume=args.resume,
        ray_recovery_attempts=args.ray_recovery_attempts,
        ray_recovery_delay=args.ray_recovery_delay,
        knowledge_backend=args.knowledge_backend,
        journal_path=args.journal_path,
        allow_code_change=args.allow_code_change,
        warehouse_neo4j_uri=args.warehouse_neo4j_uri,
        warehouse_neo4j_user=args.warehouse_neo4j_user,
        warehouse_neo4j_password=os.environ.get("WAREHOUSE_NEO4J_PASSWORD"),
        warehouse_neo4j_database=args.warehouse_neo4j_database,
        warehouse_neo4j_dataset_id=args.warehouse_neo4j_dataset_id,
        warehouse_neo4j_package_dir=args.warehouse_neo4j_package_dir,
        simulation_dataset_dir=args.simulation_dataset_dir,
        warehouse_neo4j_write_enabled=os.environ.get("WAREHOUSE_NEO4J_WRITE_ENABLED", "0") == "1",
        warehouse_neo4j_write_mode=args.warehouse_neo4j_write_mode,
        warehouse_neo4j_state_interval_ticks=args.warehouse_neo4j_state_interval_ticks,
        replay_record=args.replay_record,
        replay_record_seconds=args.replay_record_seconds,
        replay_record_dir=args.replay_record_dir,
    )
    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit("Uvicorn is not installed; install requirements.txt first") from exc
    uvicorn.run(create_app(settings), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
