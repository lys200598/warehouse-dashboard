from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import os
from pathlib import Path
import time
from typing import Any

from .checkpoint import read_checkpoint, write_checkpoint
from .runtime import build_runtime
from .simulation import SimulationConfig, SimulationCoordinator


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


class SimulationService:
    def __init__(self, settings: ServerSettings):
        self.settings = settings
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
            ),
        )
        self.lock = asyncio.Lock()
        self.subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self.runner: asyncio.Task[None] | None = None
        self.stopping = False
        self.latest_snapshot = self.coordinator.snapshot()
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
        else:
            self._save_checkpoint_locked()
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
            self._save_checkpoint_locked()
            self.coordinator.close()

    async def control(self, action: str, value: float | None = None) -> dict[str, Any]:
        async with self.lock:
            snapshot = self.coordinator.control(action, value)
            self.latest_snapshot = snapshot
            if action == "reset":
                self._save_checkpoint_locked()
        await self.publish(snapshot)
        return snapshot

    async def inject_priority_task(self) -> dict[str, Any]:
        async with self.lock:
            result = self.coordinator.inject_priority_task()
            self.latest_snapshot = self.coordinator.snapshot()
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

    async def _run_loop(self) -> None:
        while not self.stopping:
            started = time.monotonic()
            if self.coordinator.running:
                try:
                    async with self.lock:
                        snapshot = self.coordinator.step_once()
                        self.latest_snapshot = snapshot
                        every = self.settings.checkpoint_every_ticks
                        if every > 0 and self.coordinator.tick % every == 0:
                            self._save_checkpoint_locked()
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
            **snapshot["runtime"],
        }

    @app.get("/api/snapshot")
    async def snapshot() -> dict[str, Any]:
        return service.latest_snapshot

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
    )
    try:
        import uvicorn
    except ImportError as exc:
        raise SystemExit("Uvicorn is not installed; install requirements.txt first") from exc
    uvicorn.run(create_app(settings), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
