"""Bounded, browser-independent simulation and scale-data generator.

--backend local is explicitly offline generation, not evidence of Ray execution.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import time

from .checkpoint import read_checkpoint, write_checkpoint
from .knowledge import build_knowledge_store, source_fingerprint
from .runtime import build_runtime
from .simulation import SimulationConfig, SimulationCoordinator


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["local", "ray"], default="local")
    parser.add_argument("--robot-count", type=int, default=100)
    parser.add_argument("--max-active-robots", type=int, default=20)
    parser.add_argument('--inbound-layout',choices=['edge','corner'],default='edge')
    parser.add_argument('--flow-rate',type=float,default=48.)
    parser.add_argument('--initial-occupancy',type=float,default=.52)
    parser.add_argument('--pause-on-finish',action='store_true')
    parser.add_argument("--seed", type=int, default=20261013)
    parser.add_argument("--step-seconds", type=float, default=0.25)
    parser.add_argument("--steps", type=int, default=100)
    parser.add_argument("--max-wall-seconds", type=float, default=600)
    parser.add_argument("--ray-address")
    parser.add_argument("--ray-actor-cpus", type=float, default=0.1)
    parser.add_argument("--knowledge-backend", choices=["journal", "neo4j"], default="journal")
    parser.add_argument("--journal-path", type=Path, required=True)
    parser.add_argument("--checkpoint-path", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--allow-code-change", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--flush-every", type=int, default=1)
    args = parser.parse_args()
    if args.steps < 1 or args.max_wall_seconds <= 0 or not 1 <= args.flush_every <= 100:
        parser.error("positive steps/time budget and flush-every 1..100 required")
    if not args.resume and args.checkpoint_path.exists():
        parser.error("checkpoint already exists; use --resume or a fresh path")
    store = build_knowledge_store(args.knowledge_backend, args.journal_path)
    store.flush_every = args.flush_every
    runtime = build_runtime(args.backend, ray_address=args.ray_address, ray_actor_cpus=args.ray_actor_cpus)
    config = SimulationConfig(robot_count=args.robot_count, max_active_robots=args.max_active_robots,
                              seed=args.seed, step_seconds=args.step_seconds,
                              flow_rate=args.flow_rate,inbound_layout=args.inbound_layout,initial_occupancy=args.initial_occupancy)
    sim = None
    started = time.monotonic()
    reason = "step_limit"
    try:
        sim = SimulationCoordinator(runtime, config, knowledge_store=store, initialize=not args.resume,
            run_id=args.run_id, metadata={"dataset_origin": "offline-local-simulation" if args.backend == "local" else "ray-simulation"})
        if args.resume:
            payload = store.recovery_payload(read_checkpoint(args.checkpoint_path))
            if payload.get("run_metadata", {}).get("source_sha256") != source_fingerprint() and not args.allow_code_change:
                raise ValueError("source changed since checkpoint")
            if payload["config"] != asdict(config):
                raise ValueError("resume configuration differs")
            sim.restore_checkpoint(payload)
            store.flush(sim.run_id, batch_size=args.flush_every)
        first_tick = sim.tick
        for index in range(args.steps):
            if time.monotonic()-started > args.max_wall_seconds:
                reason = "wall_budget"
                break
            sim.step_once()
            if (index+1) % 100 == 0:
                write_checkpoint(args.checkpoint_path, sim.checkpoint_payload())
                print(json.dumps({"run_id": sim.run_id, "tick": sim.tick, "completed": sim.completed,
                                  "wall_seconds": round(time.monotonic()-started, 2), "knowledge": sim.knowledge_status()}), flush=True)
        if args.pause_on_finish:
            sim.control('pause')
        store.flush(sim.run_id, batch_size=args.flush_every)
        write_checkpoint(args.checkpoint_path, sim.checkpoint_payload())
        result = {"run_id": sim.run_id, "backend": args.backend, "dataset_origin": sim.run_metadata["dataset_origin"],
                  "first_tick": first_tick, "last_tick": sim.tick, "completed": sim.completed,
                  "worker_pids": sorted({p["worker_pid"] for p in sim.runtime.snapshots()}),
                  "wall_seconds": round(time.monotonic()-started, 3), "reason": reason,
                  "source_sha256": sim.run_metadata.get("source_sha256"), "current_disk_source_sha256": source_fingerprint(),
                  "store_timings":store.timings, "config": asdict(config), "knowledge": sim.knowledge_status()}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False), flush=True)
        if reason != "step_limit" or result["knowledge"]["lag"]:
            raise RuntimeError("bounded experiment incomplete; inspect progress and checkpoint")
    finally:
        if sim:
            sim.close()
        else:
            runtime.close()
        store.close()


if __name__ == "__main__":
    main()
