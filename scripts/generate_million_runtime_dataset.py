"""Generate deterministic AgentState/Event replay data for graph scale tests."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def generate(output: Path, robots: int, steps: int, write: bool) -> dict[str, int | str]:
    if robots < 1 or steps < 1:
        raise ValueError("robots and steps must be positive")
    states = robots * steps
    result = {"run_id": "scale-replay-100x10000", "robots": robots, "steps": steps, "agent_states": states, "runtime_events": states // 20}
    if not write:
        return {"mode": "dry-run", **result}
    output.mkdir(parents=True, exist_ok=True)
    state_path, event_path = output / "agent_states.jsonl", output / "runtime_events.jsonl"
    with state_path.open("w", encoding="utf-8") as states_file, event_path.open("w", encoding="utf-8") as events_file:
        for tick in range(steps):
            for robot_id in range(1, robots + 1):
                task_number = tick // 80 * robots + robot_id
                row = {"id": f"scale-replay-100x10000:robot:{robot_id}:state:{tick}", "run_id": "scale-replay-100x10000", "robot_id": robot_id, "tick": tick, "stage": ("charging" if robot_id % 17 == 0 else "toRack" if tick % 80 < 55 else "idle"), "battery": round(25 + ((robot_id * 7 + tick) % 75), 2), "x": round(1.0 + (robot_id % 20) * 1.45, 3), "y": round(1.0 + ((robot_id // 20) * 3 + tick % 30) * .2, 3), "task_id": f"T-SCALE-{task_number:07d}"}
                states_file.write(json.dumps(row, separators=(",", ":")) + "\n")
                if tick % 20 == 0:
                    event = {"id": f"scale-replay-100x10000:event:{tick}:{robot_id}", "run_id": row["run_id"], "robot_id": robot_id, "tick": tick, "type": "state_sampled", "task_id": row["task_id"]}
                    events_file.write(json.dumps(event, separators=(",", ":")) + "\n")
    return {"mode": "write", **result, "output": str(output)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create million-scale runtime replay JSONL.")
    parser.add_argument("--output", type=Path, default=Path("data/generated/million-runtime-replay"))
    parser.add_argument("--robots", type=int, default=100)
    parser.add_argument("--steps", type=int, default=10_000)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    print(json.dumps(generate(args.output, args.robots, args.steps, args.write), ensure_ascii=False, indent=2))
