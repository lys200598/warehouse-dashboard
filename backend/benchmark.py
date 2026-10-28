"""Repeatable full-context benchmark. Client identities are not physical devices."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import statistics
import platform
import os
import threading
import time

from .knowledge_graph import Neo4jMirror
from .knowledge_queries import WarehouseQueries, CATALOG_VERSION
from .knowledge import source_fingerprint


def percentile(values, fraction):
    values = sorted(values)
    if not values:
        return None
    index = (len(values)-1)*fraction
    lower = int(index)
    upper = min(lower+1, len(values)-1)
    return values[lower]+(values[upper]-values[lower])*(index-lower)


def summarize(rows, seconds):
    values = [r["ms"] for r in rows if r["ok"]]
    return {"requests": len(rows), "successes": len(values), "failures": len(rows)-len(values),
            "mean_ms": statistics.mean(values) if values else None,
            "p50_ms": percentile(values, .5), "p95_ms": percentile(values, .95), "p99_ms": percentile(values, .99),
            "max_ms": max(values) if values else None,
            "over_500ms": sum(v > 500 for v in values),
            "throughput_contexts_s": len(values)/seconds if seconds else 0, "wall_seconds": seconds}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--clients", type=int, default=100)
    parser.add_argument("--samples-per-client", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-agent-states", type=int, default=1_000_000)
    args = parser.parse_args()
    if not 1 <= args.clients <= 100 or not 1 <= args.rounds <= 20 or not 1 <= args.samples_per_client <= 100:
        parser.error("clients 1..100, rounds 1..20, samples 1..100")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if (args.output_dir/"queries.jsonl").exists():
        parser.error("output already contains a benchmark; choose a new directory")
    mirror = Neo4jMirror.from_env()
    queries = WarehouseQueries(mirror)
    try:
        count = mirror.query("MATCH (s:WHAgentState {run_id:$run}) RETURN count(s) AS n", {"run": args.run_id})[0]["n"]
        if count < args.min_agent_states:
            raise RuntimeError(f"dataset only has {count} AgentState nodes")
        run = mirror.query("MATCH (r:WHRun {run_id:$run}) RETURN properties(r) AS data", {"run":args.run_id})[0]["data"]
        seq = run["committed_seq"]
        indexes = mirror.query("SHOW INDEXES YIELD name,state WHERE name STARTS WITH 'wh' RETURN name,state", {})
        if any(row["state"] != "ONLINE" for row in indexes):
            raise RuntimeError("warehouse indexes are not ONLINE")
        # Explicit warmup: this is a warm dataset measurement, not cold start latency.
        warm_barrier = threading.Barrier(args.clients)
        def warm_client(robot):
            warm_barrier.wait(timeout=60)
            start = time.perf_counter()
            queries.execute({"run_id":args.run_id, "query_id":"context", "robot_id":robot, "as_of_seq":seq})
            return {"client_id":robot,"ms":(time.perf_counter()-start)*1000}
        with ThreadPoolExecutor(max_workers=args.clients) as pool:
            warmup_rows = list(pool.map(warm_client,range(1,args.clients+1)))
        (args.output_dir/"warmup.json").write_text(json.dumps(warmup_rows,indent=2),encoding="utf-8")
        all_rows, summaries = [], []
        for round_id in range(1, args.rounds+1):
            barrier = threading.Barrier(args.clients)
            lock = threading.Lock()
            active = 0
            peak = 0
            def client(robot):
                nonlocal active, peak
                barrier.wait(timeout=60)
                rows = []
                for sample in range(args.samples_per_client):
                    with lock:
                        active += 1
                        peak = max(peak, active)
                    start = time.perf_counter()
                    try:
                        result = queries.execute({"run_id":args.run_id, "query_id":"context", "robot_id":robot, "as_of_seq":seq})
                        if len(result["rows"]) != 1 or result["rows"][0]["data"]["robot_id"] != robot:
                            raise RuntimeError("empty or wrong-identity context")
                        error = None
                    except Exception as exc:
                        error = type(exc).__name__
                    finally:
                        with lock:
                            active -= 1
                    rows.append({"round":round_id,"client_id":robot,"sample":sample,"as_of_seq":seq,
                                 "ok":error is None,"error":error,"ms":(time.perf_counter()-start)*1000})
                return rows
            started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=args.clients) as pool:
                rows = [row for batch in pool.map(client, range(1,args.clients+1)) for row in batch]
            summary = {"round":round_id, "peak_active_context_requests":peak, **summarize(rows,time.perf_counter()-started)}
            summaries.append(summary)
            all_rows.extend(rows)
            with (args.output_dir/"queries.jsonl").open("a", encoding="utf-8") as output:
                for row in rows:
                    output.write(json.dumps(row)+"\n")
            print(json.dumps(summary), flush=True)
        report = {"catalog_version":CATALOG_VERSION,"source_sha256":source_fingerprint(), "run_id":args.run_id,
                  "agent_state_nodes":count,"clients":args.clients,"as_of_seq":seq,
                  "warmup":"one simultaneous context per identity; pool primed; warmup.json retained separately",
                  "measurement":"full materialized context (state + recent events + active task), native Bolt client",
                  "not_measured":["physical edge devices","cross-modal vector query","cold cache","third-party certification"],
                  "dataset_metadata":run,"indexes":indexes,"rounds":summaries,
                  "client_environment":{"hostname":platform.node(),"platform":platform.platform(),
                      "python":platform.python_version(),"logical_cpus":os.cpu_count()},
                  "database_components":mirror.query("CALL dbms.components() YIELD name,versions,edition RETURN name,versions,edition",{}),
                  "raw_sha256":hashlib.sha256((args.output_dir/"queries.jsonl").read_bytes()).hexdigest()}
        (args.output_dir/"report.json").write_text(json.dumps(report, ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        if any(s["failures"] for s in summaries):
            raise RuntimeError("benchmark contains failures; report retained")
    finally:
        mirror.close()


if __name__ == "__main__":
    main()
