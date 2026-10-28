"""Durable commit journal; Neo4j is an idempotent, watermark-tracked read model.

The journal atomically stores a committed batch and its recovery checkpoint.
The UI's bounded recent-events list is never used as the event archive.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any
import zlib


class KnowledgeError(RuntimeError):
    pass


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def checkpoint_identity(value):
    data = {k: v for k, v in value.items() if k != "saved_at"}
    data["robots"] = [{k: v for k, v in r.items() if k != "worker_pid"} for r in value["robots"]]
    return digest(data)


def make_batch(checkpoint: dict, events: list[dict], previous: dict | None = None) -> dict:
    """Only changed task/slot versions; all robot states at each committed step."""
    run = checkpoint["run_id"]
    seq = checkpoint["knowledge_seq"]
    previous = previous or {}
    before_tasks = previous.get("tasks", {})
    before_slots = {s["slot_id"]: s for r in previous.get("racks", []) for s in r["slots"]}
    tasks = [deepcopy(t) for key, t in checkpoint["tasks"].items() if before_tasks.get(key) != t]
    slots = [deepcopy(s) for r in checkpoint["racks"] for s in r["slots"] if before_slots.get(s["slot_id"]) != s]
    robots = [deepcopy(p["state"]) for p in checkpoint["robots"]]
    return {
        "schema_version": 1, "run_id": run, "seq": seq,
        "tick": checkpoint["tick"], "sim_time": checkpoint["elapsed"],
        "completed": checkpoint["completed"], "config": checkpoint["config"],
        "running": checkpoint["running"], "speed": checkpoint["speed"],
        "flow_rate": checkpoint["config"]["flow_rate"],
        "metadata": checkpoint.get("run_metadata", {}),
        "racks": [{k: v for k, v in r.items() if k != "slots"} for r in checkpoint["racks"]] if not previous else [],
        "robots": robots, "tasks": tasks, "slots": slots, "events": deepcopy(events),
    }


class JournalKnowledgeStore:
    """One writer; bounded replay batches; full recovery checkpoint is transactional."""
    def __init__(self, path: str | Path, mirror=None, flush_every=1):
        self.path = str(path)
        if self.path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS commits (
            run_id TEXT NOT NULL, seq INTEGER NOT NULL, tick INTEGER NOT NULL,
            checksum TEXT NOT NULL, batch BLOB NOT NULL, PRIMARY KEY(run_id,seq));
          CREATE INDEX IF NOT EXISTS commits_tick ON commits(run_id,tick,seq);
          CREATE TABLE IF NOT EXISTS heads (
            run_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, checksum TEXT NOT NULL,
            checkpoint BLOB NOT NULL, mirrored_seq INTEGER NOT NULL DEFAULT 0);
        """)
        self.lock = threading.RLock()
        self.mirror = mirror
        self.flush_every = max(1, int(flush_every))
        self.last_error: str | None = None
        self.timings = {"journal_seconds": 0.0, "mirror_seconds": 0.0, "commits": 0}

    def latest_checkpoint(self, run_id: str) -> dict | None:
        with self.lock:
            row = self.db.execute("SELECT checksum,checkpoint FROM heads WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                return None
            raw = zlib.decompress(row[1])
            if hashlib.sha256(raw).hexdigest() != row[0]:
                raise KnowledgeError("journal checkpoint checksum mismatch")
            return json.loads(raw)

    def recovery_payload(self, checkpoint: dict) -> dict:
        latest = self.latest_checkpoint(checkpoint["run_id"])
        if latest is None:
            if checkpoint.get("knowledge_seq", 0):
                raise KnowledgeError("checkpoint references a missing journal run")
            return checkpoint
        static = lambda config: {k:v for k,v in config.items() if k != "flow_rate"}
        if static(latest["config"]) != static(checkpoint["config"]):
            raise KnowledgeError("journal and checkpoint configuration mismatch")
        if latest["knowledge_seq"] < checkpoint.get("knowledge_seq", 0):
            raise KnowledgeError("checkpoint is ahead of durable journal")
        if latest.get("run_metadata") != checkpoint.get("run_metadata"):
            raise KnowledgeError("journal and checkpoint run metadata mismatch")
        return latest

    def append(self, checkpoint: dict, events: list[dict]) -> None:
        with self.lock:
            started = time.perf_counter()
            try:
                previous = self.latest_checkpoint(checkpoint["run_id"])
                seq = checkpoint["knowledge_seq"]
                if previous and seq == previous["knowledge_seq"]:
                    if checkpoint_identity(previous) != checkpoint_identity(checkpoint):
                        raise KnowledgeError("conflicting duplicate commit")
                    self.flush(checkpoint["run_id"])
                    return
                if seq != (previous["knowledge_seq"] if previous else 0) + 1:
                    raise KnowledgeError("non-contiguous journal sequence")
                batch = make_batch(checkpoint, events, previous)
                raw = canonical(checkpoint)
                with self.db:
                    self.db.execute("INSERT INTO commits VALUES (?,?,?,?,?)", (
                        checkpoint["run_id"], seq, checkpoint["tick"], digest(batch), zlib.compress(canonical(batch))))
                    self.db.execute("""INSERT INTO heads(run_id,seq,checksum,checkpoint) VALUES(?,?,?,?)
                      ON CONFLICT(run_id) DO UPDATE SET seq=excluded.seq,checksum=excluded.checksum,checkpoint=excluded.checkpoint""",
                        (checkpoint["run_id"], seq, hashlib.sha256(raw).hexdigest(), zlib.compress(raw)))
            except (sqlite3.Error, OSError, ValueError) as exc:
                raise KnowledgeError(f"durable knowledge commit failed: {type(exc).__name__}") from exc
            self.timings["journal_seconds"] += time.perf_counter()-started
            self.timings["commits"] += 1
            if previous is None or seq % self.flush_every == 0:
                self.flush(checkpoint["run_id"], batch_size=self.flush_every)

    def flush(self, run_id: str, batch_size: int = 10) -> None:
        if self.mirror is None:
            return
        with self.lock:
            head = self.db.execute("SELECT mirrored_seq FROM heads WHERE run_id=?", (run_id,)).fetchone()
            if head is None:
                return
            try:
                started = time.perf_counter()
                if hasattr(self.mirror, "committed_point"):
                    point = self.mirror.committed_point(run_id)
                    graph_seq = int(point["seq"] or 0) if point else 0
                    durable_seq = self.db.execute("SELECT seq FROM heads WHERE run_id=?",(run_id,)).fetchone()[0]
                    if graph_seq > durable_seq:
                        raise KnowledgeError("graph is ahead of this journal; refusing foreign run")
                    if graph_seq:
                        expected = self.db.execute("SELECT checksum FROM commits WHERE run_id=? AND seq=?",(run_id,graph_seq)).fetchone()
                        if expected is None or expected[0] != point["checksum"]:
                            raise KnowledgeError("graph and journal commit hashes differ")
                    if graph_seq < head[0]:
                        with self.db:
                            self.db.execute("UPDATE heads SET mirrored_seq=? WHERE run_id=?",(graph_seq,run_id))
                        head = (graph_seq,)
                while True:
                    rows = self.db.execute("SELECT seq,checksum,batch FROM commits WHERE run_id=? AND seq>? ORDER BY seq LIMIT ?", (run_id, head[0], batch_size)).fetchall()
                    if not rows:
                        break
                    batches = [json.loads(zlib.decompress(r[2])) for r in rows]
                    if any(digest(b) != r[1] for b, r in zip(batches, rows)):
                        raise KnowledgeError("journal batch checksum mismatch")
                    self.mirror.apply_many(batches)
                    with self.db:
                        self.db.execute("UPDATE heads SET mirrored_seq=? WHERE run_id=?", (rows[-1][0], run_id))
                    head = (rows[-1][0],)
                self.last_error = None
            except Exception as exc:
                # The durable source is intact. Expose lag rather than rollback business state.
                self.last_error = f"graph mirror delayed: {type(exc).__name__}"
                import logging
                logging.getLogger(__name__).exception("warehouse graph mirror delayed; durable journal retained")
            finally:
                self.timings["mirror_seconds"] += time.perf_counter()-started

    def status(self, run_id: str) -> dict:
        with self.lock:
            row = self.db.execute("SELECT seq,mirrored_seq FROM heads WHERE run_id=?", (run_id,)).fetchone()
            return {"backend": "neo4j" if self.mirror else "journal", "run_id": run_id,
                    "durable_seq": row[0] if row else 0, "mirrored_seq": row[1] if row else 0,
                    "lag": row[0]-row[1] if row and self.mirror else 0, "last_error": self.last_error}

    def close(self):
        with self.lock:
            self.db.close()
            if self.mirror:
                self.mirror.close()


def source_fingerprint() -> str:
    root = Path(__file__).parent
    return digest({p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.glob("*.py"))})


def build_knowledge_store(backend: str, path: str | Path):
    if backend == "none":
        return None
    if backend not in {"journal", "neo4j"}:
        raise ValueError("knowledge backend must be none, journal or neo4j")
    if backend == "neo4j":
        from .knowledge_graph import Neo4jMirror
        return JournalKnowledgeStore(path, Neo4jMirror.from_env())
    return JournalKnowledgeStore(path)
