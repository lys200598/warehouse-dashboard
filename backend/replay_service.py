"""Standalone read-only multi-run playback service.

The service discovers recorded run directories, exposes a run catalog and the
same frame/status/scene endpoints used by the existing wide playback page.
It never advances a simulation and never writes a checkpoint or graph.
"""
from __future__ import annotations

import argparse
import json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sqlite3
import time
import zlib
from urllib.parse import parse_qs, urlsplit

from .checkpoint import read_checkpoint
from .wide.graph import WideMirror
from .wide.queries import WideQueries


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOTS = (ROOT / "data/local-runs", ROOT / "data/runs")
DEFAULT_DIST = ROOT / "dist"
DEFAULT_PORT = 14185


class RunCatalog:
    """Discover immutable run directories beneath the configured roots."""

    def __init__(self, roots):
        self.roots = [Path(root) for root in roots]
        self.runs = []

    def refresh(self):
        found = {}
        for root in self.roots:
            if not root.is_dir():
                continue
            for directory in sorted(root.iterdir()):
                if not directory.is_dir():
                    continue
                if not all((directory / name).is_file() for name in
                           ("latest.json", "frames.sqlite", "checkpoint.json")):
                    continue
                try:
                    found[directory.name] = self._metadata(directory)
                except Exception as exc:
                    found[directory.name] = {
                        "id": directory.name,
                        "directory": str(directory),
                        "error": f"{type(exc).__name__}: {exc}",
                        "modified_at": 0,
                    }
        self.runs = sorted(
            found.values(),
            key=lambda item: (not item.get("current"), -item.get("modified_at", 0)),
        )
        return self.runs

    def _metadata(self, directory):
        latest = read_checkpoint(directory / "latest.json")
        results = {}
        results_path = directory / "results.json"
        if results_path.is_file():
            try:
                value = json.loads(results_path.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    results = value
            except (OSError, UnicodeError, json.JSONDecodeError):
                results = {}
        manifest = {}
        manifest_path = directory / "live-manifest.json"
        if manifest_path.is_file():
            try:
                value = json.loads(manifest_path.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    manifest = value
            except (OSError, UnicodeError, json.JSONDecodeError):
                manifest = {}
        with sqlite3.connect(f"file:{directory / 'frames.sqlite'}?mode=ro", uri=True, timeout=5) as db:
            frame_count, first_second, latest_second = db.execute(
                "SELECT COUNT(*), MIN(second), MAX(second) FROM frames"
            ).fetchone()
        run_id = str(latest.get("runId") or results.get("run_id") or directory.name)
        modified_at = (directory / "frames.sqlite").stat().st_mtime
        running = bool(latest.get("running"))
        metrics = latest.get("metrics") if isinstance(latest.get("metrics"), dict) else {}
        total_tasks = (
            results.get("tasks_total")
            or latest.get("totalTasks")
            or manifest.get("tasks_total")
        )
        completed = results.get("completed")
        if completed is None:
            completed = metrics.get("completed")
        unreleased = latest.get("unreleased")
        released = results.get("released")
        if released is None and total_tasks is not None and unreleased is not None:
            released = max(0, int(total_tasks) - int(unreleased))
        # A run is "current" when it is still being written or was active in
        # the last two minutes. Historical runs keep their immutable status.
        current = (running or time.time() - modified_at <= 120) and results.get("status") not in {"complete", "failed"}
        return {
            "id": directory.name,
            "run_id": run_id,
            "directory": str(directory),
            "status": results.get("status") or ("running" if running else "recorded"),
            "running": running,
            "current": current,
            "dataset_id": str(
                manifest.get("dataset_id")
                or latest.get("dataset_id")
                or latest.get("datasetId")
                or ""
            ) or None,
            "elapsed": latest.get("elapsed"),
            "wall_seconds": results.get("wall_seconds"),
            "tasks_total": total_tasks,
            "completed": completed,
            "released": released,
            "unfinished": results.get("unfinished"),
            "frame_count": int(frame_count or 0),
            "first_second": first_second,
            "latest_second": latest_second,
            "modified_at": modified_at,
        }

    def resolve(self, run):
        if not self.runs:
            self.refresh()
        matches = [item for item in self.runs if item.get("id") == run]
        if not matches:
            matches = [item for item in self.runs if item.get("run_id") == run]
        if not matches:
            self.refresh()
            matches = [item for item in self.runs if item.get("id") == run]
        if len(matches) != 1 or matches[0].get("error"):
            raise KeyError(run)
        return Path(matches[0]["directory"])


def frame_at(directory, second):
    latest = read_checkpoint(Path(directory) / "latest.json")
    latest_second = max(0, int(latest.get("elapsed", 0)))
    wanted = max(0, min(latest_second, int(second)))
    with sqlite3.connect(f"file:{Path(directory) / 'frames.sqlite'}?mode=ro", uri=True, timeout=10) as db:
        row = db.execute(
            "SELECT payload FROM frames WHERE second<=? ORDER BY second DESC LIMIT 1",
            (wanted,),
        ).fetchone()
    if not row:
        raise KeyError("frame")
    return json.loads(zlib.decompress(row[0]))


def status_for(directory):
    directory = Path(directory)
    latest = read_checkpoint(directory / "latest.json")
    results_path = directory / "results.json"
    results = None
    if results_path.is_file():
        try:
            results = json.loads(results_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            results = None
    return {
        "run_id": latest.get("runId"),
        "latest_second": latest.get("elapsed"),
        "knowledge": latest.get("knowledge"),
        "results": results,
        "running": latest.get("running"),
    }


def scene_for(directory):
    return read_checkpoint(Path(directory) / "checkpoint.json")["run_metadata"]["scene"]


def serve(args):
    catalog = RunCatalog(args.runs_root)
    catalog.refresh()
    dist = Path(args.dist).resolve()
    queries = None
    mirror = None
    if args.neo4j:
        try:
            mirror = WideMirror.from_env()
            queries = WideQueries(mirror)
        except Exception:
            queries = None

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, directory=str(dist), **kw)

        def json(self, value, status=200):
            raw = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def run_directory(self, query):
            run = (query.get("run", [""])[0] or "").strip()
            if not run:
                raise ValueError("run query parameter is required")
            return catalog.resolve(run)

        def do_GET(self):
            request = urlsplit(self.path)
            query = parse_qs(request.query)
            try:
                if request.path == "/":
                    self.send_response(302)
                    self.send_header("Location", "/wide.html")
                    self.end_headers()
                    return
                if request.path == "/api/runs":
                    return self.json(catalog.refresh())
                if request.path in {"/api/status", "/api/health"}:
                    return self.json(status_for(self.run_directory(query)))
                if request.path == "/api/scene":
                    return self.json(scene_for(self.run_directory(query)))
                if request.path == "/api/frame":
                    latest = status_for(self.run_directory(query)).get("latest_second") or 0
                    wanted = max(0, min(int(latest), int(query.get("second", [str(latest)])[0])))
                    return self.json(frame_at(self.run_directory(query), wanted))
                if request.path.startswith("/api/knowledge/"):
                    if queries is None:
                        return self.json({"error": "graph not configured"}, 503)
                    directory = self.run_directory(query)
                    latest = read_checkpoint(directory / "latest.json")
                    run_id = str(latest["runId"])
                    knowledge = latest.get("knowledge") or {}
                    cutoff = int(query.get("seq", [str(knowledge.get("mirrored_seq", 0))])[0])
                    if cutoff > int(knowledge.get("mirrored_seq", 0)):
                        raise ValueError("cutoff exceeds this run playback watermark")
                    identity = query.get("id", [""])[0]
                    limit = int(query.get("limit", ["200"])[0])
                    cursor = int(query.get("after_event_seq", ["0"])[0])
                    if request.path == "/api/knowledge/goods":
                        return self.json(queries.goods_history(run_id, identity, cutoff, limit, cursor))
                    if request.path == "/api/knowledge/robot":
                        return self.json(queries.robot_context(run_id, int(identity), cutoff))
                    if request.path == "/api/knowledge/cooperation":
                        return self.json(queries.cooperation_history(
                            run_id, query.get("task", [None])[0], cutoff, min(limit, 100), cursor))
                    return self.json({"error": "unknown query"}, 404)
                if request.path.startswith("/api/"):
                    return self.json({"error": "unknown endpoint"}, 404)
                super().do_GET()
            except KeyError:
                self.json({"error": "unknown or incomplete run"}, 404)
            except (ValueError, TypeError):
                self.json({"error": "invalid request"}, 400)
            except Exception:
                self.json({"error": "query or snapshot unavailable"}, 503)

        def do_POST(self):
            self.json({"error": "read-only endpoint"}, 405)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Multi-run replay service on http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        if mirror is not None:
            mirror.close()


def parser():
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--host", default="127.0.0.1")
    value.add_argument("--port", type=int, default=DEFAULT_PORT)
    value.add_argument("--runs-root", action="append", type=Path,
                       help="repeatable run root; default data/local-runs and data/runs")
    value.add_argument("--dist", type=Path, default=DEFAULT_DIST)
    value.add_argument("--neo4j", action="store_true", help="enable read-only knowledge queries")
    return value


def main(argv=None):
    args = parser().parse_args(argv)
    args.runs_root = args.runs_root or list(DEFAULT_ROOTS)
    serve(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
