from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import tempfile
from typing import Any


def write_checkpoint(path: str | Path, payload: dict[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        text=True,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            envelope = {"format": "warehouse-checkpoint-v2", "sha256": hashlib.sha256(raw.encode()).hexdigest(), "payload": payload}
            json.dump(envelope, stream, ensure_ascii=False, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
        if os.name != "nt":
            directory = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def read_checkpoint(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    if not isinstance(payload, dict):
        raise ValueError("checkpoint root must be an object")
    if payload.get("format") == "warehouse-checkpoint-v2":
        raw = json.dumps(payload["payload"], ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if hashlib.sha256(raw.encode()).hexdigest() != payload.get("sha256"):
            raise ValueError("checkpoint SHA256 mismatch")
        return payload["payload"]
    return payload
