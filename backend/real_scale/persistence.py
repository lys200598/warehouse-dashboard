"""Single-encode atomic v2 envelope; old read_checkpoint remains authoritative."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any


VERSION = 'canonical-single-encode-atomic-v1'


def write_checkpoint(path: str | Path, payload: dict[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Exactly the old reader's canonical payload and SHA contract. Encoding the
    # same payload a second time with json.dump is unnecessary: embed these
    # already valid UTF-8 JSON bytes directly, without quoting or interpolation.
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                     separators=(',', ':'), allow_nan=False).encode('utf-8')
    checksum = hashlib.sha256(raw).hexdigest().encode('ascii')
    envelope = b'{"format":"warehouse-checkpoint-v2","sha256":"' + checksum + b'","payload":' + raw + b'}'
    descriptor, temporary_name = tempfile.mkstemp(prefix=f'.{destination.name}.',
                                                 suffix='.tmp', dir=destination.parent)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(envelope)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
        if os.name != 'nt':
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
