"""Canonical graph commit hook, without modifying the legacy simulation runner."""
import json
from pathlib import Path

from .fleet import AgentFleet
from .service import Neo4jBackend


def agent_mirror_class(base_class, *, output_dir, query_interval=30,
                       observation_interval=30, timeout=30., startup_timeout=120.):
    """Wrap an existing mirror class; usable by current runner/live factories.

    base.apply_many commits the original source transaction first. Only then
    are its exact per-robot state samples delivered to authenticated agent
    processes. Exceptions propagate to JournalKnowledgeStore.last_error, so
    existing runners pause/fail before another physical step.
    """
    output_dir = Path(output_dir)

    class AgentMirror(base_class):
        agent_fleet = None

        def apply_many(self, batches):
            super().apply_many(batches)
            for batch in batches:
                if self.agent_fleet is None:
                    backend = Neo4jBackend.from_env()
                    self.agent_fleet = AgentFleet(run_id=batch['run_id'],
                        robot_ids=[row['robot_id'] for row in batch['robots']], backend=backend,
                        output_dir=output_dir, query_interval=query_interval,
                        observation_interval=observation_interval, timeout=timeout,
                        startup_timeout=startup_timeout)
                self.agent_fleet.publish({row['robot_id']: row for row in batch['robots']},
                                         batch['sim_time'], batch['seq'])

        def close(self):
            try:
                if self.agent_fleet is not None:
                    try:
                        value = self.agent_fleet.status()
                        output_dir.mkdir(parents=True, exist_ok=True)
                        (output_dir/'agent-final-status.json').write_text(json.dumps(value, indent=2), encoding='utf-8')
                    finally:
                        # Evidence I/O can fail independently of process
                        # cleanup. Never leak the owned fleet on a full disk.
                        self.agent_fleet.close()
            finally:
                super().close()

    return AgentMirror
