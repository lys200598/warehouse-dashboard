"""Opt-in resource projection in the SAME transaction as real source commits."""
from .real_scale.writer import RealMirror
from .resource_graph import ensure_schema, validate_forward_batches, apply_resources


class ResourceMirror(RealMirror):
    def ensure_schema(self):
        super().ensure_schema()
        with self.driver.session(database=self.database) as session:
            ensure_schema(session)

    @staticmethod
    def _apply(tx, batches):
        if not batches:
            return
        run = batches[0]['run_id']
        row = tx.run('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq', run=run).single()
        head = int(row['seq'] or 0) if row else 0
        forward = [batch for batch in batches if batch['seq'] > head]
        # Validate IDs, layout and inputs before the parent writes anything.
        prepared = validate_forward_batches(forward) if forward else None
        RealMirror._apply(tx, batches)
        if forward:
            apply_resources(tx, forward, prepared=prepared)
