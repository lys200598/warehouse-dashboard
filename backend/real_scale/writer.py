"""Five-second real journal/graph writer over the frozen selected V5 policy."""
from copy import deepcopy
from functools import partial
import json
from pathlib import Path

from ..checkpoint import read_checkpoint
from ..knowledge import digest
from .runner import build_parser, run
from ..scheduling_v5.sim import CandidateSimulation, Config, execution_identity
from ..wide.graph import WideMirror, audit_wide
from . import VERSION, DIMENSIONS, VECTOR_INDEX, SOURCE_KIND, VECTOR_KIND, real_vector, positive_integer


SELECTED_POLICY = 'port-guard'
SELECTED_PLANNER = 'original-sipp'
SELECTED_PARAMETERS = {'wait_limit': 300, 'deadline_guard': 120.,
                       'port_pressure_weight': 1., 'port_queue_cap': 6}


def real_identity(commit_seconds=5):
    positive_integer(commit_seconds, 'real commit cadence', 300)
    value = {'version': VERSION, 'source_kind': SOURCE_KIND, 'commit_seconds': commit_seconds,
             'vector_kind': VECTOR_KIND, 'dimensions': DIMENSIONS,
             'scheduler': execution_identity(SELECTED_POLICY, SELECTED_PARAMETERS, SELECTED_PLANNER)}
    value['execution_sha256'] = digest(value)
    return value


class RealSimulation(CandidateSimulation):
    def __init__(self, scene_path, dataset, run_id, config=None, checkpoint=None, *, commit_seconds=5):
        self.real_identity = real_identity(commit_seconds)
        super().__init__(scene_path, dataset, run_id, config or Config(), checkpoint=None,
                         policy=SELECTED_POLICY, policy_parameters=SELECTED_PARAMETERS,
                         planner=SELECTED_PLANNER)
        self.run_metadata.update(backend=VERSION, real_scale=deepcopy(self.real_identity),
            sampling=f'all actual business events; complete 100-agent states every {commit_seconds}s plus disturbance/final boundaries',
            execution_code_sha256=self.real_identity['execution_sha256'])
        if checkpoint is not None:
            self.restore(checkpoint)

    def checkpoint(self):
        value = super().checkpoint()
        value['real_scale'] = {'identity': deepcopy(self.real_identity)}
        return value

    def restore(self, state):
        if state.get('real_scale', {}).get('identity') != self.real_identity:
            raise ValueError('real sampling/version/source identity mismatch; no implicit migration')
        super().restore(state)

    def execution_status(self):
        value = super().execution_status()
        value['scheduler_identity'] = value['identity']
        value['identity'] = deepcopy(self.real_identity)
        return value


class RealMirror(WideMirror):
    def ensure_schema(self):
        super().ensure_schema()
        with self.driver.session(database=self.database) as session:
            for query in [
                'CREATE INDEX whreal_goods_event IF NOT EXISTS FOR (n:WHGoodsState) ON (n.run_id,n.goods_id,n.event_seq)',
                'CREATE INDEX whreal_event_task_order IF NOT EXISTS FOR (n:WHEvent) ON (n.run_id,n.task_id,n.event_seq)',
                f'''CREATE VECTOR INDEX {VECTOR_INDEX} IF NOT EXISTS FOR (n:WHRealState)
                    ON (n.real_embedding) OPTIONS {{indexConfig: {{`vector.dimensions`: {DIMENSIONS},
                    `vector.similarity_function`: 'cosine',`vector.quantization.enabled`: false}}}}''',
                'CALL db.awaitIndexes(120)',
            ]:
                session.run(query).consume()

    @staticmethod
    def _apply(tx, batches):
        if not batches:
            return
        # Validate all source rows before the first write, not after parent head advancement.
        for batch in batches:
            ids = [row.get('robot_id') for row in batch['robots']]
            if len(ids) != 100 or set(ids) != set(range(1, 101)) or any(isinstance(v, bool) for v in ids):
                raise ValueError('real history requires all 100 unique actual robot states per commit')
            for state in batch['robots']:
                real_vector(state, batch['seq'])
        run_id = batches[0]['run_id']
        head = tx.run('MATCH (r:WHRun {run_id:$run}) RETURN r.committed_seq AS seq', run=run_id).single()
        head = int(head['seq'] or 0) if head else 0
        WideMirror._apply(tx, batches)
        forward = [batch for batch in batches if batch['seq'] > head]
        if not forward:
            return
        rows = [{'key': f"{run_id}:{b['seq']}:{state['robot_id']}",
                 'vector': real_vector(state, b['seq']), 'sha256': digest(state)}
                for b in forward for state in b['robots']]
        # Same transaction as robot/task/event/custody writes and committed_seq.
        # Do NOT set the old `embedding` property or old WHScaleArchive label.
        tx.run('''UNWIND $rows AS p MATCH (a:WHAgentState {key:p.key})
            SET a:WHRealState,a.real_embedding=p.vector,a.real_payload_sha256=p.sha256''', rows=rows).consume()
        tx.run('''MATCH (r:WHRun {run_id:$run})
            SET r.real_scale_version=$version,r.real_source_kind=$source,r.real_source_run=$run,
                r.real_vector_kind=$vector_kind,r.real_dimensions=$dims,
                r.real_last_seq=$seq,r.real_last_batch_sha256=$checksum''',
            run=run_id, version=VERSION, source=SOURCE_KIND, vector_kind=VECTOR_KIND,
            dims=DIMENSIONS, seq=forward[-1]['seq'], checksum=digest(forward[-1])).consume()


def audit_real(store, sim):
    result = audit_wide(store, sim, expected_handoffs=sim.metrics['handoffs'])
    if result is None:
        return None
    rows = store.mirror.query('''MATCH (a:WHRealState {run_id:$run})
        RETURN count(a) AS states,count(a.real_embedding) AS vectors,
            count(a.real_payload_sha256) AS payload_hashes''', {'run': sim.run_id}, timeout=120)
    expected = 100 * sim.knowledge_seq
    if len(rows) != 1 or any(rows[0][name] != expected for name in ('states', 'vectors', 'payload_hashes')):
        raise AssertionError('actual committed source/vector counts disagree with runtime')
    result['real_history'] = rows[0]
    result['real_history']['scope'] = SOURCE_KIND
    return result


def main(argv=None):
    parser = build_parser()
    parser.set_defaults(commit_seconds=5)
    parser.add_argument('--policy', choices=[SELECTED_POLICY], default=SELECTED_POLICY)
    parser.add_argument('--planner', choices=[SELECTED_PLANNER], default=SELECTED_PLANNER)
    parser.add_argument('--params-json', '--policy-json', dest='params_json')
    args = parser.parse_args(argv)
    if args.params_json and json.loads(Path(args.params_json).read_text(encoding='utf-8')) != SELECTED_PARAMETERS:
        raise ValueError('real writer requires the frozen selected policy parameters')
    identity = real_identity(args.commit_seconds)
    if args.resume:
        state = read_checkpoint(Path(args.output)/'checkpoint.json')
        if state.get('real_scale', {}).get('identity') != identity:
            raise ValueError('refusing different real cadence/source/version before opening journal')
    return run(args, simulation_class=partial(RealSimulation, commit_seconds=args.commit_seconds),
               config_class=Config, mirror_class=RealMirror, graph_auditor=audit_real)


if __name__ == '__main__':
    raise SystemExit(main())
