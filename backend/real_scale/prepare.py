"""Build a NEW verified on-disk oracle for a frozen real-journal prefix."""
import argparse
import hashlib
import json
from pathlib import Path

from .oracle import RealOracle
from . import VERSION, DIMENSIONS, SOURCE_KIND, VECTOR_KIND, VECTOR_INDEX, positive_integer


def verify_graph(oracle, store, *, min_states=1000000):
    """Read-only run/prefix-specific counts; old archive nodes never count here."""
    positive_integer(min_states, 'minimum actual states')
    run, prefix = oracle.run_id, oracle.prefix
    counts = store.counts(run, prefix)
    head = store.head(run)
    rows = store.mirror.query('''MATCH (r:WHRun {run_id:$run})
        RETURN r.metadata_json AS metadata,r.config_json AS config,
            r.real_scale_version AS version,r.real_source_kind AS source_kind,
            r.real_source_run AS source_run,r.real_vector_kind AS vector_kind,r.real_dimensions AS dims''', {'run': run}, timeout=30)
    metadata = rows[0] if len(rows) == 1 else {}
    expected = oracle.identity
    checks = {
        'run_head_covers_frozen_prefix': type(head.get('seq')) is int and head['seq'] >= prefix,
        'real_marker_atomic_with_head': head.get('real_last_seq') == head.get('seq'),
        'exact_actual_state_count': counts.get('states') == 100*prefix,
        'all_actual_states_have_vectors_and_hashes': counts.get('states') == counts.get('vectors') == counts.get('payload_hashes'),
        'hundred_actual_entities': counts.get('robots') == 100,
        'exact_prefix_bounds': counts.get('first_seq') == 1 and counts.get('last_seq') == prefix,
        'all_vectors_32_dimensions': counts.get('min_dimensions') == counts.get('max_dimensions') == DIMENSIONS,
        'no_resampled_archive_or_derivation_links': counts.get('archive_states') == counts.get('derived_links') == 0,
        'minimum_requested_actual_states': counts.get('states', 0) >= min_states,
        'same_actual_source_marker': metadata.get('version') == VERSION and metadata.get('source_kind') == SOURCE_KIND
            and metadata.get('source_run') == run and metadata.get('vector_kind') == VECTOR_KIND and metadata.get('dims') == DIMENSIONS,
        'immutable_metadata_matches_source': bool(metadata) and json.loads(metadata['metadata']) == expected['metadata'],
        'immutable_config_matches_source': bool(metadata) and json.loads(metadata['config']) == expected['config'],
    }
    indexes = counts.get('vector_indexes', [])
    valid_index = len(indexes) == 1
    if valid_index:
        index = indexes[0]
        options = index.get('options', {}).get('indexConfig', {})
        similarity = options.get('vector.similarity_function')
        valid_index = (index.get('name') == VECTOR_INDEX and index.get('state') == 'ONLINE'
            and index.get('type') == 'VECTOR' and index.get('labelsOrTypes') == ['WHRealState']
            and index.get('properties') == ['real_embedding'] and options.get('vector.dimensions') == DIMENSIONS
            and isinstance(similarity, str) and similarity.lower() == 'cosine')
    checks['exact_real_vector_index_online'] = valid_index
    source_steps = {}
    for seq in sorted({1, prefix}):
        step = store.mirror.query('MATCH (s:WHStep {key:$key}) RETURN s.checksum AS checksum',
                                  {'key': f'{run}:{seq}'}, timeout=30)
        source_steps[seq] = len(step) == 1 and step[0]['checksum'] == oracle.batch_checksum(seq)
    checks['first_last_source_batch_checksums'] = all(source_steps.values())
    steps = store.mirror.query('''MATCH (s:WHStep {run_id:$run}) USING INDEX s:WHStep(run_id)
        WHERE s.seq>=1 AND s.seq<=$seq RETURN s.seq AS seq,s.checksum AS checksum ORDER BY s.seq''',
        {'run': run, 'seq': prefix}, timeout=120)
    graph_prefix = hashlib.sha256()
    continuous = len(steps) == prefix
    for number, row in enumerate(steps, 1):
        checksum = row.get('checksum')
        if (type(row.get('seq')) is not int or row['seq'] != number or not isinstance(checksum, str)
                or len(checksum) != 64 or any(c not in '0123456789abcdef' for c in checksum)):
            continuous = False
        graph_prefix.update(f"{row.get('seq')}:{checksum}\n".encode())
    checks['all_source_steps_contiguous'] = continuous
    checks['complete_graph_prefix_sha256_matches_journal'] = continuous and graph_prefix.hexdigest() == expected['prefix_sha256']
    commits = store.mirror.query('''MATCH (a:WHAgentState {run_id:$run})
        USING INDEX a:WHAgentState(run_id,seq) WHERE a:WHRealState AND a.seq>=1 AND a.seq<=$seq
        WITH a.seq AS seq,count(a) AS n,count(DISTINCT a.robot_id) AS robots
        RETURN count(*) AS commits,sum(CASE WHEN n=100 AND robots=100 THEN 0 ELSE 1 END) AS bad''',
        {'run': run, 'seq': prefix}, timeout=120)
    checks['all_commits_have_exactly_hundred_entities'] = len(commits) == 1 and commits[0]['commits'] == prefix and commits[0]['bad'] == 0
    graph_counts = {}
    for label, field, expected_count in [('WHEvent', 'commit_seq', expected['events']),
                                        ('WHTaskState', 'seq', expected['task_versions']),
                                        ('WHSlotState', 'seq', expected['slot_versions']),
                                        ('WHChargerState', 'seq', 100*prefix)]:
        row = store.mirror.query(f'MATCH (n:{label} {{run_id:$run}}) WHERE n.{field}<=$seq RETURN count(n) AS n',
                                 {'run': run, 'seq': prefix}, timeout=120)
        value = row[0]['n'] if len(row) == 1 else None
        graph_counts[label] = value
        checks['source_count_'+label] = value == expected_count
    from .benchmark import driver_identity
    return {'status': 'passed' if all(checks.values()) else 'failed', 'checks': checks,
        'run_id': run, 'prefix': prefix, 'source_identity': expected, 'counts': counts,
        'other_same_run_prefix_counts': graph_counts, 'head_at_probe': head,
        'source_first_last_step_checksums_match': source_steps, 'driver': driver_identity(),
        'graph_prefix_step_count': len(steps), 'graph_prefix_sha256': graph_prefix.hexdigest(),
        'qualification': {'million_actual_agent_states': counts.get('states', 0) >= 1000000,
                          'hundred_actual_entities': counts.get('robots') == 100,
                          'semantic_embeddings': False, 'specified_edge_device': False,
                          'query_latency_evaluated': False, 'full_task2_acceptance': False},
        'scope': 'read-only source/scale/schema proof, not a latency benchmark; only this actual run/prefix counted'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--journal', type=Path)
    source.add_argument('--oracle-index', type=Path)
    parser.add_argument('--prefix', type=int)
    parser.add_argument('--run-id')
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--verify-graph', action='store_true')
    parser.add_argument('--min-states', type=int, default=1000000)
    args = parser.parse_args(argv)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    try:
        built = RealOracle.open(args.oracle_index) if args.oracle_index else RealOracle(
            args.journal, args.prefix, index_path=args.output_dir/'oracle.sqlite', run_id=args.run_id)
        with built as oracle:
            if args.oracle_index and (args.prefix is not None and args.prefix != oracle.prefix or
                                      args.run_id is not None and args.run_id != oracle.run_id):
                raise ValueError('requested run/prefix differs from verified index')
            identity = oracle.identity
            identity['oracle_index_path'] = str(oracle.index_path)
            identity['oracle_index_sha256'] = _file_sha(oracle.index_path)
            (args.output_dir/'identity.json').write_text(json.dumps(identity, ensure_ascii=False, indent=2), encoding='utf-8')
            if args.verify_graph:
                from .store import RealStore
                store = RealStore.from_env()
                try:
                    verification = verify_graph(oracle, store, min_states=args.min_states)
                    (args.output_dir/'graph-verification.json').write_text(json.dumps(verification, ensure_ascii=False, indent=2), encoding='utf-8')
                    if verification['status'] != 'passed':
                        raise ValueError('actual graph/source/schema verification failed; report retained')
                finally:
                    store.close()
            print(json.dumps({k: identity[k] for k in ['run_id', 'prefix', 'states', 'events', 'logical_tasks', 'prefix_sha256']}))
    except Exception as exc:
        (args.output_dir/'failure.json').write_text(json.dumps({'error': type(exc).__name__, 'message': str(exc)}), encoding='utf-8')
        raise
    return 0


def _file_sha(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk := stream.read(1024*1024):
            value.update(chunk)
    return value.hexdigest()


if __name__ == '__main__':
    raise SystemExit(main())
