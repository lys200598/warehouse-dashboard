"""Read-only source-prefix size audit for every original query sample.

Each selected cutoff receives its own cumulative distinct-node count. Final
graph size is never copied backwards into an earlier pressure round. This tool
has no Neo4j contact and does not import the production query implementation.
"""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib


MILLION = 1_000_000


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_sha(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def loads(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate JSON field: ' + key)
            result[key] = value
        return result
    def invalid(value):
        raise ValueError('nonfinite JSON value: ' + value)
    return json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)


def read_lines(path):
    opener = gzip.open if Path(path).suffix == '.gz' else open
    with opener(path, 'rt', encoding='utf-8') as stream:
        for number, line in enumerate(stream, 1):
            if line.strip():
                yield number, loads(line)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, label, minimum=1, maximum=None):
    require(type(value) is int and value >= minimum and (maximum is None or value <= maximum), 'invalid ' + label)
    return value


def source_prefixes(path):
    """Reconstruct the writer's cumulative disjoint object keys per commit."""
    path = Path(path).resolve(); before = file_sha(path)
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    counts = Counter()
    known = {name: set() for name in ('robots', 'tasks', 'slots', 'cargo', 'racks', 'zones')}
    sizes, first_crossing = {}, None
    last_event = 0
    prefix_hash = hashlib.sha256()
    try:
        db.execute('BEGIN')
        heads = db.execute('SELECT run_id,seq,checksum,checkpoint,mirrored_seq FROM heads').fetchall()
        require(len(heads) == 1, 'one final original run per journal required')
        run, prefix, head_sha, head_blob, mirrored = heads[0]
        integer(prefix, 'source head')
        raw = zlib.decompress(head_blob)
        require(hashlib.sha256(raw).hexdigest() == head_sha, 'source head SHA mismatch')
        checkpoint = loads(raw)
        require(checkpoint['run_id'] == run and checkpoint['knowledge_seq'] == prefix, 'source head identity differs')
        scene = checkpoint['run_metadata']['scene']
        static_slots = None
        metadata_sha, config_sha = digest(checkpoint['run_metadata']), digest(checkpoint['config'])
        for seq, checksum, blob in db.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? ORDER BY seq', (run,)):
            integer(seq, 'source batch seq', 1, prefix)
            require(seq == len(sizes) + 1, 'source batch sequence gap')
            raw = zlib.decompress(blob); batch = loads(raw)
            require(hashlib.sha256(raw).hexdigest() == checksum == digest(batch), 'source batch SHA mismatch')
            require(batch['run_id'] == run and type(batch['seq']) is int and batch['seq'] == seq, 'foreign source batch')
            require(digest(batch['metadata']) == metadata_sha and digest(batch['config']) == config_sha, 'source config/metadata changed')
            integer(batch['sim_time'], 'source simulation time', 0)
            ids = [r['robot_id'] for r in batch['robots']]
            require(len(ids) == 100 and set(ids) == set(range(1, 101)) and all(type(owner) is int for owner in ids), 'source batch needs 100 actual robots')
            known['robots'].update(ids)
            counts['WHStep'] += 1
            counts['WHAgentState'] += len(ids)
            counts['WHChargerState'] += batch['config']['chargers']
            counts['WHTaskState'] += len(batch['tasks'])
            counts['WHSlotState'] += len(batch['slots'])
            require(len({t['task_id'] for t in batch['tasks']}) == len(batch['tasks']), 'duplicate source task version')
            require(len({s['slot_id'] for s in batch['slots']}) == len(batch['slots']), 'duplicate source slot version')
            for task in batch['tasks']:
                known['tasks'].add(task['task_id']); known['cargo'].add(task['goods_id'])
            for slot in batch['slots']:
                known['slots'].add(slot['slot_id'])
                if slot.get('cargo_id') is not None:
                    known['cargo'].add(slot['cargo_id'])
            if seq == 1:
                static_slots = set(known['slots'])
                counts['WHLocation'] = len(static_slots) + len(scene['docks']) + len(scene['homes']) + len(scene['stations']) + 100 + 1
            require(known['slots'] == static_slots, 'new slot identity after initial static registry')
            for rack in batch['racks']:
                known['racks'].add(rack['id']); known['zones'].add(rack['zone'])
            for event in batch['events']:
                number = integer(event['event_seq'], 'source event seq')
                require(number == last_event + 1 and event['event_id'] == f'{run}:event:{number}', 'source event sequence/identity gap')
                last_event = number
                counts['WHEvent'] += 1
                if event.get('task_id') is not None:
                    known['tasks'].add(event['task_id'])
                if event.get('goods_id') is not None:
                    known['cargo'].add(event['goods_id'])
                if 'goods_id' in event and 'location_id' in event:
                    counts['WHGoodsState'] += 1
            for label, value in {'WHRun': 1, 'WHRobot': len(known['robots']), 'WHTask': len(known['tasks']),
                                 'WHSlot': len(known['slots']), 'WHCargo': len(known['cargo']),
                                 'WHRack': len(known['racks']), 'WHZone': len(known['zones'])}.items():
                counts[label] = value
            nodes = sum(counts.values())
            sizes[seq] = {'seq': seq, 'sim_time': batch['sim_time'], 'source_nodes': nodes,
                          'actual_robot_states': counts['WHAgentState'], 'events': counts['WHEvent'],
                          'batch_checksum': checksum}
            if first_crossing is None and nodes >= MILLION:
                first_crossing = dict(sizes[seq])
            prefix_hash.update(f'{seq}:{checksum}\n'.encode())
        require(len(sizes) == prefix and last_event == checkpoint['event_sequence'], 'source final prefix/events differ')
        expected_labels = dict(counts)
        expected_labels.update(WHRealState=counts['WHAgentState'], WHCharger=len(scene['docks']),
                               WHPhysicalBay=len(scene['docks']), WHLocationAlias=len(scene['homes']))
        require(file_sha(path) == before, 'final source journal changed during size audit')
        return {'run_id': run, 'prefix': prefix, 'mirrored_seq': mirrored, 'journal_sha256': before,
                'prefix_sha256': prefix_hash.hexdigest(), 'head_checkpoint_sha256': head_sha,
                'sizes': sizes, 'first_million_node_prefix': first_crossing,
                'expected_graph_label_counts': expected_labels,
                'final_source_nodes': sizes[prefix]['source_nodes'],
                'counting_note': 'Cumulative distinct primary nodes in this run only. WHRealState shares WHAgentState nodes; WHCharger/WHPhysicalBay/WHLocationAlias share WHLocation nodes. No resampling or future prefix sizes.'}
    finally:
        db.close()


def response_point(payload):
    require(isinstance(payload, dict), 'successful full response must be an object')
    if 'catalog_version' not in payload:
        return payload
    inner, query = payload.get('result'), payload.get('query_id')
    require(isinstance(inner, dict), 'catalog result must be an object')
    name = 'context' if query in ('self', 'readiness', 'task', 'goods', 'similar_history') else 'point'
    point = inner.get(name)
    require(isinstance(point, dict), 'catalog full response lacks explicit source point')
    for key in ('run_id', 'seq'):
        if key in point:
            require(digest(point[key]) == digest(payload[key]), 'catalog inner/outer source point differs')
    return {**point, 'run_id': payload['run_id'], 'seq': payload['seq']}


def summarize_requests(source, requests_path, responses_path=None):
    responses = {}
    if responses_path is not None:
        for line, record in read_lines(responses_path):
            rid = record['request_id']
            require(rid not in responses, 'duplicate full response ID at line ' + str(line))
            responses[rid] = record['result']
    rounds, seen, nodes_by_request = defaultdict(list), set(), []
    for line, record in read_lines(requests_path):
        rid = record['request_id']
        require(isinstance(rid, str) and rid and rid not in seen, 'duplicate/invalid request ID at line ' + str(line))
        seen.add(rid)
        require(record['run_id'] == source['run_id'], 'request belongs to another run')
        seq = integer(record['seq'], 'request cutoff', 1, source['prefix'])
        number = integer(record['round'], 'request round')
        require(type(record['ok']) is bool, 'request status must be boolean')
        nested = record.get('request')
        if nested is not None:
            require(nested.get('run_id') == source['run_id'] and digest(nested.get('seq')) == digest(seq), 'embedded request cutoff differs')
        value = {'request_id': rid, 'round': number, 'seq': seq,
                 'source_nodes': source['sizes'][seq]['source_nodes'], 'ok': record['ok']}
        if responses_path is not None:
            require(rid in responses, 'missing original full response')
            payload = responses[rid]
            require(digest(payload) == record['response_sha256'], 'full response SHA mismatch')
            if record['ok']:
                point = response_point(payload)
                require(point.get('run_id') == source['run_id'] and digest(point.get('seq')) == digest(seq), 'response cutoff/run differs')
                head = integer(point.get('graph_committed_seq'), 'response observed head', seq, source['prefix'])
                require(point.get('batch_checksum') == source['sizes'][seq]['batch_checksum'], 'response batch checksum differs from original source')
                value.update(observed_head=head, observed_head_source_nodes=source['sizes'][head]['source_nodes'])
        rounds[number].append(value); nodes_by_request.append(value)
    require(bool(seen), 'no original pressure requests')
    if responses_path is not None:
        require(set(responses) == seen, 'extra unmatched full responses')
    summaries = []
    for number, values in sorted(rounds.items()):
        summary = {'round': number, 'requests': len(values), 'failed_requests': sum(not r['ok'] for r in values),
                   'minimum_seq': min(r['seq'] for r in values), 'maximum_seq': max(r['seq'] for r in values),
                   'minimum_source_nodes': min(r['source_nodes'] for r in values),
                   'maximum_source_nodes': max(r['source_nodes'] for r in values),
                   'million_node_requests': sum(r['source_nodes'] >= MILLION for r in values),
                   'successful_million_node_requests': sum(r['source_nodes'] >= MILLION and r['ok'] for r in values),
                   'all_requests_at_million_node_prefix': all(r['source_nodes'] >= MILLION for r in values)}
        heads = [r['observed_head_source_nodes'] for r in values if 'observed_head_source_nodes' in r]
        if heads:
            summary.update(minimum_observed_head_source_nodes=min(heads), maximum_observed_head_source_nodes=max(heads))
        summaries.append(summary)
    return {'request_count': len(seen), 'rounds': summaries,
            'million_node_requests': sum(r['source_nodes'] >= MILLION for r in nodes_by_request),
            'successful_million_node_requests': sum(r['source_nodes'] >= MILLION and r['ok'] for r in nodes_by_request),
            'each_response_source_point_verified': responses_path is not None,
            'request_prefix_sizes': nodes_by_request}


def audit(journal, requests, comparison, responses=None):
    inputs = {'journal': Path(journal).resolve(), 'requests': Path(requests).resolve(), 'comparison': Path(comparison).resolve()}
    if responses is not None:
        inputs['responses'] = Path(responses).resolve()
    before = {name: file_sha(path) for name, path in inputs.items()}
    source = source_prefixes(inputs['journal'])
    pressure = summarize_requests(source, inputs['requests'], inputs.get('responses'))
    compared = loads(inputs['comparison'].read_text(encoding='utf-8'))
    expected = compared['source_statistics']['required']
    graph = compared.get('graph_statistics')
    checks = {
        'comparison_passed': compared.get('passed') is True,
        'same_original_journal': expected['run_id'] == source['run_id'] and expected['journal_sha256'] == source['journal_sha256'],
        'same_final_prefix': digest(expected['prefix']) == digest(source['prefix']) and expected['prefix_sha256'] == source['prefix_sha256'],
        'final_expected_distinct_nodes_match': digest(expected['expected_distinct_run_nodes']) == digest(source['final_source_nodes']),
        'final_expected_labels_match': digest(expected['expected_graph_label_counts']) == digest(source['expected_graph_label_counts']),
        'final_graph_actual_available': isinstance(graph, dict),
        'source_inputs_unchanged': before == {name: file_sha(path) for name, path in inputs.items()},
    }
    if isinstance(graph, dict):
        checks.update(final_graph_same_run=graph.get('run_id') == source['run_id'],
                      final_graph_same_head=digest(graph.get('head')) == digest(source['prefix']),
                      final_graph_distinct_nodes_match=digest(graph.get('distinct_nodes')) == digest(source['final_source_nodes']),
                      final_graph_labels_match=digest(graph.get('label_counts')) == digest(source['expected_graph_label_counts']))
    return {'version': 'warehouse-query-source-prefix-size-v1', 'passed': all(checks.values()),
            'checks': checks, 'failed_checks': [name for name, value in checks.items() if not value],
            'run_id': source['run_id'], 'final_prefix': source['prefix'],
            'final_source_nodes': source['final_source_nodes'], 'final_actual_robot_states': source['sizes'][source['prefix']]['actual_robot_states'],
            'first_million_node_prefix': source['first_million_node_prefix'],
            'pressure': pressure, 'prefix_sizes': list(source['sizes'].values()), 'file_hashes': before,
            'qualification': 'Size audit success validates bindings and counts, not latency/concurrency. Only requests with their own cutoff prefix >=1,000,000 qualify as million-prefix pressure samples. Actual observed head size is separately reported when full responses are provided.',
            'counting_note': source['counting_note']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', type=Path, required=True)
    parser.add_argument('--requests', type=Path, required=True)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--responses', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error('preserve previous audit; select a new output')
    try:
        result = audit(args.journal, args.requests, args.comparison, args.responses)
    except Exception as exc:
        result = {'version': 'warehouse-query-source-prefix-size-v1', 'passed': False,
                  'error': type(exc).__name__ + ': ' + str(exc)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False); stream.write('\n')
    print(json.dumps({'passed': result['passed'], 'error': result.get('error'),
                      'failed_checks': result.get('failed_checks'),
                      'first_million_node_prefix': result.get('first_million_node_prefix'),
                      'rounds': result.get('pressure', {}).get('rounds')}))
    return 0 if result['passed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
