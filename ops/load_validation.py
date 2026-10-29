"""Offline structural validators. Source/oracle equality remains a separate audit."""
import hashlib
import json


def validate_catalog(row):
    request, response = row['request'], row['response']
    assert isinstance(response, dict) and isinstance(request, dict)
    assert response['run_id'] == request['run_id']
    assert response['query_id'] == request['query_id']
    if 'seq' in request:
        assert type(response['seq']) is int and response['seq'] == request['seq']
    assert isinstance(response['result'], dict)
    if request['query_id'] in ('self', 'readiness'):
        context = response['result']['context']
        assert context['run_id'] == response['run_id']
        assert context['seq'] == response['seq']
        assert type(context['graph_committed_seq']) is int
        assert context['graph_committed_seq'] >= context['seq']
        assert len(context['robots']) == 1
        robot = context['robots'][0]
        assert robot['robot_id'] == request['robot_id']
        assert robot['robot']['robot_id'] == request['robot_id']
        # Same canonical encoding used by backend.knowledge.digest. Verifying a
        # self-consistent checksum alone does NOT prove source/oracle equality.
        encoded = json.dumps(robot['robot'], sort_keys=True, separators=(',', ':'),
                             ensure_ascii=False, allow_nan=False).encode('utf-8')
        assert hashlib.sha256(encoded).hexdigest() == robot['robot_sha256']
    return True
