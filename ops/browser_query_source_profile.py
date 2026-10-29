"""Untimed saved-browser source audit with explicit JSON number semantics.

The unmodified HTTP-wire verifier remains strict. This extra UI-only profile
permits finite int/float representation changes only when values are exactly
equal; it never treats bool as a number, rounds a value or invents timings.
"""
from copy import deepcopy
import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import sys


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def browser_number_compare(actual, expected, *, approx_type=None, approx_compare=None):
    errors, conversions = [], []

    def walk(a, e, path):
        if approx_type is not None and isinstance(e, approx_type):
            # Keep the original verifier's explicit score tolerance and type
            # contract. This profile does not introduce a general epsilon.
            errors.extend(approx_compare(a, e, path, []))
            return
        a_number, e_number = type(a) in (int, float), type(e) in (int, float)
        if a_number and e_number:
            if (type(a) is float and not math.isfinite(a)) or (type(e) is float and not math.isfinite(e)):
                errors.append(path + ': nonfinite JSON number')
            elif a != e:
                # Python compares int/float exactly here. Deliberately do NOT
                # float(a)==float(e), which can hide large-integer loss.
                errors.append(path + ': numeric values differ')
            elif type(a) is not type(e):
                conversions.append({'path': path, 'actual_type': type(a).__name__, 'actual_value': a,
                    'expected_type': type(e).__name__, 'expected_value': e,
                    'rule': 'finite exactly-equal JSON number representation; no rounding'})
            return
        if type(a) is not type(e):
            errors.append(path + ': type mismatch ' + type(a).__name__ + '/' + type(e).__name__)
        elif isinstance(e, dict):
            if set(a) != set(e):
                errors.append(path + ': object field set differs')
            for key in e:
                if key in a:
                    walk(a[key], e[key], path + '.' + key)
        elif isinstance(e, list):
            if len(a) != len(e):
                errors.append(path + ': list length differs')
            for index, (left, right) in enumerate(zip(a, e)):
                walk(left, right, f'{path}[{index}]')
        elif a != e:
            errors.append(path + ': value differs')

    walk(actual, expected, '$')
    return {'passed': not errors, 'errors': errors, 'error_count': len(errors),
            'conversions': conversions, 'conversion_count': len(conversions)}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    for flag in ('source-root', 'verifier', 'proof', 'strict-directory', 'output'):
        p.add_argument('--' + flag, type=Path, required=True)
    args = p.parse_args(argv)
    if args.output.exists():
        raise FileExistsError(args.output)
    sys.path.insert(0, str(args.source_root.resolve()))
    spec = importlib.util.spec_from_file_location('original_strict_catalog_verifier', args.verifier)
    verifier = importlib.util.module_from_spec(spec);spec.loader.exec_module(verifier)
    from backend.real_scale.oracle import RealOracle
    strict_path = args.strict_directory / 'browser-source-verification.json'
    strict = verifier.loads(strict_path.read_text(encoding='utf-8'))
    proof_sha = sha(args.proof)
    proof = verifier.loads(args.proof.read_text(encoding='utf-8'))
    request, actual = verifier.loads(proof['request']), verifier.loads(proof['response'])
    snapshot = args.strict_directory / 'journal-online-snapshot.sqlite'
    if proof_sha != strict['browser_proof_sha256'] or sha(snapshot) != strict['source_snapshot_sha256']:
        raise ValueError('original browser proof or source snapshot changed')
    oracle = RealOracle.open(args.strict_directory / 'oracle.sqlite')
    truth = verifier.JournalTruth(snapshot, oracle)
    try:
        strict_errors = verifier.ResponseVerifier(truth).check(request, actual)
        comparable = deepcopy(actual)
        expected = verifier.ResponseVerifier(truth).expected(request, comparable)
        result = browser_number_compare(comparable, expected, approx_type=verifier.Approx, approx_compare=verifier.compare)
        report = {'passed': result['passed'], 'profile': 'browser_numeric_semantics_v1',
            'scope': 'saved UI visible field values vs independent original journal; not original HTTP bytes or latency',
            'strict_passed': not strict_errors, 'strict_errors': strict_errors,
            'strict_original_report_sha256': sha(strict_path), 'browser_proof_sha256': proof_sha,
            'source_snapshot_sha256': sha(snapshot), 'source_run_id': truth.run_id,
            'source_snapshot_head': truth.head, 'source_prefix': oracle.prefix,
            'source_prefix_sha256': truth.identity['prefix_sha256'], 'source_batch_checksum': oracle.batch_checksum(request['seq']),
            'source_sim_time': truth.batch(request['seq'])['sim_time'], 'request': request,
            'errors': result['errors'], 'error_count': result['error_count'],
            'allowed_numeric_representations': result['conversions'], 'conversion_count': result['conversion_count'],
            'original_verifier_sha256': sha(args.verifier), 'profile_script_sha256': sha(__file__),
            'performance_request': False, 'network_request_issued': False, 'producer_control_issued': False,
            'original_proof_and_source_unchanged': sha(args.proof) == proof_sha and sha(snapshot) == strict['source_snapshot_sha256']}
        report['passed'] = report['passed'] and report['original_proof_and_source_unchanged']
    finally:
        truth.close();oracle.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as out:
        json.dump(report, out, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report['passed'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
