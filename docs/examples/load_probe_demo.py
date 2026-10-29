"""Local teaching endpoint for load_harness/load_probe; no warehouse or Neo4j.

Run from the repository root. Responses explicitly say demo=true, so these
measurements cannot be mistaken for knowledge-graph acceptance evidence.
"""
import argparse
import asyncio
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from ops.load_probe import LoadProbeMiddleware


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new server trace file')
    parser.add_argument('--port', type=int, default=14590)
    args = parser.parse_args()

    async def app(scope, receive, send):
        status = 200
        if scope['path'] == '/api/health' and scope['method'] == 'GET':
            value = {'status': 'ok', 'demo': True}
        elif scope['path'] == '/api/knowledge/query' and scope['method'] == 'POST':
            data = b''
            while True:
                message = await receive()
                if message['type'] == 'http.disconnect':
                    return
                data += message.get('body', b'')
                if not message.get('more_body', False):
                    break
            try:
                request = json.loads(data)
                value = {'run_id': request['run_id'], 'robot_id': request['robot_id'], 'demo': True}
            except (ValueError, KeyError, TypeError):
                status, value = 400, {'error': 'run_id and robot_id required', 'demo': True}
            await asyncio.sleep(.005)
        else:
            status, value = 404, {'error': 'unknown demo endpoint', 'demo': True}
        raw = json.dumps(value).encode('utf-8')
        await send({'type': 'http.response.start', 'status': status,
                    'headers': [(b'content-type', b'application/json'),
                                (b'content-length', str(len(raw)).encode())]})
        await send({'type': 'http.response.body', 'body': raw})

    import uvicorn
    probe = LoadProbeMiddleware(app, output=args.output,
                                source_identity={'scope': 'teaching demo; NOT a graph benchmark'})
    try:
        uvicorn.run(probe, host='127.0.0.1', port=args.port, lifespan='off',
                    access_log=False, log_level='warning')
    finally:
        probe.close()


if __name__ == '__main__':
    main()
