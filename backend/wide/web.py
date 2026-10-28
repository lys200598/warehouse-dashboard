"""Read-only wide-run playback and run-scoped temporal knowledge endpoints."""
import argparse
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
import json
from pathlib import Path
import sqlite3
from urllib.parse import urlsplit, parse_qs
import zlib

from ..checkpoint import read_checkpoint
from .graph import WideMirror
from .queries import WideQueries


def serve(args):
    root=Path(args.output).resolve()
    dist=Path(args.dist).resolve()
    mirror=WideMirror.from_env() if args.neo4j else None
    queries=WideQueries(mirror) if mirror else None

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self,*a,**kw):super().__init__(*a,directory=str(dist),**kw)

        def json(self,value,status=200):
            raw=json.dumps(value,ensure_ascii=False,allow_nan=False).encode()
            self.send_response(status)
            self.send_header('Content-Type','application/json; charset=utf-8')
            self.send_header('Cache-Control','no-store')
            self.send_header('Content-Length',str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def latest(self):return read_checkpoint(root/'latest.json')

        def do_GET(self):
            request=urlsplit(self.path)
            q=parse_qs(request.query)
            try:
                if request.path=='/':
                    self.send_response(302);self.send_header('Location','/wide.html');self.end_headers();return
                if request.path in {'/api/status','/api/health'}:
                    frame=self.latest()
                    result=json.loads((root/'results.json').read_text(encoding='utf-8')) if (root/'results.json').exists() else None
                    return self.json({'run_id':frame['runId'],'latest_second':frame['elapsed'],
                        'knowledge':frame['knowledge'],'results':result,'running':frame['running']})
                if request.path=='/api/scene':
                    return self.json(read_checkpoint(root/'checkpoint.json')['run_metadata']['scene'])
                if request.path=='/api/frame':
                    latest=self.latest()['elapsed']
                    wanted=max(0,min(latest,int(q.get('second',[str(latest)])[0])))
                    with sqlite3.connect(f'file:{root / "frames.sqlite"}?mode=ro',uri=True,timeout=10) as db:
                        row=db.execute('SELECT payload FROM frames WHERE second<=? ORDER BY second DESC LIMIT 1',(wanted,)).fetchone()
                    if not row:return self.json({'error':'frame not recorded'},404)
                    return self.json(json.loads(zlib.decompress(row[0])))
                if request.path.startswith('/api/knowledge/'):
                    if not queries:return self.json({'error':'graph not configured'},503)
                    frame=self.latest()
                    run=frame['runId']
                    cutoff=int(q.get('seq',[str(frame['knowledge']['mirrored_seq'])])[0])
                    if cutoff>frame['knowledge']['mirrored_seq']:
                        raise ValueError('cutoff exceeds this run playback watermark')
                    identity=q.get('id',[''])[0]
                    limit=int(q.get('limit',['200'])[0])
                    cursor=int(q.get('after_event_seq',['0'])[0])
                    if request.path=='/api/knowledge/goods':
                        return self.json(queries.goods_history(run,identity,cutoff,limit,cursor))
                    if request.path=='/api/knowledge/robot':
                        return self.json(queries.robot_context(run,int(identity),cutoff))
                    if request.path=='/api/knowledge/cooperation':
                        return self.json(queries.cooperation_history(run,q.get('task',[None])[0],cutoff,min(limit,100),cursor))
                    return self.json({'error':'unknown query'},404)
                if request.path.startswith('/api/'):
                    return self.json({'error':'unknown endpoint'},404)
                super().do_GET()
            except (ValueError,KeyError,FileNotFoundError) as exc:
                self.json({'error':str(exc)},400)
            except Exception:
                self.json({'error':'query or snapshot unavailable'},503)

        def do_POST(self):self.json({'error':'read-only endpoint'},405)

    server=ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
    print(f'Wide knowledge view on 127.0.0.1:{args.port}',flush=True)
    try:server.serve_forever()
    finally:
        server.server_close()
        if mirror:mirror.close()


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--output',required=True)
    p.add_argument('--dist',default='dist')
    p.add_argument('--port',type=int,default=14179)
    p.add_argument('--neo4j',action='store_true')
    serve(p.parse_args())
