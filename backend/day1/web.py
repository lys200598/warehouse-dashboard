"""Read-only run/status/history server. No reset/control or arbitrary Cypher API."""
from __future__ import annotations
import argparse
from http.server import ThreadingHTTPServer,SimpleHTTPRequestHandler
import json
from pathlib import Path
import sqlite3
from urllib.parse import urlsplit,parse_qs
import zlib
from ..checkpoint import read_checkpoint
from ..knowledge_queries import WarehouseQueries,CATALOG,CATALOG_VERSION

def serve(args):
    root=Path(args.output).resolve();dist=Path(args.dist).resolve();mirror=None
    if args.neo4j:
        import os
        from neo4j import GraphDatabase
        from ..knowledge_graph import Neo4jMirror
        driver=GraphDatabase.driver(os.environ['NEO4J_URI'],auth=(os.environ.get('NEO4J_USER','neo4j'),os.environ['NEO4J_PASSWORD']),max_connection_pool_size=12)
        driver.verify_connectivity();mirror=Neo4jMirror(driver,os.environ.get('NEO4J_DATABASE','neo4j'))
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self,*a,**kw):super().__init__(*a,directory=str(dist),**kw)
        def answer(self,data,status=200):
            raw=json.dumps(data,ensure_ascii=False,allow_nan=False).encode();self.send_response(status)
            self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Content-Length',str(len(raw)));self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(raw)
        def latest(self):return read_checkpoint(root/'latest.json')
        def do_GET(self):
            u=urlsplit(self.path);q=parse_qs(u.query)
            try:
                if u.path in {'/api/day/status','/api/health'}:
                    f=self.latest();result=json.loads((root/'results.json').read_text()) if (root/'results.json').exists() else None
                    return self.answer({'run_id':f['runId'],'runId':f['runId'],'latest_second':f['elapsed'],'running':f['running'],'knowledge':f['knowledge'],'results':result})
                if u.path=='/api/day/graph-summary':
                    f=self.latest();result=json.loads((root/'results.json').read_text()) if (root/'results.json').exists() else {}
                    example=mirror.query('MATCH (g:WHCargo {run_id:$run}) RETURN g.cargo_id AS id ORDER BY id LIMIT 1',{'run':f['runId']}) if mirror else []
                    return self.answer({'run_id':f['runId'],'watermark':f['knowledge'],'counts':result.get('graph_audit'),'example_goods':example[0]['id'] if example else None})
                if u.path=='/api/day/entity':
                    if not mirror:return self.answer({'error':'graph unavailable'},503)
                    f=self.latest();run=f['runId'];kind=q.get('kind',[''])[0];identity=q.get('id',[''])[0]
                    cutoff=int(q.get('seq',[str(f['knowledge']['mirrored_seq'])])[0])
                    if not 1<=len(identity)<=128 or not 1<=cutoff<=f['knowledge']['mirrored_seq']:raise ValueError('invalid id or committed cutoff')
                    if kind=='goods':
                        rows=mirror.query('''MATCH (v:WHGoodsState {run_id:$run,goods_id:$id})
                            WHERE v.seq<=$seq AND v.event_seq IS NOT NULL
                            RETURN v.event_seq AS event_seq,v.sim_time AS sim_time,v.kind AS kind,v.location_id AS location,v.seq AS seq
                            ORDER BY v.event_seq DESC LIMIT 100''',{'run':run,'id':identity,'seq':cutoff})
                        rows.reverse()
                    elif kind=='charger':
                        rows=mirror.query('''MATCH (v:WHChargerState {key:$key})
                            RETURN v.charger_id AS charger_id,v.sim_time AS sim_time,v.reserved_robot_ids AS reserved_robots,v.occupant_robot_id AS occupant''',{'key':run+':'+str(cutoff)+':'+identity})
                    else:raise ValueError('unknown entity kind')
                    return self.answer({'run_id':run,'seq':cutoff,'rows':rows})
                if u.path=='/api/day/frame':
                    wanted=int(q.get('second',['0'])[0]);latest=self.latest()['elapsed'];wanted=max(0,min(latest,wanted))
                    with sqlite3.connect(f'file:{root / "frames.sqlite"}?mode=ro',uri=True,timeout=10) as db:
                        row=db.execute('SELECT payload FROM frames WHERE second<=? ORDER BY second DESC LIMIT 1',(wanted,)).fetchone()
                    if row is None:return self.answer({'error':'frame not available'},404)
                    return self.answer(json.loads(zlib.decompress(row[0])))
                if u.path=='/api/knowledge/status':return self.answer(self.latest()['knowledge'])
                if u.path=='/api/knowledge/templates':return self.answer({'catalog_version':CATALOG_VERSION,'templates':CATALOG})
                if u.path=='/':
                    self.send_response(302);self.send_header('Location','/layout-preview.html?run=1');self.end_headers();return
                super().do_GET()
            except (ValueError,FileNotFoundError,KeyError) as e:self.answer({'error':str(e)},400)
            except Exception as e:self.answer({'error':type(e).__name__},503)
        def do_POST(self):
            if urlsplit(self.path).path!='/api/knowledge/query':return self.answer({'error':'read-only pilot'},405)
            if not mirror:return self.answer({'error':'graph unavailable'},503)
            try:
                length=int(self.headers.get('Content-Length','0'))
                if not 0<length<=8192:raise ValueError('invalid request size')
                request=json.loads(self.rfile.read(length));run=self.latest()['runId']
                if request.get('run_id')!=run:raise ValueError('only this pilot run may be queried')
                self.answer(WarehouseQueries(mirror).execute(request))
            except ValueError as e:self.answer({'error':str(e)},400)
            except Exception:self.answer({'error':'graph query failed'},503)
    server=ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
    print(f'read-only day1 view on 127.0.0.1:{args.port}',flush=True)
    try:server.serve_forever()
    finally:
        server.server_close()
        if mirror:mirror.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--dist',default='dist');p.add_argument('--port',type=int,default=14178);p.add_argument('--neo4j',action='store_true');serve(p.parse_args())
