"""Batch-import generated runtime replay JSONL into Neo4j; --apply is required."""
from __future__ import annotations
import argparse, getpass, json, os
from pathlib import Path

def batches(path: Path, size: int):
    batch=[]
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            batch.append(json.loads(line))
            if len(batch)==size: yield batch; batch=[]
    if batch: yield batch

STATE = """UNWIND $rows AS r MERGE (run:Run {id:r.run_id}) MERGE (robot:Robot {id:r.run_id+':robot:'+toString(r.robot_id)}) SET robot.robot_id=r.robot_id MERGE (robot)-[:IN_RUN]->(run) MERGE (s:AgentState {id:r.id}) SET s+=r MERGE (s)-[:IN_RUN]->(run) MERGE (s)-[:OF_ROBOT]->(robot)"""
EVENT = """UNWIND $rows AS r MATCH (run:Run {id:r.run_id}) MATCH (robot:Robot {id:r.run_id+':robot:'+toString(r.robot_id)}) MERGE (e:RuntimeEvent {id:r.id}) SET e+=r MERGE (e)-[:IN_RUN]->(run) MERGE (e)-[:ABOUT_ROBOT]->(robot)"""

if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('--dir',type=Path,default=Path('data/generated/million-runtime-replay')); p.add_argument('--batch-size',type=int,default=5000); p.add_argument('--apply',action='store_true'); a=p.parse_args()
    files=[a.dir/'agent_states.jsonl',a.dir/'runtime_events.jsonl']
    counts=[sum(1 for _ in f.open(encoding='utf-8')) for f in files]
    print(json.dumps({'states':counts[0],'events':counts[1],'mode':'apply' if a.apply else 'dry-run'},ensure_ascii=False))
    if not a.apply: raise SystemExit()
    from neo4j import GraphDatabase
    password=os.environ.get('WAREHOUSE_NEO4J_PASSWORD') or getpass.getpass('Neo4j 密码: ')
    driver=GraphDatabase.driver(os.environ.get('WAREHOUSE_NEO4J_URI','bolt://127.0.0.1:47687'),auth=('neo4j',password))
    written=[0,0]
    with driver.session() as s:
        for path,query in zip(files,[STATE,EVENT]):
            index=0 if path.name.startswith('agent_') else 1
            for rows in batches(path,a.batch_size):
                s.run(query,rows=rows).consume(); written[index]+=len(rows)
                if written[index] % 100000 == 0: print(json.dumps({'file':path.name,'written':written[index]},ensure_ascii=False))
        run_id='scale-replay-100x10000'
        audit=s.run("""MATCH (r:Run {id:$id})
        RETURN COUNT { (r)<-[:IN_RUN]-(:Robot) } AS robots,
               COUNT { (r)<-[:IN_RUN]-(:AgentState) } AS states,
               COUNT { (r)<-[:IN_RUN]-(:RuntimeEvent) } AS events""",id=run_id).single()
        print(json.dumps({'mode':'audit','run_id':run_id,'written_states':written[0],'written_events':written[1],**dict(audit)},ensure_ascii=False))
    driver.close()
