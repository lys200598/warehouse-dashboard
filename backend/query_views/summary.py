"""Transactional, incrementally replayable summaries of original journal facts.

The journal is always opened read-only. A sidecar is a derived artifact, never
an oracle or a replacement source. Every returned row is bound to the exact
source checksum, graph run creation, scene/config and simulation clock.
"""
from contextlib import closing
from pathlib import Path
import json
import math
import sqlite3
import threading
import time
import zlib

from ..knowledge import digest
from . import VERSION


class ViewNotReady(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def connect_ro(path):
    db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True, timeout=10)
    db.execute('PRAGMA query_only=ON')
    return db


def number(value, name):
    if type(value) not in (int,float) or not math.isfinite(value):
        raise ValueError('invalid '+name)
    return value


class JournalSummary:
    def __init__(self, journal, cache, run, created_version):
        self.journal, self.path = Path(journal).resolve(), Path(cache).resolve()
        if self.path == self.journal:
            raise ValueError('derived view must not overwrite original journal')
        if not isinstance(run,str) or not run or not isinstance(created_version,str) or not created_version:
            raise ValueError('run creation identity required')
        self.run, self.created = run, created_version
        self.lock = threading.RLock()
        self.path.parent.mkdir(parents=True,exist_ok=True)
        if self.path.exists() and self.path.stat().st_size:
            with closing(connect_ro(self.path)) as existing:
                tables={r[0] for r in existing.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if any(not name.startswith('view_') for name in tables):
                    raise ValueError('refusing unrelated SQLite file as derived cache')
        with closing(sqlite3.connect(self.path,timeout=30)) as db:
            db.executescript('''PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS view_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS view_tasks(task TEXT PRIMARY KEY,zone TEXT NOT NULL,
                    status TEXT NOT NULL,due REAL NOT NULL,overdue INTEGER NOT NULL);
                CREATE INDEX IF NOT EXISTS view_pending_due ON view_tasks(due)
                    WHERE overdue=0 AND status<>'completed';
                CREATE TABLE IF NOT EXISTS view_counts(zone TEXT,status TEXT,n INTEGER NOT NULL,
                    overdue INTEGER NOT NULL,PRIMARY KEY(zone,status));
                CREATE TABLE IF NOT EXISTS view_points(seq INTEGER PRIMARY KEY,checksum TEXT NOT NULL,
                    payload TEXT NOT NULL,payload_sha256 TEXT NOT NULL);
            ''')
            rows=dict(db.execute('SELECT key,value FROM view_meta'))
            if rows:
                if rows.get('run')!=run or rows.get('created')!=created_version or rows.get('version')!=VERSION:
                    raise ValueError('derived view belongs to another run creation/version')
            else:
                db.executemany('INSERT INTO view_meta VALUES (?,?)', [('run',run),('created',created_version),('version',VERSION),('cursor','0')])
            db.commit()

    @staticmethod
    def _count(db, zone, status, n, late):
        db.execute('''INSERT INTO view_counts VALUES (?,?,?,?) ON CONFLICT(zone,status)
            DO UPDATE SET n=n+excluded.n,overdue=overdue+excluded.overdue''',(zone,status,n,late))

    def catch_up(self, target=None):
        """Consume an immutable prefix in one transaction; timings include IO/hash.

        A failed/gapped source leaves the old cursor and all old views unchanged.
        Caller controls whether this happens during startup, background update or
        a measured API request; this method never hides its elapsed time.
        """
        started=time.perf_counter()
        with self.lock, closing(connect_ro(self.journal)) as source, closing(sqlite3.connect(self.path,timeout=30)) as db:
            source.execute('BEGIN')
            head=source.execute('SELECT seq FROM heads WHERE run_id=?',(self.run,)).fetchone()
            if not head:raise ViewNotReady('source run absent')
            end=head[0] if target is None else target
            if type(end) is not int or end<1 or end>head[0]:raise ViewNotReady('source does not cover requested graph watermark')
            # Hot same-watermark reads do not acquire a SQLite writer lock.
            meta=dict(db.execute("SELECT key,value FROM view_meta WHERE key IN ('run','created','version','cursor')"))
            if (meta.get('run')!=self.run or meta.get('created')!=self.created or meta.get('version')!=VERSION):
                raise ValueError('derived view identity changed')
            cursor=int(meta['cursor'])
            if cursor and end<=cursor and head[0]>=cursor:
                original=source.execute('SELECT checksum FROM commits WHERE run_id=? AND seq=?',(self.run,cursor)).fetchone()
                stored=db.execute('SELECT checksum FROM view_points WHERE seq=?',(cursor,)).fetchone()
                if original is None or original!=stored:raise ValueError('consumed source prefix was replaced')
                return {'head':cursor,'consumed':0,'wall_seconds':time.perf_counter()-started,'read_only_fast_path':True}
            db.execute('BEGIN IMMEDIATE')
            try:
                # Another process may have advanced the view while this one
                # waited for its writer lock. Re-open the source snapshot too.
                source.rollback();source.execute('BEGIN')
                head=source.execute('SELECT seq FROM heads WHERE run_id=?',(self.run,)).fetchone()
                if head is None:raise ViewNotReady('source run disappeared')
                end=head[0] if target is None else target
                if type(end) is not int or end<1 or end>head[0]:raise ViewNotReady('source watermark unavailable')
                meta=dict(db.execute('SELECT key,value FROM view_meta'))
                cursor=int(meta['cursor'])
                if head[0]<cursor:raise ValueError('source head regressed')
                if cursor:
                    original=source.execute('SELECT checksum FROM commits WHERE run_id=? AND seq=?',(self.run,cursor)).fetchone()
                    stored=db.execute('SELECT checksum FROM view_points WHERE seq=?',(cursor,)).fetchone()
                    if original is None or original!=stored:raise ValueError('consumed source prefix was replaced')
                if end<=cursor:
                    db.rollback();return {'head':cursor,'consumed':0,'wall_seconds':time.perf_counter()-started}
                previous=cursor
                for seq,checksum,blob in source.execute('SELECT seq,checksum,batch FROM commits WHERE run_id=? AND seq>? AND seq<=? ORDER BY seq',(self.run,cursor,end)):
                    batch=json.loads(zlib.decompress(blob))
                    if (seq!=previous+1 or batch.get('run_id')!=self.run or type(batch.get('seq')) is not int
                            or batch['seq']!=seq or digest(batch)!=checksum):
                        raise ValueError('source gap/run/checksum mismatch')
                    clock=batch['sim_time'];number(clock,'source clock')
                    if type(clock) is not int:raise ValueError('source clock must be integer')
                    if previous:
                        last=json.loads(db.execute('SELECT payload FROM view_points WHERE seq=?',(previous,)).fetchone()[0])
                        if clock<last['sim_time']:raise ValueError('source clock regressed')
                    if not meta.get('metadata_sha256'):
                        racks={r['id']:r['zone'] for r in batch['metadata']['scene']['racks']}
                        zones={s['slot_id']:racks.get(s['rack_id'],'unmapped') for s in batch['slots']}
                        for key,value in [('metadata_sha256',digest(batch['metadata'])),('config_sha256',digest(batch['config'])),('zones',canonical(zones)),('first_checksum',checksum)]:
                            db.execute('INSERT INTO view_meta VALUES (?,?)',(key,value));meta[key]=value
                    if digest(batch['metadata'])!=meta['metadata_sha256'] or digest(batch['config'])!=meta['config_sha256']:
                        raise ValueError('source metadata/config changed')
                    zones=json.loads(meta['zones'])
                    for zone,status,count in db.execute("SELECT zone,status,count(*) FROM view_tasks WHERE overdue=0 AND status<>'completed' AND due<? GROUP BY zone,status",(clock,)).fetchall():
                        self._count(db,zone,status,0,count)
                    db.execute("UPDATE view_tasks SET overdue=1 WHERE overdue=0 AND status<>'completed' AND due<?",(clock,))
                    seen=set()
                    for task in batch['tasks']:
                        key,status=task['task_id'],task['status']
                        if key in seen or status=='scheduled' or task['release_second']>clock:
                            raise ValueError('duplicate/unpublished source task delta')
                        seen.add(key);due=number(task['due_second'],'deadline')
                        old=db.execute('SELECT zone,status,overdue FROM view_tasks WHERE task=?',(key,)).fetchone()
                        if old:self._count(db,old[0],old[1],-1,-old[2])
                        zone=zones.get(task['slot_id'],'unmapped');late=int(status!='completed' and due<clock)
                        self._count(db,zone,status,1,late)
                        db.execute('INSERT OR REPLACE INTO view_tasks VALUES (?,?,?,?,?)',(key,zone,status,due,late))
                    counts=[{'zone':z,'status':s,'tasks':n,'overdue':late} for z,s,n,late in db.execute('SELECT zone,status,n,overdue FROM view_counts WHERE n<>0 ORDER BY zone,status')]
                    if any(type(c['tasks']) is not int or c['tasks']<0 or not 0<=c['overdue']<=c['tasks'] for c in counts):
                        raise ValueError('derived count invariant failed')
                    entities=sum(c['tasks'] for c in counts)
                    if sum(c['tasks'] for c in counts if c['status']=='completed')!=batch['completed']:
                        raise ValueError('derived completed count differs from source Step')
                    value={'run_id':self.run,'seq':seq,'sim_time':clock,'completed':batch['completed'],
                           'batch_checksum':checksum,'task_entity_count':entities,'counts':counts}
                    db.execute('INSERT INTO view_points VALUES (?,?,?,?)',(seq,checksum,canonical(value),digest(value)))
                    previous=seq
                if previous!=end:raise ValueError('source prefix incomplete')
                db.execute("UPDATE view_meta SET value=? WHERE key='cursor'",(str(end),))
                db.commit()
            except BaseException:
                db.rollback();raise
        return {'head':end,'consumed':end-cursor,'wall_seconds':time.perf_counter()-started}

    ingest = catch_up

    def status(self):
        with closing(connect_ro(self.path)) as db:
            m=dict(db.execute("SELECT key,value FROM view_meta WHERE key='cursor'"))
        return {'version':VERSION,'run_id':self.run,'created_version':self.created,'head':int(m['cursor'])}

    def read(self,run,seq,fleet,head_seq,head_checksum):
        with closing(connect_ro(self.path)) as db:
            db.execute('BEGIN')
            meta=dict(db.execute("SELECT key,value FROM view_meta WHERE key IN ('run','created','metadata_sha256','config_sha256')"))
            if (meta['run']!=run or meta['created']!=fleet['token'][1]
                    or meta.get('metadata_sha256')!=digest(fleet['metadata'])
                    or meta.get('config_sha256')!=digest(fleet['config'])):
                raise ValueError('derived view source/creation/config mismatch')
            records={}
            for wanted in set((seq,head_seq)):
                row=db.execute('SELECT checksum,payload,payload_sha256 FROM view_points WHERE seq=?',(wanted,)).fetchone()
                if row is None:raise ViewNotReady('derived view has not covered graph watermark')
                value=json.loads(row[1])
                if (digest(value)!=row[2] or value.get('run_id')!=run or value.get('seq')!=wanted
                        or value.get('batch_checksum')!=row[0]):raise ValueError('derived row corrupted')
                records[wanted]=value
        requested,head=records[seq],records[head_seq]
        if (head['batch_checksum']!=head_checksum or requested['batch_checksum']!=fleet['token'][0]
                or requested['sim_time']!=fleet['step']['sim_time'] or requested['completed']!=fleet['step']['completed']):
            raise ValueError('derived summary is not the exact graph point')
        if requested['task_entity_count']>head['task_entity_count']:raise ValueError('source task entities regressed')
        return requested,head['task_entity_count']


class SummaryDirectory:
    def __init__(self,root,cache_root=None):
        self.root=Path(root).resolve();self.cache_root=Path(cache_root).resolve() if cache_root else self.root/'.query-summary'
        self.repositories={};self.lock=threading.Lock()

    def repository(self,run,created):
        source=(self.root/run/'journal.sqlite').resolve()
        if not source.is_relative_to(self.root):raise ValueError('source path escapes configured journal root')
        key=(run,created)
        with self.lock:
            if key not in self.repositories:
                cache=self.cache_root/(digest([run,created])+'.sqlite')
                self.repositories[key]=JournalSummary(source,cache,run,created)
            return self.repositories[key]

    def catch_up(self,run,created,target=None):
        return self.repository(run,created).catch_up(target)

    def status(self,run,created):
        return self.repository(run,created).status()

    def catch_up_from_graph(self,mirror,run,target,*,timeout=15):
        """Publisher hook after mirror commit, before exposing a new frame.

        Fresh graph creation identity is read, never derived from the run name.
        This only writes this directory's derived SQLite cache, not Neo4j.
        """
        rows=mirror.query('''MATCH (r:WHRun {run_id:$run}) WHERE r.committed_seq >= $seq
            MATCH (s:WHStep {key:$run+':'+toString($seq)})
            RETURN r.run_id AS run_id,r.committed_seq AS head,
                toString(r.created_at) AS created,s.checksum AS checksum''',
            {'run':run,'seq':target},timeout=timeout)
        if len(rows)!=1 or rows[0]['run_id']!=run or type(rows[0]['head']) is not int or rows[0]['head']<target:
            raise ViewNotReady('graph has not mirrored summary target')
        row=rows[0];repository=self.repository(run,row['created'])
        result=repository.catch_up(target)
        with closing(connect_ro(repository.path)) as db:
            actual=db.execute('SELECT checksum FROM view_points WHERE seq=?',(target,)).fetchone()
        if actual is None or actual[0]!=row['checksum']:
            raise ValueError('derived publisher prefix differs from actual graph commit')
        return {**result,'run_id':run,'graph_head':row['head'],'created_version':row['created'],
                'published_seq':target,'batch_checksum':row['checksum']}
