import os,tempfile
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(tempfile.mkdtemp(prefix='withdrawal-'),'test.db')
os.environ['ARTIFACT_DIR']=tempfile.mkdtemp(prefix='withdrawal-art-')
os.environ['JWT_SECRET']='test-secret-withdrawal'
os.environ['SEED_DEMO']='false'
import pytest
from redis import Redis
from src.config.database import Base,engine,Session
from src.models.user import User
from src.models.source import Source
from src.models.record import Record
from src.models.run import Run
from src.models.note import Note
from src.models.withdrawal import Withdrawal
from src.utils.security import hash_password
from src.utils.serialization import now
from src.services.algorithm import calculate

class FakeRedis:
    def __init__(self):self.items=[]
    def rpush(self,k,v):self.items.append(str(v));return len(self.items)
    def lrem(self,k,count,v):
        target=str(v);before=len(self.items)
        self.items=[x for x in self.items if x!=target]
        return before-len(self.items)
    def lrange(self,k,s,e):
        seg=self.items if e==-1 else self.items[s:e+1]
        return [x.encode() for x in seg]
    def blpop(self,k,timeout=2):
        if not self.items:return None
        return (k,self.items.pop(0).encode())
    def ping(self):return True

@pytest.fixture
def fake_queue(monkeypatch):
    q=FakeRedis()
    import src.services.runs as runs_mod
    import src.worker as worker_mod
    import src.services.erasure as erasure_mod
    monkeypatch.setattr(runs_mod,'queue',q)
    monkeypatch.setattr(worker_mod,'queue',q)
    monkeypatch.setattr(erasure_mod,'queue',q)
    yield q

@pytest.fixture
def db(fake_queue):
    Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
    with Session() as s:
        admin=User(email='admin@x.demo',password_hash=hash_password('pw'),role='admin')
        analyst=User(email='analyst@x.demo',password_hash=hash_password('pw'),role='analyst')
        viewer=User(email='viewer@x.demo',password_hash=hash_password('pw'),role='viewer')
        s.add_all([admin,analyst,viewer]);s.commit()
    yield Session
    Base.metadata.drop_all(engine)

class Factory:
    def __init__(self,session):self.s=session
    def source(self,key):
        src=Source(key=key,name=key,filename=key+'.csv',description='d')
        self.s.add(src);self.s.flush();return src
    def record(self,src,ref,visitor,exhibit,rating):
        r=Record(source_id=src.id,reference=ref,values={'visitor':visitor,'exhibit':exhibit,'rating':rating})
        self.s.add(r);self.s.flush();return r
    def completed_run(self,src,records,name='研究'):
        payload=[{'id':r.id,'source_id':r.source_id,'reference':r.reference,'values':r.values} for r in records]
        result=calculate(payload,{'neighbors':5,'min_common':2})
        run=Run(source_id=src.id,name=name,state='completed',parameters={'neighbors':5,'min_common':2},
                snapshot={'source':{'id':src.id,'name':src.name,'filename':src.filename},'records':payload},
                result=result,created_at=now())
        self.s.add(run);self.s.flush()
        return run
    def note(self,run,state='approved'):
        n=Note(run_id=run.id,title='备选',body='正文内容',state=state,
               frozen_result=run.result,author='analyst@x.demo',
               approved_by='admin@x.demo' if state=='approved' else '')
        self.s.add(n);self.s.flush();return n

@pytest.fixture
def factory(db):
    with db() as s:yield Factory(s)

@pytest.fixture
def users(db):
    with db() as s:
        return {u.role:u for u in s.query(User).all()}
