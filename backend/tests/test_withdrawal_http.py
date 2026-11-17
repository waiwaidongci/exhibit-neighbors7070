from sqlalchemy import select
from fastapi.testclient import TestClient
from src.main import app
from src.config.database import Session
from src.models.run import Run
from src.models.note import Note
from src.models.record import Record
from src.models.audit import Audit
from src.services.suppression import visitor_credential
from src.utils.security import token_for

TARGET='V-042';OTHER='V-043'

def _seed(factory):
    s1=factory.source('WP-E2E-1');s2=factory.source('WP-E2E-2')
    recs=[]
    for src,pfx in ((s1,'e1'),(s2,'e2')):
        for i,v in enumerate((TARGET,OTHER,'V-201','V-202')):
            for j,ex in enumerate(('E01','E02','E03')):
                recs.append(factory.record(src,f'{pfx}-{v}-{ex}',v,ex,1+((i+j)%5)))
    run=factory.completed_run(s1,recs[:12],'端到端研究')
    note=factory.note(run,'approved')
    from src.config.settings import ARTIFACT_DIR
    (ARTIFACT_DIR/f'run-{run.id}.csv').write_text('payload '+TARGET,encoding='utf-8')
    factory.s.commit()
    return s1,s2,run,note

def test_full_http_workflow(db,factory,users,fake_queue):
    s1,s2,run,note=_seed(factory)
    admin={'Authorization':'Bearer '+token_for(users['admin'])}
    viewer={'Authorization':'Bearer '+token_for(users['viewer'])}
    client=TestClient(app)
    # 阅读者不可见撤回台账
    assert client.get('/withdrawals',headers=viewer).status_code==403
    # 发起撤回
    r=client.post('/withdrawals',json={'visitor':TARGET},headers=admin)
    assert r.status_code==200
    dto=r.json()
    assert dto['state']=='completed' and dto['residual_findings']==[]
    assert dto['credential'] and TARGET not in r.text
    # 台账可查、状态为已完成
    r=client.get('/withdrawals',headers=admin)
    assert r.status_code==200 and r.json()[0]['state']=='completed'
    # 受影响研究隔离、留档停用
    r=client.get(f'/runs/{run.id}',headers=admin).json()
    assert r['state']=='quarantined'
    assert client.get(f'/runs/{run.id}/export',headers=admin).status_code==409
    assert client.get(f'/notes/{note.id}/export',headers=admin).status_code==409
    # 跨批次评分清除、他人保留
    with db() as s:
        assert not any((x.values or {}).get('visitor')==TARGET for x in s.scalars(select(Record)))
        assert len([x for x in s.scalars(select(Record)) if x.values['visitor']==OTHER])==6
        assert s.get(Note,note.id).state=='revoked'
    # 再追加该访客评分被阻断；他人可追加
    r=client.post(f'/sources/{s1.id}/records',headers=admin,
                  json={'records':[{'reference':'NEW-1','values':{'visitor':TARGET,'exhibit':'E08','rating':4}}]})
    assert r.status_code==409
    r=client.post(f'/sources/{s1.id}/records',headers=admin,
                  json={'records':[{'reference':'NEW-2','values':{'visitor':OTHER,'exhibit':'E08','rating':4}}]})
    assert r.status_code==200
    # 重复撤回 409
    r=client.post('/withdrawals',json={'visitor':TARGET},headers=admin)
    assert r.status_code==409
    # 审计全程匿名
    events=client.get('/audit',headers=admin).text
    assert TARGET not in events
    with db() as s:
        for e in s.scalars(select(Audit).where(Audit.action.like('withdrawal.%'))):
            assert TARGET not in (e.actor+e.target+e.detail)
            assert e.target==visitor_credential(TARGET)

def test_retry_validation_and_resume(db,factory,users,fake_queue):
    s1,s2,run,note=_seed(factory)
    admin={'Authorization':'Bearer '+token_for(users['admin'])}
    client=TestClient(app)
    from src.models.withdrawal import Withdrawal
    from src.utils.serialization import now
    with db() as s:
        wd=Withdrawal(visitor_hash=visitor_credential(TARGET),state='cleaning',affected_runs=[],
                      affected_notes=[],removed_ratings=0,artifact_plan=[],purged_artifacts=[],
                      residual_findings=[],created_by='admin@x.demo',created_at=now(),updated_at=now(),
                      last_error='中断')
        s.add(wd);s.commit();wid=wd.id
    # 错误标识拒绝
    r=client.post(f'/withdrawals/{wid}/retry',json={'visitor':'V-WRONG'},headers=admin)
    assert r.status_code==422
    # 正确标识续清成功
    r=client.post(f'/withdrawals/{wid}/retry',json={'visitor':TARGET},headers=admin)
    assert r.status_code==200 and r.json()['state']=='completed'
    # 终态重试幂等
    r=client.post(f'/withdrawals/{wid}/retry',json={'visitor':''},headers=admin)
    assert r.status_code==200 and r.json()['state']=='completed'
