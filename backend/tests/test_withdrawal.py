import json,os
import pytest
from sqlalchemy import select
from src.config.database import Session
from src.config.settings import ARTIFACT_DIR
from src.models.record import Record
from src.models.run import Run
from src.models.note import Note
from src.models.withdrawal import Withdrawal
from src.services import erasure,suppression
from src.services.suppression import visitor_credential
from src.services.erasure import scrub,contains_token
from src.worker import execute
from src.services.export import run_csv,note_csv
from fastapi import HTTPException

TARGET='V-007'
OTHER='V-008'

def build_world(factory,queued_run=False):
    """3 个批次；目标访客 V-007 的评分跨 3 批次，且进入两个已完成研究与批准留档。"""
    s1,s2,s3=[factory.source(k) for k in ('WP-A','WP-B','WP-C')]
    def ratings(src,prefix):
        visitors=[TARGET,OTHER,'V-101','V-102']
        out=[]
        for i,v in enumerate(visitors):
            for j,ex in enumerate(('E01','E02','E03')):
                out.append(factory.record(src,f'{prefix}-{v}-{ex}',v,ex,1+((i+j)%5)))
        return out
    r1=ratings(s1,'a');r2=ratings(s2,'b');r3=ratings(s3,'c')
    run1=factory.completed_run(s1,r1,'研究一')
    run2=factory.completed_run(s2,r2,'研究二')
    # run3 与目标无关，作为“无关研究保留”对照
    run3=factory.completed_run(s3,[r for r in r3 if r.values['visitor']!=TARGET],'研究三-无关')
    n1=factory.note(run1,'approved');n2=factory.note(run2,'draft')
    for rid in (run1.id,run2.id,run3.id):
        (ARTIFACT_DIR/f'run-{rid}.csv').write_text(f'csv {rid} {TARGET}',encoding='utf-8')
    qrun=None
    if queued_run:
        payload=[{'id':r.id,'source_id':r.source_id,'reference':r.reference,'values':r.values} for r in r3]
        qrun=Run(source_id=s3.id,name='排队研究',state='queued',parameters={'neighbors':5,'min_common':2},
                 snapshot={'source':{'id':s3.id,'name':s3.name,'filename':s3.filename},'records':payload},
                 result=None,created_at='2026-11-17T00:00:00+00:00')
        factory.s.add(qrun);factory.s.flush()
    factory.s.commit()
    return dict(s1=s1,s2=s2,s3=s3,run1=run1,run2=run2,run3=run3,n1=n1,n2=n2,qrun=qrun)

# ---------- 1. 跨批次永久清除评分，他人与无关研究保留 ----------
def test_cross_batch_erasure_and_isolation(db,factory,users,fake_queue):
    w=build_world(factory)
    before=factory.s.query(Record).count()
    dto=erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    assert dto['state']=='completed' and dto['residual_findings']==[]
    with db() as s:
        assert not any((r.values or {}).get('visitor')==TARGET for r in s.scalars(select(Record)))
        assert all((r.values or {}).get('visitor')!=TARGET for r in s.scalars(select(Record)))
        others=[r for r in s.scalars(select(Record)) if r.values['visitor']==OTHER]
        assert len(others)==9
        run1,run2,run3=s.get(Run,w['run1'].id),s.get(Run,w['run2'].id),s.get(Run,w['run3'].id)
        assert run1.state==run2.state=='quarantined'
        assert not contains_token(run1.snapshot,TARGET) and not contains_token(run1.result,TARGET)
        assert not contains_token(run2.snapshot,TARGET) and not contains_token(run2.result,TARGET)
        # 无关研究原封不动
        assert run3.state=='completed' and contains_token(run3.result,OTHER)
        n1,n2=s.get(Note,w['n1'].id),s.get(Note,w['n2'].id)
        assert n1.state==n2.state=='revoked'
        assert n1.frozen_result=={'withdrawn':True} and n2.frozen_result=={'withdrawn':True}
        assert dto['affected_runs']==sorted([run1.id,run2.id])
        assert dto['affected_notes']==sorted([n1.id,n2.id])

# ---------- 2. 留档文件清除，无关文件保留 ----------
def test_artifact_files_purged(db,factory,users):
    w=build_world(factory)
    erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    assert not (ARTIFACT_DIR/f"run-{w['run1'].id}.csv").exists()
    assert not (ARTIFACT_DIR/f"run-{w['run2'].id}.csv").exists()
    assert (ARTIFACT_DIR/f"run-{w['run3'].id}.csv").exists()

# ---------- 3. 受理即阻断新评分混入（手工追加） ----------
def test_new_rating_blocked_on_accept(db,factory,users,fake_queue):
    w=build_world(factory)
    erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    with db() as s:
        from src.services.sources import add_records
        from src.schemas.source import RecordBatch,RecordInput
        batch=RecordBatch(records=[RecordInput(reference='X1',values={'visitor':TARGET,'exhibit':'E09','rating':3})])
        with pytest.raises(HTTPException) as ei:add_records(s,users['admin'],w['s1'].id,batch)
        assert ei.value.status_code==409
        # 其他访客不受影响
        ok=RecordBatch(records=[RecordInput(reference='X2',values={'visitor':OTHER,'exhibit':'E09','rating':3})])
        assert add_records(s,users['admin'],w['s1'].id,ok)=={'added':1}

# ---------- 4. 清理期间冻结新研究被阻断 ----------
def test_freeze_blocked_while_pending(db,factory,users,fake_queue,monkeypatch):
    build_world(factory)
    with db() as s:
        wd=Withdrawal(visitor_hash=visitor_credential(TARGET),state='blocked',affected_runs=[],
                      affected_notes=[],removed_ratings=0,artifact_plan=[],purged_artifacts=[],
                      residual_findings=[],created_by='admin@x.demo',created_at='t',updated_at='t')
        s.add(wd);s.commit()
        from src.services.runs import create_run
        from src.schemas.run import RunInput
        src=s.scalars(select(Record)).first().source_id
        data=RunInput(source_id=src,name='新研究',parameters={'neighbors':5,'min_common':2})
        with pytest.raises(HTTPException) as ei:create_run(s,users['analyst'],data)
        assert ei.value.status_code==409

# ---------- 5. 完成后含撤回访客的来源仍不可冻结，干净来源可冻结 ----------
def test_freeze_gate_after_completed(db,factory,users,fake_queue):
    w=build_world(factory)
    erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    with db() as s:
        from src.services.runs import create_run
        from src.schemas.run import RunInput
        # 干净来源（s3 的目标评分已随记录删除，run3 输入本就无目标）
        s3=s.get(type(w['s3']),w['s3'].id)
        data=RunInput(source_id=s3.id,name='干净研究',parameters={'neighbors':5,'min_common':2})
        run=create_run(s,users['analyst'],data)
        assert run['state']=='queued'
        assert fake_queue.items==[str(run['id'])]

# ---------- 6. 并发旧计算发布被拦截（计算期间撤回生效） ----------
def test_worker_drops_stale_publish(db,factory,users,fake_queue,monkeypatch):
    w=build_world(factory,queued_run=True)
    qid=w['qrun'].id
    # 让 calculate 期间撤回单已存在，模拟“计算中受理撤回”
    real_calc=__import__('src.worker',fromlist=['calculate']).calculate
    def slow_calc(records,params):
        with Session() as s:
            s.add(Withdrawal(visitor_hash=visitor_credential(TARGET),state='blocked',affected_runs=[],
                             affected_notes=[],removed_ratings=0,artifact_plan=[],purged_artifacts=[],
                             residual_findings=[],created_by='admin@x.demo',created_at='t',updated_at='t'))
            s.commit()
        return real_calc(records,params)
    monkeypatch.setattr('src.worker.calculate',slow_calc)
    execute(qid)
    with db() as s:
        qr=s.get(Run,qid)
        assert qr.state=='quarantined' and qr.result is None

# ---------- 7. 旧任务重放不能复活（队列项被 worker 首道闸门丢弃） ----------
def test_queued_replay_quarantined(db,factory,users,fake_queue):
    w=build_world(factory,queued_run=True)
    qid=w['qrun'].id
    # 撤回受理（含 lrem）后，仍把同一任务再次塞入队列，模拟 AOF/旧消息重放
    erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    fake_queue.items.append(str(qid))
    execute(qid)
    with db() as s:
        qr=s.get(Run,qid)
        assert qr.state=='quarantined' and not contains_token(qr.snapshot,TARGET)

# ---------- 8. 重复导入（种子脚本）不能复活 ----------
def test_reimport_does_not_revive(db,factory,users,fake_queue):
    w=build_world(factory)
    erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    from scripts.seed_demo import seed_values
    vals=seed_values('neighbors',0,0,__import__('random').Random(1))
    vals['visitor']=TARGET
    with db() as s:
        from src.services.sources import add_records
        from src.schemas.source import RecordBatch,RecordInput
        batch=RecordBatch(records=[RecordInput(reference='RE-1',values=vals)])
        with pytest.raises(HTTPException):add_records(s,users['admin'],w['s1'].id,batch)
        assert suppression.visitor_is_suppressed(s,TARGET)

# ---------- 9. 文件清除中断可恢复，重试不重复清理 ----------
def test_resumable_after_file_phase_interrupt(db,factory,users,fake_queue,monkeypatch):
    w=build_world(factory)
    calls={'n':0}
    import src.services.erasure as em
    real_unlink=os.unlink
    def flaky(self,*a,**k):
        if str(self).endswith(f"run-{w['run2'].id}.csv") and calls['n']==0:
            calls['n']+=1;raise OSError('simulated interrupt')
        return real_unlink(self,*a,**k)
    import pathlib
    monkeypatch.setattr(pathlib.Path,'unlink',flaky)
    dto=erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    # 第一次：一个文件删除失败 -> 核验发现残留，状态停在 cleaning
    assert dto['state']=='cleaning'
    assert any(f['type']=='artifact_file' for f in dto['residual_findings'])
    assert (ARTIFACT_DIR/f"run-{w['run2'].id}.csv").exists()
    # 重试：幂等续清，已删文件不重复处理、无报错，最终 completed
    dto2=em.advance_withdrawal(dto['id'])
    assert dto2['state']=='completed' and dto2['residual_findings']==[]
    assert not (ARTIFACT_DIR/f"run-{w['run2'].id}.csv").exists()
    assert dto2['removed_ratings']==dto['removed_ratings']  # 评分删除只计数一次

# ---------- 10. 数据库阶段中断（无 token 续跑）可恢复 ----------
def test_resume_without_token_after_db_interrupt(db,factory,users,fake_queue):
    w=build_world(factory)
    wid=None
    with db() as s:
        # 手工制造一个停在 cleaning、评分尚未删除的撤回单
        wd=Withdrawal(visitor_hash=visitor_credential(TARGET),state='cleaning',affected_runs=[],
                      affected_notes=[],removed_ratings=0,artifact_plan=[],purged_artifacts=[],
                      residual_findings=[],created_by='admin@x.demo',created_at='t',updated_at='t',
                      last_error='中断')
        s.add(wd);s.commit();wid=wd.id
    # 不带 token 续跑：从残留评分还原标识
    dto=erasure.advance_withdrawal(wid)
    assert dto['state']=='completed'
    with db() as s:
        assert not any((r.values or {}).get('visitor')==TARGET for r in s.scalars(select(Record)))

# ---------- 11. 重试不重复、不误删他人 ----------
def test_retry_idempotent_and_isolated(db,factory,users,fake_queue):
    w=build_world(factory)
    d1=erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    d2=erasure.advance_withdrawal(d1['id'])  # 终态重试直接返回
    d3=erasure.advance_withdrawal(d1['id'],'')
    assert d1['id']==d2['id']==d3['id'] and d3['state']=='completed'
    with db() as s:
        other_count=len([r for r in s.scalars(select(Record)) if r.values['visitor']==OTHER])
        assert other_count==9
        assert s.query(Withdrawal).count()==1

# ---------- 12. 重复撤回请求 409，审计只留匿名凭据 ----------
def test_duplicate_request_and_anonymous_audit(db,factory,users,fake_queue):
    build_world(factory)
    erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    with pytest.raises(HTTPException) as ei:
        erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    assert ei.value.status_code==409
    from src.models.audit import Audit
    with db() as s:
        events=s.scalars(select(Audit).where(Audit.action.like('withdrawal.%'))).all()
        assert {e.action for e in events} >= {'withdrawal.requested','withdrawal.completed'}
        for e in events:
            assert TARGET not in (e.target+e.detail+e.actor)
            assert e.target==visitor_credential(TARGET) or not e.target.startswith('V-')

# ---------- 13. 权限：仅负责人可发起 ----------
def test_permission_enforced_http(db,factory,users,fake_queue):
    from fastapi.testclient import TestClient
    from src.main import app
    from src.utils.security import token_for
    build_world(factory)
    client=TestClient(app)
    r=client.get('/withdrawals')
    assert r.status_code in (401,403)
    h_analyst={'Authorization':'Bearer '+token_for(users['analyst'])}
    r=client.post('/withdrawals',json={'visitor':TARGET},headers=h_analyst)
    assert r.status_code==403
    h_admin={'Authorization':'Bearer '+token_for(users['admin'])}
    r=client.post('/withdrawals',json={'visitor':TARGET},headers=h_admin)
    assert r.status_code==200 and r.json()['state']=='completed'
    # 审计页不回传原始访客标识
    r=client.get('/audit',headers=h_admin)
    assert r.status_code==200 and TARGET not in r.text
    # 列表不回传完整哈希/原始标识
    r=client.get('/withdrawals',headers=h_admin)
    body=r.text
    assert TARGET not in body and visitor_credential(TARGET) not in body

# ---------- 14. 隔离/撤回内容禁止导出 ----------
def test_exports_blocked(db,factory,users,fake_queue):
    w=build_world(factory)
    erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    with db() as s:
        with pytest.raises(HTTPException) as e1:run_csv(s,w['run1'].id)
        assert e1.value.status_code==409
        with pytest.raises(HTTPException) as e2:note_csv(s,w['n1'].id)
        assert e2.value.status_code==409
        # 无关研究仍可导出
        assert run_csv(s,w['run3'].id).status_code==200

# ---------- 15. 邻居数据确实被清除（scrub 针对 recommendations/neighbors） ----------
def test_neighbor_scrub_unit():
    result={'recommendations':[
        {'visitor':'A','exhibit':'E1','neighbors':[{'visitor':TARGET,'similarity':0.9,'centered_rating':1.0}]},
        {'visitor':TARGET,'exhibit':'E2','neighbors':[]},
        {'visitor':'B','exhibit':'E3','neighbors':[{'visitor':'C','similarity':0.2,'centered_rating':0.5}]}],
        'chart':[{'label':TARGET+' · E9','value':3.1},{'label':'B · E1','value':2.0}]}
    out=scrub(result,TARGET)
    assert not contains_token(out,TARGET)
    assert len(out['recommendations'])==2
    assert [r['visitor'] for r in out['recommendations']]==['A','B']
    assert out['recommendations'][0]['neighbors']==[]
    assert out['chart']==[{'label':'B · E1','value':2.0}]

# ---------- 16. 核验不通过绝不报完成（模拟残留文件） ----------
def test_verification_gate_blocks_completion(db,factory,users,fake_queue,monkeypatch):
    w=build_world(factory)
    import src.services.erasure as em
    def no_unlink(self,*a,**k):
        if self.name.startswith('run-'):raise OSError('disk offline')
    import pathlib
    monkeypatch.setattr(pathlib.Path,'unlink',no_unlink)
    dto=em.create_withdrawal(factory.s,users['admin'],TARGET)
    assert dto['state']=='cleaning'
    assert dto['residual_findings'] and not dto['completed_at']
    with db() as s:
        assert s.get(Withdrawal,dto['id']).state=='cleaning'

# ---------- 17. 启动恢复把中断撤回单续清到终态 ----------
def test_startup_resume_all_pending(db,factory,users,fake_queue,monkeypatch):
    w=build_world(factory)
    import src.services.erasure as em
    def no_unlink(self,*a,**k):
        if self.name.startswith('run-'):raise OSError('disk offline')
    import pathlib
    original_unlink=pathlib.Path.unlink
    monkeypatch.setattr(pathlib.Path,'unlink',no_unlink)
    dto=em.create_withdrawal(factory.s,users['admin'],TARGET)
    assert dto['state']=='cleaning'
    # 模拟磁盘恢复后的进程重启
    pathlib.Path.unlink=original_unlink
    em.resume_all_pending()
    with db() as s:
        wd=s.get(Withdrawal,dto['id'])
        assert wd.state=='completed' and wd.residual_findings==[]
        assert not any((r.values or {}).get('visitor')==TARGET for r in s.scalars(select(Record)))

# ---------- 18. 冻结快照被整体脱敏，推荐邻居不含撤回访客 ----------
def test_frozen_snapshot_and_neighbors_scrubbed(db,factory,users,fake_queue):
    w=build_world(factory)
    dto=erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    with db() as s:
        r1=s.get(Run,w['run1'].id)
        # 冻结输入中该访客评分行全部消失，他人保留（按引用编号或 values 两种形态兼容）
        snap_visitors={rec.get('values',rec).get('visitor') for rec in r1.snapshot['records'] if rec.get('values',rec).get('visitor')}
        assert TARGET not in snap_visitors and OTHER in snap_visitors
        # 推荐、验证、图表均无该访客，邻居列表里也没有
        assert not contains_token(r1.result,TARGET)
        for rec in r1.result['recommendations']:
            assert all(nb['visitor']!=TARGET for nb in rec.get('neighbors',[]))

# ---------- 19. 前缀相似访客绝不被误删（V-007 不波及 V-0070） ----------
def test_prefix_lookalike_visitor_is_protected(db,factory,users,fake_queue):
    s=factory.source('WP-PREFIX')
    lookalike='V-0070'
    records=[]
    for i,v in enumerate((TARGET,lookalike,'V-301','V-302')):
        for j,ex in enumerate(('E01','E02','E03')):
            records.append(factory.record(s,f'p-{v}-{ex}',v,ex,1+((i+j)%5)))
    run=factory.completed_run(s,records,'前缀对照研究')
    # 另建一个只含相似访客、与撤回访客无关的研究与备选
    s2=factory.source('WP-LOOKALIKE-ONLY')
    only=[]
    for vi,v in enumerate((lookalike,'V-401','V-402')):
        for j,ex in enumerate(('E01','E02','E03')):
            only.append(factory.record(s2,f'lo-{v}-{ex}',v,ex,1+((vi+j)%5)))
    safe_run=factory.completed_run(s2,only,'相似访客独立研究')
    safe_note=factory.note(safe_run,'approved')
    (ARTIFACT_DIR/f'run-{run.id}.csv').write_text(
        'visitor,prediction\nV-0070,4.0\nV-007,3.0',encoding='utf-8')
    (ARTIFACT_DIR/f'run-{safe_run.id}.csv').write_text('visitor\nV-0070',encoding='utf-8')
    factory.s.commit()
    dto=erasure.create_withdrawal(factory.s,users['admin'],TARGET)
    assert dto['state']=='completed'
    with db() as s:
        # 前缀相似访客在两个来源的评分完整保留（每来源 3 条，共 6 条）
        kept=[r for r in s.scalars(select(Record)) if r.values['visitor']==lookalike]
        assert len(kept)==6
        target_left=[r for r in s.scalars(select(Record)) if r.values['visitor']==TARGET]
        assert target_left==[]
        rr=s.get(Run,run.id)
        assert rr.state=='quarantined'  # 研究因含 V-007 被隔离
        # 但 V-0070 的行仍在脱敏后的快照中，且其引用编号被就地脱敏
        def vof(rec):return rec.get('values',rec).get('visitor')
        assert any(vof(rec)==lookalike for rec in rr.snapshot['records'])
        assert all(vof(rec)!=TARGET for rec in rr.snapshot['records'])
        assert any(rec.get('reference')=='[已清除撤回访客标识]' for rec in rr.snapshot['records'])
        assert contains_token(rr.result,lookalike)
        assert not contains_token(rr.result,TARGET)
        # 完全无关的研究与备选不受影响、可继续取用
        sr=s.get(Run,safe_run.id)
        assert sr.state=='completed' and contains_token(sr.result,lookalike)
        sn=s.get(Note,safe_note.id)
        assert sn.state=='approved' and contains_token(sn.frozen_result,lookalike)
    # 含撤回访客的产物已删除，无关产物保留
    assert not (ARTIFACT_DIR/f'run-{run.id}.csv').exists()
    assert (ARTIFACT_DIR/f'run-{safe_run.id}.csv').exists()
    # 边界匹配单元断言
    assert erasure._mentions('V-007 · E01',TARGET)
    assert not erasure._mentions('V-0070',TARGET)
    assert erasure._mentions('编号 V-007，已撤回',TARGET)

