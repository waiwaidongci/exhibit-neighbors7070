import json,logging,time,re
from fastapi import HTTPException
from sqlalchemy import select
from ..config.database import Session
from ..config.settings import ARTIFACT_DIR
from ..models.withdrawal import Withdrawal
from ..models.record import Record
from ..models.run import Run
from ..models.note import Note
from ..utils.serialization import row,now
from .suppression import visitor_credential
from .audit import log
from .runs import queue
DROP=object()
def _mentions(text,token):
    """命中同一访客的各种载体：纯标识、图表 'V-007 · E01'、引用编号 'a-V-007-E01'、CSV 行。
    仅排除字母数字直接相邻，因此 V-007 不会误伤 V-0070，但能识别被 '-' 等标点包裹的同一标识。"""
    if not token:return False
    pattern=r'(?<![0-9A-Za-z])'+re.escape(token)+r'(?![0-9A-Za-z])'
    return re.search(pattern,text) is not None
def scrub(obj,token):
    """递归剔除撤回访客的一切痕迹：评分行、冻结输入、推荐邻居、留一验证、图表标签。"""
    if isinstance(obj,dict):
        # visitor 字段按精确值判定：该访客自己的整行剔除；绝不因前缀相似误伤其他访客
        if obj.get('visitor')==token:return DROP
        # 冻结评分行（含 reference 与 values）：只就地脱敏引用编号，保留其他访客的整行
        if 'reference' in obj and isinstance(obj.get('values'),dict):
            out={}
            for k,v in obj.items():
                if k=='reference' and isinstance(v,str) and _mentions(v,token):
                    out[k]='[已清除撤回访客标识]'
                else:
                    cleaned=scrub(v,token)
                    if cleaned is not DROP:out[k]=cleaned
            return out
        # 图表 label 等把访客标识拼接进字符串的字段，使用边界匹配后整项剔除
        if any(isinstance(v,str) and _mentions(v,token) for v in obj.values()):return DROP
        out={}
        for k,v in obj.items():
            cleaned=scrub(v,token)
            if cleaned is not DROP:out[k]=cleaned
        return out
    if isinstance(obj,list):
        return [cleaned for item in obj if (cleaned:=scrub(item,token)) is not DROP]
    return obj
def contains_token(obj,token):
    if isinstance(obj,dict):
        return any(contains_token(v,token) for v in obj.values())
    if isinstance(obj,list):
        return any(contains_token(v,token) for v in obj)
    return isinstance(obj,str) and _mentions(obj,token)
def _run_contains(run,token):
    return contains_token(run.snapshot,token) or contains_token(run.result,token)
def _note_contains(note,token):
    return contains_token(note.frozen_result,token)
def _artifact_name(run_id):return f'run-{run_id}.csv'
def create_withdrawal(db,user,token):
    token=(token or '').strip()
    if not token:raise HTTPException(422,'访客标识不能为空')
    if len(token)>600:raise HTTPException(422,'访客标识最多600字符')
    credential=visitor_credential(token)
    existing=db.scalar(select(Withdrawal).where(Withdrawal.visitor_hash==credential))
    if existing:raise HTTPException(409,'该访客的撤回请求已受理，请勿重复提交')
    w=Withdrawal(visitor_hash=credential,state='blocked',affected_runs=[],affected_notes=[],
                 removed_ratings=0,artifact_plan=[],purged_artifacts=[],residual_findings=[],
                 created_by=user.email,created_at=now(),updated_at=now())
    db.add(w)
    db.flush()
    # 审计只记录匿名凭据，原始访客标识不入库、不入日志
    log(db,user,'withdrawal.requested',credential,'受理即阻断新评分与新计算')
    db.commit()
    # 受理后立即在同一请求内推进清理；失败可断点续清，不改变已生效的阻断
    return advance_withdrawal(w.id,token)
def advance_withdrawal(withdrawal_id,token=None):
    with Session() as db:
        w=db.get(Withdrawal,withdrawal_id)
        if not w:raise HTTPException(404,'未找到撤回请求')
        if w.state=='completed':return _dto(db,w)
        # 行锁保证同一撤回单串行清理；重试拿到的是同一把锁，不会重复清理
        w=db.get(Withdrawal,withdrawal_id,with_for_update=True)
        if token is None:
            token=_recover_token(db,w)
            if token is None:
                w.last_error='清理中断后无法从残留数据还原访客标识，等待携带标识的重试'
                w.updated_at=now();db.commit();return _dto(db,w)
        # 先在锁外等待进行中的旧计算收敛（worker 第二道闸门会隔离它），避免发布与清理穿插
        if token:
            preview={r.id for r in db.scalars(select(Run)).all() if _run_contains(r,token)}
            db.commit()
            _wait_for_running(preview)
        else:
            db.commit()
        w=db.get(Withdrawal,withdrawal_id,with_for_update=True)
        try:
            w.state='cleaning';w.updated_at=now();w.last_error='';db.flush()
            affected_run_ids,affected_note_ids,artifact_plan,removed=_database_phase(db,w,token)
            db.commit()
        except Exception as e:
            db.rollback();logging.exception('withdrawal database phase failed')
            with Session() as db2:
                w2=db2.get(Withdrawal,withdrawal_id)
                if w2 and w2.state!='completed':
                    w2.state='cleaning';w2.last_error=str(e)[:2000];w2.updated_at=now();db2.commit()
            raise HTTPException(500,'数据库清理中断，可安全重试，已阻断的数据不会复活')
        purged=_file_phase(withdrawal_id,artifact_plan)
        with Session() as db:
            w=db.get(Withdrawal,withdrawal_id,with_for_update=True)
            findings=_verify(db,w,token,artifact_plan)
            w.residual_findings=findings;w.updated_at=now()
            if not findings:
                w.state='completed';w.completed_at=now();w.last_error=''
                log(db,'system','withdrawal.completed',w.visitor_hash,
                    json.dumps({'removed_ratings':w.removed_ratings,'runs':len(w.affected_runs),
                                'notes':len(w.affected_notes),'artifacts':len(w.purged_artifacts)},ensure_ascii=False))
            else:
                w.state='cleaning';w.last_error='残留核验未通过，已保留阻断状态，请重试清理'
                log(db,'system','withdrawal.residual',w.visitor_hash,json.dumps(findings,ensure_ascii=False))
            db.commit()
            return _dto(db,w)
def _recover_token(db,w):
    """中断续清：从尚未清除的评分/快照/留档中取回原始标识（哈希命中即确认）。"""
    target=w.visitor_hash
    for r in db.scalars(select(Record)).all():
        token=(r.values or {}).get('visitor')
        if token and visitor_credential(token)==target:return token
    for run in db.scalars(select(Run)).all():
        token=_find_token(run.snapshot,target)
        if token:return token
        token=_find_token(run.result,target)
        if token:return token
    for n in db.scalars(select(Note)).all():
        token=_find_token(n.frozen_result,target)
        if token:return token
    # 无残留可取：数据已清干净，直接由核验放行
    return ''
def _find_token(obj,target):
    if isinstance(obj,dict):
        token=obj.get('visitor')
        if isinstance(token,str) and visitor_credential(token)==target:return token
        for v in obj.values():
            found=_find_token(v,target)
            if found:return found
    elif isinstance(obj,list):
        for v in obj:
            found=_find_token(v,target)
            if found:return found
    return None
def _wait_for_running(affected_run_ids):
    """独立短事务轮询，避免在持锁事务内看不到 worker 的提交。"""
    if not affected_run_ids:return
    for _ in range(60):
        with Session() as probe:
            running=probe.scalars(select(Run.id).where(Run.id.in_(sorted(affected_run_ids)),Run.state=='running')).all()
        if not running:return
        time.sleep(1)
    raise RuntimeError('受影响研究仍在计算，清理已保留阻断状态，请稍后重试')
def _database_phase(db,w,token):
    if not token:return [],[],list(w.artifact_plan or []),0
    # 1. 队列防御：旧任务重放项直接摘除，worker 侧还有二次拦截
    affected_run_ids=set()
    for run in db.scalars(select(Run)).all():
        if _run_contains(run,token):affected_run_ids.add(run.id)
    for rid in sorted(affected_run_ids):
        try:queue.lrem('analysis:queue',0,str(rid))
        except Exception:logging.exception('queue lrem failed for run %s',rid)
    # 2. 受影响研究：冻结输入与结果整体脱敏，停止取用；不影响无关研究
    artifact_plan=list(w.artifact_plan or [])
    for run in db.scalars(select(Run).where(Run.id.in_(sorted(affected_run_ids)))).all() if affected_run_ids else []:
        run.snapshot=scrub(run.snapshot,token)
        if run.result is not None:run.result=scrub(run.result,token)
        run.state='quarantined'
        run.error='含已撤回授权访客的数据，已停止取用并清除留档'
        name=_artifact_name(run.id)
        if name not in artifact_plan:artifact_plan.append(name)
    # 3. 已批准备选（及一切含该访客的草案留档）停止取用并清除留档
    affected_note_ids=set()
    for n in db.scalars(select(Note)).all():
        if _note_contains(n,token):
            affected_note_ids.add(n.id)
            n.frozen_result={'withdrawn':True}
            n.state='revoked'
            n.approved_by=n.approved_by or ''
    # 4. 跨批次永久删除该访客的全部评分。删除只按 visitor 精确值判定，
    #    V-0070 等相似访客（哪怕引用编号里恰好出现 V-007 字样）绝不误删。
    doomed=[r for r in db.scalars(select(Record)).all() if (r.values or {}).get('visitor')==token]
    removed=len(doomed)
    for r in doomed:db.delete(r)
    w.affected_runs=sorted(affected_run_ids)
    w.affected_notes=sorted(affected_note_ids)
    w.removed_ratings=removed
    w.artifact_plan=artifact_plan
    db.flush()
    return sorted(affected_run_ids),sorted(affected_note_ids),artifact_plan,removed
def _file_phase(withdrawal_id,artifact_plan):
    """文件清除独立于数据库事务，逐个登记；中断后按计划续删，不重复、不漏删。"""
    for name in artifact_plan:
        path=ARTIFACT_DIR/name
        try:
            if path.exists():path.unlink()
        except OSError:logging.exception('artifact purge failed: %s',name);continue
        with Session() as db:
            w=db.get(Withdrawal,withdrawal_id)
            if not w:continue
            purged=list(w.purged_artifacts or [])
            if name not in purged:
                purged.append(name);w.purged_artifacts=purged;w.updated_at=now();db.commit()
    with Session() as db:
        return list(db.get(Withdrawal,withdrawal_id).purged_artifacts or [])
def _verify(db,w,token,artifact_plan):
    """只有残留核验通过才允许报完成。发现项只记主键，不回写访客标识。"""
    findings=[]
    if token:
        for r in db.scalars(select(Record)).all():
            if contains_token(r.values,token):findings.append({'type':'rating_record','id':r.id})
        for run in db.scalars(select(Run)).all():
            if contains_token(run.snapshot,token) or contains_token(run.result,token):
                findings.append({'type':'run','id':run.id})
        for n in db.scalars(select(Note)).all():
            if contains_token(n.frozen_result,token):findings.append({'type':'note','id':n.id})
    for name in artifact_plan:
        path=ARTIFACT_DIR/name
        if path.exists():
            try:
                if _mentions(path.read_text(encoding='utf-8',errors='ignore'),token):
                    findings.append({'type':'artifact_file','name':name})
            except OSError:findings.append({'type':'artifact_file','name':name})
    # 队列里不应再有待执行的受影响任务（防重放复活）
    for rid in (w.affected_runs or []):
        try:
            entries={str(x,encoding='utf-8') if isinstance(x,(bytes,bytearray)) else str(x) for x in queue.lrange('analysis:queue',0,-1)}
            if str(rid) in entries:
                findings.append({'type':'queue_entry','id':rid})
        except Exception:findings.append({'type':'queue_unavailable','id':rid})
    return findings
def resume_all_pending():
    """启动恢复：把中断在 blocked/cleaning 的撤回单续清到终态。"""
    with Session() as db:
        ids=[x for x in db.scalars(select(Withdrawal.id).where(Withdrawal.state!='completed')).all()]
    for withdrawal_id in ids:
        try:advance_withdrawal(withdrawal_id)
        except Exception:logging.exception('resume withdrawal %s failed',withdrawal_id)
def list_withdrawals(db):
    return [_dto(db,w) for w in db.scalars(select(Withdrawal).order_by(Withdrawal.id.desc()))]
def get_withdrawal(db,withdrawal_id):
    w=db.get(Withdrawal,withdrawal_id)
    if not w:raise HTTPException(404,'未找到撤回请求')
    return _dto(db,w)
def retry_withdrawal(db,withdrawal_id,token):
    w=db.get(Withdrawal,withdrawal_id)
    if not w:raise HTTPException(404,'未找到撤回请求')
    if w.state=='completed':return _dto(db,w)
    if token:
        if visitor_credential(token.strip())!=w.visitor_hash:
            raise HTTPException(422,'访客标识与该撤回单的匿名凭据不一致')
        token=token.strip()
    db.commit()
    return advance_withdrawal(withdrawal_id,token)
def _dto(db,w):
    d=row(w);d.pop('visitor_hash',None)
    d['credential']=w.visitor_hash[:12]
    d['artifact_total']=len(w.artifact_plan or [])
    d['artifact_purged']=len(w.purged_artifacts or [])
    return d
