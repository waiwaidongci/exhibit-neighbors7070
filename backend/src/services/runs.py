from sqlalchemy import select
from fastapi import HTTPException
from redis import Redis
from ..config.settings import REDIS_URL
from ..models.run import Run
from ..utils.serialization import row,now
from .sources import source_or_404,list_records
from .domain_validation import validate_parameters
from .audit import log
from .suppression import load_hashes,visitor_credential,active_withdrawals
queue=Redis.from_url(REDIS_URL)
def get_run(db,id):
    r=db.get(Run,id)
    if not r:raise HTTPException(404,'未找到分析任务')
    return r
def create_run(db,user,data):
    s=source_or_404(db,data.source_id);records=list_records(db,s.id)
    if len(records)<3:raise HTTPException(422,'至少录入3条样本后才能执行')
    # 撤回受理即冻结：清理未走完前不接受新研究，已撤回访客的历史评分不得进入新快照
    if active_withdrawals(db):raise HTTPException(409,'存在进行中的授权撤回清理，请在清理完成后再冻结研究')
    suppressed=load_hashes(db)
    if suppressed:
        for r in records:
            if visitor_credential(r['values']['visitor']) in suppressed:
                raise HTTPException(409,'来源含已撤回授权访客的评分，禁止冻结为新研究输入')
    try: params=validate_parameters(data.parameters)
    except ValueError as e:raise HTTPException(422,str(e))
    r=Run(source_id=s.id,name=data.name,parameters=params,snapshot={'source':row(s),'records':records},created_at=now(),state='queued')
    db.add(r);db.flush();log(db,user,'run.queued',r.id,data.name);db.commit()
    try:queue.rpush('analysis:queue',r.id)
    except Exception:
        r.state='failed';r.error='队列暂不可用，请重新创建任务';db.commit();raise HTTPException(503,r.error)
    return row(r)
def list_runs(db):
    return [{k:v for k,v in row(r).items() if k not in ('snapshot','result')} for r in db.scalars(select(Run).order_by(Run.id.desc()))]
