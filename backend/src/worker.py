import json,time,logging
from redis import Redis
from sqlalchemy import select
from .config.database import Session
from .config.settings import REDIS_URL,ARTIFACT_DIR
from .models.run import Run
from .models import source,record,user,note,audit
from .services.algorithm import calculate
from .services.audit import log
from .services.export import csv_response
from .services.suppression import load_hashes,visitor_credential
queue=Redis.from_url(REDIS_URL)
ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)
def _snapshot_tokens(snapshot):
    return {rec['values']['visitor'] for rec in (snapshot or {}).get('records',[])}
def execute(id):
    with Session() as db:
        r=db.get(Run,id)
        if not r or r.state!='queued':return
        # 第一道闸门：撤回受理后，旧队列项（含重复重放）在此丢弃，不进入计算
        suppressed=load_hashes(db)
        if suppressed and any(visitor_credential(t) in suppressed for t in _snapshot_tokens(r.snapshot)):
            r.state='quarantined';r.error='任务含已撤回授权访客的数据，已停止取用'
            log(db,'worker','run.quarantined',r.id);db.commit();return
        r.state='running';db.commit()
        try:
            result=calculate(r.snapshot['records'],r.parameters)
            json.dumps(result,allow_nan=False)
            # 第二道闸门：计算期间撤回被受理则禁止发布旧结果，交由清理流程续清
            db.expire_all()
            r=db.get(Run,id)
            suppressed=load_hashes(db)
            if r.state!='running' or (suppressed and any(visitor_credential(t) in suppressed for t in _snapshot_tokens(r.snapshot))):
                if r.state=='running':
                    r.state='quarantined';r.error='撤回授权在计算期间生效，结果禁止发布'
                    log(db,'worker','run.quarantined',r.id)
                db.commit();return
            r.result=result;r.state='completed'
            response=csv_response(result,'run.csv')
            (ARTIFACT_DIR/f'run-{r.id}.csv').write_bytes(response.body)
            log(db,'worker','run.completed',r.id)
        except Exception as e:
            logging.exception('analysis failed');r.state='failed';r.error=str(e)[:2000]
            log(db,'worker','run.failed',r.id,r.error)
        db.commit()
def main():
    while True:
        (ARTIFACT_DIR/'heartbeat').write_text(str(time.time()))
        value=queue.blpop('analysis:queue',timeout=2)
        if value:execute(int(value[1]))
if __name__=='__main__':main()
