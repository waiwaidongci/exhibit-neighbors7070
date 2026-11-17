from sqlalchemy import Column,Integer,String,JSON,Text
from ..config.database import Base
class Withdrawal(Base):
    __tablename__='visitor_withdrawals'
    id=Column(Integer,primary_key=True)
    # HMAC-SHA256 匿名凭据；原始访客标识永不入库、不入审计
    visitor_hash=Column(String(64),unique=True,nullable=False,index=True)
    # blocked(已阻断) / cleaning(清理中) / completed(已完成)
    state=Column(String(20),default='blocked',nullable=False)
    affected_runs=Column(JSON,nullable=False,default=list)
    affected_notes=Column(JSON,nullable=False,default=list)
    removed_ratings=Column(Integer,default=0,nullable=False)
    artifact_plan=Column(JSON,nullable=False,default=list)
    purged_artifacts=Column(JSON,nullable=False,default=list)
    residual_findings=Column(JSON,nullable=False,default=list)
    last_error=Column(Text,default='')
    created_by=Column(String(120),nullable=False)
    created_at=Column(String(40),nullable=False)
    updated_at=Column(String(40),nullable=False)
    completed_at=Column(String(40),default='')
