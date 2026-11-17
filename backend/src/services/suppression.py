import hmac,hashlib
from fastapi import HTTPException
from sqlalchemy import select
from ..config.settings import JWT_SECRET
from ..models.withdrawal import Withdrawal
# 撤回授权的匿名凭据：以服务端密钥对访客标识做带盐 HMAC，单向不可逆。
_PREFIX='visitor-withdrawal:v1:'
def visitor_credential(token):
    return hmac.new((JWT_SECRET+':withdrawal').encode(),(_PREFIX+token).encode(),hashlib.sha256).hexdigest()
def load_hashes(db):
    return set(db.scalars(select(Withdrawal.visitor_hash)).all())
def hash_is_suppressed(db,visitor_hash):
    return db.scalar(select(Withdrawal.id).where(Withdrawal.visitor_hash==visitor_hash)) is not None
def visitor_is_suppressed(db,token):
    return hash_is_suppressed(db,visitor_credential(token))
def assert_visitor_allowed(db,token):
    # 受理撤回后，任何写入路径（手工追加、重复导入、种子脚本）都在此被阻断
    if token and visitor_is_suppressed(db,token):
        raise HTTPException(409,'该访客已撤回试展授权，其数据禁止再次进入系统')
def active_withdrawals(db):
    return list(db.scalars(select(Withdrawal).where(Withdrawal.state!='completed')))
