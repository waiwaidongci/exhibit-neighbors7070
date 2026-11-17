from fastapi import APIRouter,Depends
from ..config.database import db_session
from ..schemas.withdrawal import WithdrawalInput,WithdrawalRetry
from ..services.auth import admin
from ..controllers.withdrawals import WithdrawalsController as C
router=APIRouter()
@router.get('/withdrawals')
def all_withdrawals(db=Depends(db_session),u=Depends(admin)):return C.list(db)
@router.post('/withdrawals')
def create(data:WithdrawalInput,db=Depends(db_session),u=Depends(admin)):return C.create(db,u,data)
@router.get('/withdrawals/{id}')
def detail(id:int,db=Depends(db_session),u=Depends(admin)):return C.get(db,id)
@router.post('/withdrawals/{id}/retry')
def retry(id:int,data:WithdrawalRetry,db=Depends(db_session),u=Depends(admin)):return C.retry(db,id,data)
