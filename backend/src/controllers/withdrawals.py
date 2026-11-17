from ..services import erasure
class WithdrawalsController:
    list=staticmethod(erasure.list_withdrawals)
    get=staticmethod(erasure.get_withdrawal)
    @staticmethod
    def create(db,user,data):return erasure.create_withdrawal(db,user,data.visitor)
    @staticmethod
    def retry(db,id,data):return erasure.retry_withdrawal(db,id,(data.visitor or '').strip())
