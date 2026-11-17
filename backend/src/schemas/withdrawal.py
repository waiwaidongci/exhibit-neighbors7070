from pydantic import BaseModel,Field
class WithdrawalInput(BaseModel):
    visitor:str=Field(min_length=1,max_length=600)
class WithdrawalRetry(BaseModel):
    visitor:str=Field(default='',max_length=600)
