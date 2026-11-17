from enum import Enum
class Role(str,Enum):
    admin='admin'; analyst='analyst'; viewer='viewer'
class RunState(str,Enum):
    queued='queued'; running='running'; completed='completed'; failed='failed'; quarantined='quarantined'
class ReviewState(str,Enum):
    draft='draft'; approved='approved'; revoked='revoked'
class WithdrawalState(str,Enum):
    blocked='blocked'; cleaning='cleaning'; completed='completed'
