from enum import Enum

class RowKeys(Enum):
    AGGREGATED = "AGGREGATED"
    CURR_TICK = "CURR_TICK"
    PREV_TICK = "PREV_TICK"

class ReactionTypes(Enum):
    APPRECIATE = "APPRECIATE"
    LAUGH = "LAUGH"
    CLAP = "CLAP"