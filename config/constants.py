from enum import Enum
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

deserializer = TypeDeserializer()
serializer = TypeSerializer()


class RowKeys(Enum):
    WINDOW = "WINDOW"
    CURR_TICK = "CURR_TICK"


class ReactionTypes(Enum):
    APPRECIATE = "APPRECIATE"
    LAUGH = "LAUGH"
    CLAP = "CLAP"
