from datetime import datetime
from math import ceil
from config.constants import deserializer, serializer


def get_key_value_for_primary_key(rowKey):
    return {
        "dataType": rowKey
    }


def get_timestamp():
    return ceil(datetime.now().timestamp())


def deserialize_db_objects(db_object):
    return {
        k: deserializer.deserialize(v) for k, v in db_object.items()
    }


def serialize_to_db_objects(data_object):
    return {
        k: serializer.serialize(v) for k, v in data_object.items()
    }
