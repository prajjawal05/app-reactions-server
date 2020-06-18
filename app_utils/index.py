from datetime import datetime


def get_key_value_for_primary_key(rowKey):
    return {
        "dataType": rowKey
    }


def get_timestamp():
    return ceil(datetime.now().timestamp())