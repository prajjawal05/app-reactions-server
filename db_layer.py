import boto3
from app_utils.index import serialize_to_db_objects, deserialize_db_objects
from app_utils.logging import log_line
from config.constants import RowKeys
from config.env import table_name

_dynamo_client = None


def _get_client():
    global _dynamo_client
    if _dynamo_client is None:
        _dynamo_client = boto3.client('dynamodb')
    return _dynamo_client


def get_items_from_table(req_id):
    log_line(req_id, "Getting Table: {}".format(table_name))
    response_array = _get_client().batch_get_item(
        RequestItems={
            table_name: {
                'Keys': list(map(lambda rowKey: {
                    "dataType": {
                        'S': rowKey.value
                    }
                }, RowKeys)),
                'ConsistentRead': False
            }
        }
    )['Responses'][table_name]

    curr_tick_record = ""
    for serialised_response in response_array:
        curr_tick_record = deserialize_db_objects(serialised_response)

    return curr_tick_record


def update_item_in_table(req_id, key, update_expression, values_for_update_expression):
    log_line(req_id,
             "Updating TableName: {} Key: {} Update Expression: {} Values To Update: {}".format(
                 table_name, serialize_to_db_objects(key), update_expression,
                 serialize_to_db_objects(values_for_update_expression)))
    _get_client().update_item(
        TableName=table_name,
        Key=serialize_to_db_objects(key),
        UpdateExpression=update_expression,
        ExpressionAttributeValues=serialize_to_db_objects(values_for_update_expression)
    )
