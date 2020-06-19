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
                'ConsistentRead': True
            }
        }
    )['Responses'][table_name]

    window_record = ""
    curr_tick_record = ""
    for serialised_response in response_array:
        response = deserialize_db_objects(serialised_response)
        if response['dataType'] == RowKeys.WINDOW.value:
            window_record = response
        else:
            curr_tick_record = response

    return window_record, curr_tick_record


def update_item_in_table_conditionally(req_id, key, update_expression, values_for_update_expression,
                                       condition_to_check):
    log_line(req_id,
             "Updating TableName: {} Key: {} Update Expression: {} Values To Update: {} Condition To Check :{}".format(
                 table_name, serialize_to_db_objects(key), update_expression,
                 serialize_to_db_objects(values_for_update_expression), condition_to_check))
    _get_client().update_item(
        TableName=table_name,
        Key=serialize_to_db_objects(key),
        UpdateExpression=update_expression,
        ExpressionAttributeValues=serialize_to_db_objects(values_for_update_expression),
        ConditionExpression=condition_to_check
    )


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
