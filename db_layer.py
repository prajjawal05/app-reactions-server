import boto3
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

    return response_array


def update_item_in_table(req_id, key, update_expression, values_for_update_expression, condition_to_check=""):
    log_line(req_id,
             "Updating TableName: {} Key: {} Update Expression: {} Values To Update: {} Condition To Check :{}".format(
                 table_name, key, update_expression,
                 values_for_update_expression, condition_to_check))
    _get_client().Table(table_name).update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=values_for_update_expression,
        ConditionExpression=condition_to_check
    )
