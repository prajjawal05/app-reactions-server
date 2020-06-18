import boto3
from config.constants import RowKeys
from config.env import tableName

_table = None

def _get_table():
    if _table is None:
        _table = boto3.client('dynamodb').Table(tableName)
    return _table


def get_items_from_table(req_id):
    log_line(req_id, "Getting PrimaryKey: {}".format(table_name, key))
    return _get_table().batch_get_item(
        RequestItems={
            tableName: {
                'Keys': list(map(lambda rowKey: {
                    "dataType": {
                        'S': rowKey.value
                    }
                } , RowKeys)),
                'ConsistentRead': True
            }
        }
    )['Responses'][tableName]


def update_item_in_table(req_id, key, update_expression, values_for_update_expression):
    log_line(req_id,
             "Updating TableName: {} Key: {} Update Expression: {} Values TTo Update: {}".format(table_name, key
                                                                                                 , update_expression,
                                                                                                 values_for_update_expression))
    _get_table().update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=values_for_update_expression
    )

get_items_from_table("AA")