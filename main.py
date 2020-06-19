from random import getrandbits
from app_utils.logging import log_line
from handlers import get_reactions, update_reactions, update_stop_time

op_func_map = {
    "get": get_reactions,
    "update": update_reactions,
    "stop": update_stop_time
}


def lambda_handler(event, context):
    req_id = getrandbits(8)
    operation_type = event['queryStringParameters']['op']
    body = event["body"]

    log_line(req_id, "Body: {} OperationType: {}".format(body, operation_type))
    response_body = op_func_map[operation_type](req_id, body)

    return {
        'statusCode': 200,
        'body': response_body
    }