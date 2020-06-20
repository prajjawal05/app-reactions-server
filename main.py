import json
from random import getrandbits
from app_utils.logging import log_line
from handlers import maintain_window, update_reactions, update_stop_time

op_func_map = {
    "maintain": maintain_window,
    "update": update_reactions,
    "stop": update_stop_time
}


def lambda_handler(event, context):
    req_id = getrandbits(32)
    operation_type = event['queryStringParameters']['op']
    body = json.loads(event['body'])

    log_line(req_id, "Body: {} OperationType: {}".format(body, operation_type))
    response_body = op_func_map[operation_type](req_id, body)

    return {
        'statusCode': 200,
        'body': response_body
    }