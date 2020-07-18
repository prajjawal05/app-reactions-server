import json
from random import getrandbits
from app_utils.logging import log_line
from handlers import get_choices, update_choices

op_func_map = {
    "update": update_choices,
    "get": get_choices
}


def lambda_handler(event, context):
    req_id = getrandbits(32)
    print("Event: {} ReqId: {}".format(event, req_id))
    operation_type = event['queryStringParameters']['op']
    body = json.loads(event["body"])

    try: 
        log_line(req_id, "Body: {} OperationType: {}".format(body, operation_type))
        response_body = op_func_map[operation_type](req_id, body)
        return {
            'statusCode': 200,
            'body': response_body
        }
    except:
        return {
            'statusCode': 500,
            'body': req_id
        }