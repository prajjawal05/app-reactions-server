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

    log_line(req_id, "Body: {} OperationType: {}".format(event["body"], operation_type))
    
    response_body = op_func_map(req_id, body)

    return {
        'statusCode': 200,
        'body': json.dumps(response_body)
    }