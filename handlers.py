from functools import reduce
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from app_utils.index import get_key_value_for_primary_key, get_timestamp, deserialize_db_objects
from app_utils.logging import log_line
from config.constants import ReactionTypes, RowKeys
from db_layer import get_items_from_table, update_item_in_table


def _update_window(req_id, window_reactions, curr_tick_reactions, operation_timestamp):
    update_expressions = []
    values_for_update_expression = {
        ":lastUpdatedTimestamp": operation_timestamp
    }

    for reaction_type in ReactionTypes:
        window_reactions[reaction_type.value].append(curr_tick_reactions[reaction_type.value])
        if len(window_reactions[reaction_type.value]) > 0:
            window_reactions[reaction_type.value].pop(0)
        update_expressions.append("{r} = :{r}".format(r=reaction_type.value))
        values_for_update_expression[reaction_type.value] = window_reactions[reaction_type.value]

    update_expression = "SET {}, lastUpdatedTimestamp = :lastUpdatedTimestamp".format(", ".join(update_expressions))

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.WINDOW.value), update_expression,
                         values_for_update_expression)


def _update_reactions_for_curr_tick_in_table(req_id, reactions, op_type):
    update_expressions = []
    for reactionType in ReactionTypes:
        update_expressions.append("{r} = {r} {op_type} :{r}".format(r=reactionType.value, op_type=op_type))

    update_expression = "SET {}".format(", ".join(update_expressions))

    values_for_update_expression = dict(reduce(lambda acc, reactionType:
                                               acc + [(":{}".format(reactionType.value), reactions[reactionType.value])],
                                               ReactionTypes, []))

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.CURR_TICK.value), update_expression,
                         values_for_update_expression)


def _mark_window_as_in_progress(req_id):
    update_expression = "SET isCurrentlyUpdating = :isCurrentlyUpdating"
    values_for_update_expression = {
        ":isCurrentlyUpdating": True
    }
    condition_to_check = Attr('isCurrentlyUpdating').eq(False)
    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.WINDOW.value), update_expression,
                         values_for_update_expression, condition_to_check)


def _try_updating_window_and_resetting_tick(req_id, window_reactions, curr_tick_reactions, operation_timestamp):
    try:
        _mark_window_as_in_progress(req_id)
    except ClientError as err:
        log_line(req_id, "Window Updation already in Progress")
    else:
        _update_window(req_id, window_reactions, curr_tick_reactions, operation_timestamp)
        _update_reactions_for_curr_tick_in_table(req_id, curr_tick_reactions, "-")


def _sum_up_reactions(window_reactions, curr_tick_reactions):
    return True


def get_reactions(req_id):
    serialised_window_reactions, serialised_curr_tick_reactions = get_items_from_table(req_id)
    curr_timestamp = get_timestamp()
    window_reactions = deserialize_db_objects(serialised_window_reactions)
    curr_tick_reactions = deserialize_db_objects(serialised_curr_tick_reactions)
    aggregated_reactions = _sum_up_reactions(window_reactions, curr_tick_reactions)

    if not window_reactions["isCurrentlyUpdating"] or curr_timestamp - window_reactions['lastUpdatedTimestamp'] > 1000:
        _try_updating_window_and_resetting_tick(window_reactions, curr_tick_reactions, curr_timestamp)

    return aggregated_reactions


def update_reactions(req_id, body):
    reactions = body['reactions']
    _update_reactions_for_curr_tick_in_table(req_id, reactions, "+")
    return "SUCCESS"


def update_stop_time(req_id):
    update_expression = "SET lastStoppedTime = :currTimestamp"
    values_for_update_expression = {
        ":currTimeStamp": get_timestamp()
    }

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.WINDOW.value), update_expression,
                         values_for_update_expression)

    return "SUCCESS"
