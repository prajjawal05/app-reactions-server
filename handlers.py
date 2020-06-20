import json
from functools import reduce
from botocore.exceptions import ClientError
from app_utils.index import get_key_value_for_primary_key, get_timestamp, DecimalEncoder
from app_utils.logging import log_line
from config.constants import ReactionTypes, RowKeys
from db_layer import get_items_from_table, update_item_in_table, update_item_in_table_conditionally


def _update_window(req_id, window_reactions_doc, curr_tick_reactions_doc, operation_timestamp):
    update_expressions = []
    values_for_update_expression = {
        ":lastUpdatedTimestamp": operation_timestamp,
        ":isCurrentlyUpdating": False
    }

    for reaction_type in ReactionTypes:
        if len(window_reactions_doc[reaction_type.value]) >= 4:
            window_reactions_doc[reaction_type.value].pop(0)
        window_reactions_doc[reaction_type.value].append(curr_tick_reactions_doc[reaction_type.value])
        update_expressions.append("{r} = :{r}".format(r=reaction_type.value))
        values_for_update_expression[":{}".format(reaction_type.value)] = window_reactions_doc[reaction_type.value]

    update_expression = "SET {}, lastUpdatedTimestamp = :lastUpdatedTimestamp, isCurrentlyUpdating=:isCurrentlyUpdating".format(
        ", ".join(update_expressions))

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.WINDOW.value), update_expression,
                         values_for_update_expression)


def _update_reactions_for_curr_tick_in_table(req_id, reactions, op_type):
    update_expressions = []
    
    print("Updating Curr Tick: {}".format(req_id))
    for reactionType in ReactionTypes:
        print("Appending: {}".format(req_id))
        update_expressions.append("{r} = {r} {op_type} :{r}".format(r=reactionType.value, op_type=op_type))
        print("Update Expressions: {}".format(update_expressions))

    update_expression = "SET {}".format(", ".join(update_expressions))

    print("Updating Curr Tick update_expressions: {}".format(update_expression))
    try:
        values_for_update_expression = dict(reduce(lambda acc, reactionType:
                                                   acc + [
                                                       (":{}".format(reactionType.value), reactions[reactionType.value])],
                                                   ReactionTypes, []))
    except Exception as e:
        print("Error {}".format(e))
    print("Updating Table: {}".format(values_for_update_expression))

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.CURR_TICK.value), update_expression,
                         values_for_update_expression)


def _mark_window_as_in_progress(req_id):
    condition_to_check = "isCurrentlyUpdating <> :isCurrentlyUpdating"
    update_expression = "SET isCurrentlyUpdating = :isCurrentlyUpdating"
    values_for_update_expression = {
        ":isCurrentlyUpdating": True
    }
    update_item_in_table_conditionally(req_id, get_key_value_for_primary_key(RowKeys.WINDOW.value), update_expression,
                                       values_for_update_expression, condition_to_check)


def _try_updating_window_and_resetting_tick(req_id, window_reactions_doc, curr_tick_reactions_doc, operation_timestamp):
    try:
        _mark_window_as_in_progress(req_id)
    except ClientError as err:
        log_line(req_id, "Window Updation already in Progress")
    else:
        _update_window(req_id, window_reactions_doc, curr_tick_reactions_doc, operation_timestamp)
        _update_reactions_for_curr_tick_in_table(req_id, curr_tick_reactions_doc, "-")


def _aggregate_reaction(acc, reaction_type, window_reaction, curr_tick_reaction):
    acc[reaction_type] = reduce(lambda x, y: x + y, window_reaction, 0) + curr_tick_reaction
    return acc


def _sum_up_reactions(window_reactions_doc, curr_tick_reactions_doc):
    return reduce(lambda acc, reaction_type: _aggregate_reaction(acc, reaction_type.value,
                                                                 window_reactions_doc[reaction_type.value],
                                                                 curr_tick_reactions_doc[reaction_type.value]),
                  ReactionTypes, {})


def _get_aggregated_reactions(req_id):
    print("Get aggregations".format(req_id))
    window_reactions_doc, curr_tick_reactions_doc = get_items_from_table(req_id)
    aggregated_reactions = _sum_up_reactions(window_reactions_doc, curr_tick_reactions_doc)
    return aggregated_reactions


def maintain_window(req_id, body):
    aggregated_reactions = _get_aggregated_reactions(req_id)
    curr_timestamp = get_timestamp()

    if not window_reactions_doc["isCurrentlyUpdating"] or curr_timestamp - window_reactions_doc[
        'lastUpdatedTimestamp'] > 1:
        _try_updating_window_and_resetting_tick(req_id, window_reactions_doc, curr_tick_reactions_doc, curr_timestamp)

    return json.dumps({**aggregated_reactions, "lastStoppedTime": window_reactions_doc["lastStoppedTimestamp"]},
                      cls=DecimalEncoder)


def update_reactions(req_id, reactions):
    print("Update Aggreagations: {}".format(req_id))
    _update_reactions_for_curr_tick_in_table(req_id, reactions, "+")
    return _get_aggregated_reactions(req_id)


def update_stop_time(req_id, body):
    update_expression = "SET lastStoppedTimestamp = :currTimestamp"
    values_for_update_expression = {
        ":currTimestamp": get_timestamp()
    }

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.WINDOW.value), update_expression,
                         values_for_update_expression)

    return "SUCCESS"