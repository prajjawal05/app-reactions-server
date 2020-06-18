from functools import reduce
from math import ceil
from app_utils.index import get_key_value_for_primary_key, get_timestamp
from config.constants import ReactionTypes, RowKeys
from db_layer import get_items_from_table, update_item_in_table


def get_reactions(req_id):
    row_items = get_items_from_table(req_id)
    const curr_timestamp = get_timestamp()
    # if curr_timestamp - agg_record['lastUpdatedTimestamp'] > 1000:
    #     print(curr_timestamp)




def update_reactions(req_id, body):
    reactions = body['reactions']
    
    update_expressions = []
    for reactionType in ReactionTypes:
        update_expressions.append("{r} = {r} + :{r}".format(r=reactionType.value))

    update_expression = "SET {}, version = version + 1".format(", ".join(update_expressions))

    values_for_update_expression = dict(reduce(lambda acc, reactionType:
        acc + [(":{}".format(reactionType), reactions[reactionType])], list(reactions.keys()), []))

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.CURR_TICK.value), update_expression, values_for_update_expression)

    return "SUCCESS"


def update_stop_time(req_id):
    update_expression = "SET lastStoppedTime = :currTimestamp"
    values_for_update_expression = {
        ":currTimeStamp": get_timestamp()
    }

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.AGGREGATED.value), update_expression, values_for_update_expression)

    return "SUCCESS"