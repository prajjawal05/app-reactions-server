import json
from functools import reduce
from app_utils.index import get_key_value_for_primary_key, DecimalEncoder
from config.constants import  RowKeys
from db_layer import get_items_from_table, update_item_in_table


def _update_choices_in_table(req_id, choices, op_type):
    update_expressions = []
    
    print("Updating Curr Tick: {}".format(req_id))

    for choice in choices.keys():
        print("Adding: {}".format(req_id))
        update_expressions.append("{r} = {r} {op_type} :{r}".format(r=choice, op_type=op_type))
        print("Update Expressions: {}".format(update_expressions))

    update_expression = "SET {}".format(", ".join(update_expressions))

    print("Updating Curr Tick update_expressions: {}".format(update_expression))
    values_for_update_expression = dict(reduce(lambda acc, choice:
                                               acc + [
                                                   (":{}".format(choice.value), choices[choice.value])],
                                               choices.keys(), []))

    print("Updating Table: {}".format(values_for_update_expression))

    update_item_in_table(req_id, get_key_value_for_primary_key(RowKeys.CURR_TICK.value), update_expression,
                         values_for_update_expression)


def get_choices(req_id, body):
    curr_tick_reactions_doc = get_items_from_table(req_id)
    return json.dumps(curr_tick_reactions_doc, cls=DecimalEncoder)


def update_choices(req_id, choices):
    print("Update Aggreagations: {}".format(req_id))
    _update_choices_in_table(req_id, choices, "+")
    return "SUCCESS"
