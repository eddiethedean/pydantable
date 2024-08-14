from collections import defaultdict


class ValidationErrors(Exception):
    ...
    

def combine_errors(errors) -> dict:
    if len(errors) == 0: return {}
    inputs = [error['input'] for error in errors]
    first = errors[0].copy()
    del first['input']
    first['inputs'] = inputs
    return first

def group_errors(errors) -> list[dict]:
    e_data = defaultdict(list)
    for error in errors:
        col = error['loc'][0]
        tp = error['type']
        value = error.copy()
        e_data[(col, tp)].append(value)
        
    for key in e_data.keys():
        e_data[key] = combine_errors(e_data[key])

    return list(e_data.values())