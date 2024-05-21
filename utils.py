import json
from datetime import datetime, timedelta
from json import JSONDecodeError

from db_connection import mongodb


async def msg_validation(msg: str) -> dict:
    msg_dict = json.loads(msg)
    if type(msg_dict) is not dict:
        raise ValueError('Invalid input data')
    if 'dt_from' not in msg_dict or 'dt_upto' not in msg_dict or 'group_type' not in msg_dict:
        raise ValueError('Invalid input data')
    if msg_dict['group_type'] not in ('hour', 'day', 'week', 'month'):
        raise ValueError('"group_type" must be one of "hour", "day", "week", "month"')
    return msg_dict


def generate_time_labels(start: datetime, end: datetime, group_type: str):
    current = start
    labels = []
    if group_type == 'hour':
        delta = timedelta(hours=1)
        date_format = "%Y-%m-%dT%H:00:00"
    elif group_type == 'day':
        delta = timedelta(days=1)
        date_format = "%Y-%m-%dT00:00:00"
    elif group_type == 'week':
        current = current - timedelta(days=current.weekday())
        delta = timedelta(weeks=1)
        date_format = "%Y-%m-%dT00:00:00"
    else:
        delta = timedelta(days=1)
        date_format = "%Y-%m-01T00:00:00"
        current = current.replace(day=1)

    while current <= end:
        labels.append(current.strftime(date_format))
        if group_type == 'month':
            next_month = current.replace(day=28) + timedelta(days=4)
            current = next_month - timedelta(days=next_month.day - 1)
        else:
            current += delta

    return labels


async def data_aggregate(dt_from: datetime, dt_upto: datetime, group_type: str) -> dict:

    if group_type == 'hour':
        grouping = {'year': {'$year': '$dt'}, 'month': {'$month': '$dt'},
                    'day': {'$dayOfYear': '$dt'}, 'hour': {'$hour': '$dt'}}
        date_format = "%Y-%m-%dT%H:00:00"
    elif group_type == 'day':
        grouping = {'year': {'$year': '$dt'}, 'day': {'$dayOfYear': '$dt'}}
        date_format = "%Y-%m-%dT00:00:00"
    elif group_type == 'week':
        grouping = {'year': {'$isoWeekYear': '$dt'}, 'week': {'$isoWeek': '$dt'}}
        date_format = "%Y-%m-%dT00:00:00"
    else:
        grouping = {'year': {'$year': '$dt'}, 'month': {'$month': '$dt'}}
        date_format = "%Y-%m-01T00:00:00"

    pipeline = [
        {'$match': {
            'dt': {'$gte': dt_from,
                   "$lte": dt_upto}}},
        {'$group': {'_id': grouping,
                    'label': {'$first': {'$dateToString': {'format': date_format, 'date': '$dt'}}},
                    'total': {'$sum': '$value'}}},
        {'$sort': {'_id': 1}}
    ]
    if group_type == 'week':
        pipeline.append({'$addFields': {
            'first_day_of_week': {
                '$dateFromParts': {
                    'isoWeekYear': '$_id.year',
                    'isoWeek': '$_id.week',
                    'isoDayOfWeek': 1
                }
            }
        }})
        pipeline.append({'$project': {
            '_id': 1,
            'label': {'$dateToString': {'format': date_format, 'date': '$first_day_of_week'}},
            'total': 1
        }})

    cursor = mongodb.collection.aggregate(pipeline)
    aggregated_data = await cursor.to_list(length=None)
    result = {doc['label']: doc['total'] for doc in aggregated_data}
    return result


async def combine_data(msg: str) -> dict:

    try:
        msg_dict = await msg_validation(msg)
    except JSONDecodeError:
        return {'error': 'Invalid input data'}
    except ValueError as e:
        return {'error': e.args[0]}
    except TypeError as e:
        return {'error': e.args[0]}

    msg_dict['dt_from'] = datetime.fromisoformat(msg_dict['dt_from'])
    msg_dict['dt_upto'] = datetime.fromisoformat(msg_dict['dt_upto'])

    aggregate_result = await data_aggregate(msg_dict['dt_from'], msg_dict['dt_upto'], msg_dict['group_type'])
    labels = generate_time_labels(msg_dict['dt_from'], msg_dict['dt_upto'], msg_dict['group_type'])
    dataset = [aggregate_result.get(label, 0) for label in labels]
    return {'dataset': dataset, 'labels': labels}
