import bson
import json
import pandas as pd


async def read_bson_file(file_path) -> list:
    """
    Read bson file
    :param file_path: file path
    :return: bson data
    """
    with open(file_path, 'rb') as file:
        return bson.decode_all(file.read())


async def aggregate_salary_data(bson_file_path, dt_from, dt_upto, group_type) -> dict or None:
    """
    Aggregate salary data
    :param bson_file_path: bson file path
    :param dt_from: datetime from
    :param dt_upto: datetime upto
    :param group_type: group type (hour, day, month)
    :return: dict or None
    """
    bson_data = await read_bson_file(bson_file_path)

    # Convert to DataFrame
    df = pd.DataFrame(bson_data)

    # Filter data based on date range
    df_filtered = df[(df['dt'] >= dt_from) & (df['dt'] <= dt_upto)]

    # Get frequency
    freq = None
    if group_type == 'hour':
        freq = 'h'
    elif group_type == 'day':
        freq = 'D'
    elif group_type == 'month':
        freq = 'ME'

    if freq is not None:
        grouped = df_filtered.groupby(pd.Grouper(key='dt', freq=freq))['value'].sum()

        # Fill missing hours
        idx = pd.date_range(dt_from, dt_upto, freq=freq)
        grouped = grouped.reindex(idx, fill_value=0)

        # If month change index to start of month
        if group_type == 'month':
            grouped.index = grouped.index.to_period('M').to_timestamp()

        # Convert to dict
        result = {
            'dataset': grouped.tolist(),
            'labels': grouped.index.strftime('%Y-%m-%dT%H:%M:%S').tolist()
        }

        return json.dumps(result)

    return None


async def parse_json_text(text) -> tuple:
    """
    Parse JSON message
    :param text: message text
    :return: dt_from, dt_upto, group_type
    """
    try:
        data = json.loads(text)
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']

        return dt_from, dt_upto, group_type
    except Exception as e:
        print(e)
        return None, None, None
