import asyncio
import datetime
import bson
import pandas as pd


async def read_bson_file(file_path):
    with open(file_path, 'rb') as file:
        return bson.decode_all(file.read())


async def aggregate_salary_data(bson_file_path, dt_from, dt_upto, group_type):
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

        return result

    return None


async def main():
    # Test data
    dt_from = datetime.datetime(2022, 9, 1, 0, 0, 0)
    dt_upto = datetime.datetime(2022, 12, 31, 23, 59, 0)
    group_type = 'month'

    # Call function
    result = await aggregate_salary_data('sample_collection.bson', dt_from, dt_upto, group_type)

    exp_output = {
        "dataset": [5906586, 5515874, 5889803, 6092634],
        "labels": ["2022-09-01T00:00:00", "2022-10-01T00:00:00", "2022-11-01T00:00:00", "2022-12-01T00:00:00"]
    }

    print(result == exp_output)


asyncio.run(main())
