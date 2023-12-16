import pandas as pd
from time import time


def PandasFirstRequest(data, prints):
    selected_data = data[['VendorID']]
    grouped_data = selected_data.groupby('VendorID')
    final_data = grouped_data.size().reset_index(name='counts')
    if prints:
        print(final_data)


def PandasSecondRequest(data, prints):
    selected_data = data[['passenger_count', 'total_amount']]
    grouped_data = selected_data.groupby('passenger_count')
    final_data = grouped_data.mean().reset_index()
    if prints:
        print(final_data)


def PandasThirdRequest(data, prints):
    selected_data = data[['passenger_count', 'tpep_pickup_datetime']]
    selected_data['year'] = pd.to_datetime(
        selected_data.pop('tpep_pickup_datetime'),
        format='%Y-%m-%d %H:%M:%S').dt.year
    grouped_data = selected_data.groupby(['passenger_count', 'year'])
    final_data = grouped_data.size().reset_index(name='counts')
    if prints:
        print(final_data)


def PandasFourthRequest(data, prints):
    selected_data = data[['passenger_count', 'tpep_pickup_datetime', 'trip_distance']]
    selected_data['trip_distance'] = selected_data['trip_distance'].round().astype(int)
    selected_data['year'] = pd.to_datetime(
        selected_data.pop('tpep_pickup_datetime'),
        format='%Y-%m-%d %H:%M:%S').dt.year
    grouped_data = selected_data.groupby(['passenger_count', 'year', 'trip_distance'])
    final_data = grouped_data.size().reset_index(name='counts')
    final_data = final_data.sort_values(
        ['year', 'counts'],
        ascending=[True, False])
    if prints:
        print(final_data)


def CheckPandas(tries, big_data=False):
    prints = 0

    time_sum = 0
    time_first = 0
    time_second = 0
    time_third = 0
    time_fourth = 0
    pd.options.mode.chained_assignment = None

    if big_data:
        data = pd.read_csv('nyc_yellow_big.csv')
    else:
        data = pd.read_csv('nyc_yellow_tiny.csv')

    for i in range(tries):

        start_time = time()
        PandasFirstRequest(data, prints)
        time_first += time() - start_time
        time1 = time()
        PandasSecondRequest(data, prints)
        time_second += time() - time1
        time2 = time()
        PandasThirdRequest(data, prints)
        time_third += time() - time2
        time3 = time()
        PandasFourthRequest(data, prints)
        time_fourth += time() - time3
        finish_time = time()

        time_sum += finish_time - start_time

    return [time_sum / tries,
            time_first / tries,
            time_second / tries,
            time_third / tries,
            time_fourth / tries]
