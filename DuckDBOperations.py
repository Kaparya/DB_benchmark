import duckdb
from time import time


def DuckDBFirstQuery(data):
    duckdb.sql("SELECT VendorID, count(*) FROM data GROUP BY 1;")


def DuckDBSecondQuery(data):
    duckdb.sql("""SELECT passenger_count, avg(total_amount) 
                  FROM data 
                  GROUP BY 1;""")


def DuckDBThirdQuery(data):
    duckdb.sql("""SELECT
                       passenger_count, 
                       extract(year from tpep_pickup_datetime),
                       count(*)
                   FROM data
                   GROUP BY 1, 2;
                  """)


def DuckDBFourthQuery(data):
    duckdb.sql("""SELECT
                       passenger_count,
                       extract(year from tpep_pickup_datetime),
                       round(trip_distance),
                       count(*)
                  FROM data
                  GROUP BY 1, 2, 3
                  ORDER BY 2, 4 desc;
                  """)


def CheckDuckDB(tries, big_data=False):
    time_sum = 0
    time_first = 0
    time_second = 0
    time_third = 0
    time_fourth = 0

    if big_data:
        data = duckdb.read_csv('nyc_yellow_big.csv')
    else:
        data = duckdb.read_csv('nyc_yellow_tiny.csv')

    for i in range(tries):

        start_time = time()
        DuckDBFirstQuery(data)
        time_first += time() - start_time
        time1 = time()
        DuckDBSecondQuery(data)
        time_second += time() - time1
        time2 = time()
        DuckDBThirdQuery(data)
        time_third += time() - time2
        time3 = time()
        DuckDBFourthQuery(data)
        time_fourth += time() - time3
        finish_time = time()

        time_sum += finish_time - start_time

    return [time_sum / tries,
            time_first / tries,
            time_second / tries,
            time_third / tries,
            time_fourth / tries]