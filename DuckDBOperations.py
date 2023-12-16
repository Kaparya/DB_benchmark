import duckdb
from time import time


def DuckDBFirstQuery(data, prints):
    result = duckdb.sql("SELECT VendorID, count(*) FROM data GROUP BY 1;")
    if prints:
        result.show()


def DuckDBSecondQuery(data, prints):
    result = duckdb.sql("""SELECT passenger_count, avg(total_amount) 
                  FROM data 
                  GROUP BY 1;""")
    if prints:
        result.show()


def DuckDBThirdQuery(data, prints):
    result = duckdb.sql("""SELECT
                       passenger_count, 
                       extract(year from tpep_pickup_datetime),
                       count(*)
                   FROM data
                   GROUP BY 1, 2;
                  """)
    if prints:
        result.show()


def DuckDBFourthQuery(data, prints):
    result = duckdb.sql("""SELECT
                       passenger_count,
                       extract(year from tpep_pickup_datetime),
                       round(trip_distance),
                       count(*)
                  FROM data
                  GROUP BY 1, 2, 3
                  ORDER BY 2, 4 desc;
                  """)
    if prints:
        result.show()


def CheckDuckDB(tries, big_data=False):
    prints = 0

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
        DuckDBFirstQuery(data, prints)
        time_first += time() - start_time
        time1 = time()
        DuckDBSecondQuery(data, prints)
        time_second += time() - time1
        time2 = time()
        DuckDBThirdQuery(data, prints)
        time_third += time() - time2
        time3 = time()
        DuckDBFourthQuery(data, prints)
        time_fourth += time() - time3
        finish_time = time()

        time_sum += finish_time - start_time

    return [time_sum / tries,
            time_first / tries,
            time_second / tries,
            time_third / tries,
            time_fourth / tries]