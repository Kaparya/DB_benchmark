import psycopg2
import sqlite3
import duckdb
import PandasOperations
from pandas import read_csv
import sqlalchemy


time_pandas = PandasOperations.CheckPandas(3, False)

print(time_pandas)
