from PandasOperations import CheckPandas
from DuckDBOperations import CheckDuckDB
from PsycopgOperations import CheckPostgres
from SqliteOperations import CheckSqlite
from SQLAlchemyOperations import CheckSQLAlchemy


big_data = True
tries = 3

time_pandas = CheckPandas(tries, big_data)
print('Pandas')
print(time_pandas)

time_duckdb = CheckDuckDB(tries, big_data)
print('DuckDB')
print(time_duckdb)

time_psycopg = CheckPostgres(tries, big_data)
print('Postgres')
print(time_psycopg)

time_sqlite = CheckSqlite(tries, big_data)
print('SQLite')
print(time_sqlite)

time_SQLAlchemy = CheckSQLAlchemy(tries, big_data)
print('SQLAlchemy')
print(time_SQLAlchemy)
