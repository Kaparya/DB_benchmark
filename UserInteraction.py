from os import mkdir, path

from Operation_files.PandasOperations import CheckPandas
from Operation_files.DuckDBOperations import CheckDuckDB
from Operation_files.PsycopgOperations import CheckPostgres
from Operation_files.SqliteOperations import CheckSqlite
from Operation_files.SQLAlchemyOperations import CheckSQLAlchemy

def PrintResults(results, library):
    print(f'------------ {library} ------------')
    print('Median time of all 4 queries: {:.7f}'.format(results[0]))
    print('Median time of first query:   {:.7f}'.format(results[1]))
    print('Median time of second query:  {:.7f}'.format(results[2]))
    print('Median time of third query:   {:.7f}'.format(results[3]))
    print('Median time of fourth query:  {:.7f}\n'.format(results[4]))


def CheckLibraries(tries, big_data):
    print('/-------------\\')
    time_pandas = CheckPandas(tries, big_data)
    print('###', end='')
    time_duckdb = CheckDuckDB(tries, big_data)
    print('###', end='')
    time_psycopg = CheckPostgres(tries, big_data)
    print('###', end='')
    time_sqlite = CheckSqlite(tries, big_data)
    print('###', end='')
    time_SQLAlchemy = CheckSQLAlchemy(tries, big_data)
    print('###\n')
    return time_pandas, time_duckdb, time_psycopg, time_sqlite, time_SQLAlchemy


def SaveResults(time_pandas, time_duckdb, time_psycopg, time_sqlite, time_SQLAlchemy, big_data):
    save = input("\nWould you like to save the results? (Y): ")
    if save == 'Y':
        if not path.isdir('Results'):
            mkdir('Results')
        if big_data:
            file_output = open("Results/ResultsOfBigTest.txt", "w")
        else:
            file_output = open("Results/ResultsOfTinyTest.txt", "w")
        data = f'''--- Results ---
Pandas: 
{' '.join(map(lambda a: '{:.7f}'.format(a), time_pandas))}
DuckDB: 
{' '.join(map(lambda a: '{:.7f}'.format(a), time_duckdb))}
Postgres: 
{' '.join(map(lambda a: '{:.7f}'.format(a), time_psycopg))}
SQLite: 
{' '.join(map(lambda a: '{:.7f}'.format(a), time_sqlite))}
SQLAlchemy: 
{' '.join(map(lambda a: '{:.7f}'.format(a), time_SQLAlchemy))}'''
        print(data, file=file_output)

        print('\nData saved to ', end='')
        if big_data:
            print("Results/ResultsOfBigTest.txt")
        else:
            print("Results/ResultsOfTinyTest.txt")
        file_output.close()


def RunTest(tries, big_data):
    (time_pandas, time_duckdb, time_psycopg, time_sqlite, time_SQLAlchemy) = CheckLibraries(tries, big_data)
    PrintResults(time_pandas, 'Pandas')
    PrintResults(time_duckdb, 'DuckDB')
    PrintResults(time_psycopg, 'Postgres')
    PrintResults(time_sqlite, 'SQLite')
    PrintResults(time_SQLAlchemy, 'SQLAlchemy')

    SaveResults(time_pandas, time_duckdb, time_psycopg, time_sqlite, time_SQLAlchemy, big_data)


def RunTestTiny():
    big_data = False
    tries = -1
    while tries == -1:
        tries_str = input('\nInput the number of tests (no more than 50 tests): ')
        if tries_str.isnumeric() and 1 <= int(tries_str) <= 50:
            tries = int(tries_str)
            break
        print('Write correct number!\n')

    print('\n\n\nRunning test with tiny file.')
    RunTest(tries, big_data)


def RunTestBig():
    big_data = True
    tries = -1
    while tries == -1:
        tries_str = input('\nInput the number of tests (no more than 20 tests): ')
        if tries_str.isnumeric() and 1 <= int(tries_str) <= 20:
            tries = int(tries_str)
            break
        print('Write correct number!\n')

    print('\n\n\nRunning test with big file.')
    RunTest(tries, big_data)