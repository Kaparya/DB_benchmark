# Лабораторная работа №3 Benchmark

### Описание лабораторной работы

Задание данной лабораторной работы заключалось в написании бенчмарка для измерения скорости выполения 4 запросов на 5 различных библиотеках на языке Python (psycopg2, SQLite, DuckDB, Pandas, SQLAlchemy).

### Задача

Была поставлена задача проверить и сравнить время работы 4 запросов из статьи на [Medium](https://medium.unum.cloud/pandas-cudf-modin-arrow-spark-and-a-billion-taxi-rides-f85973bfafd5):

* First query
  ```sql
  
  SELECT cab_type, count(*) FROM trips GROUP BY 1;
  
  ```
* Second query
  ```sql
  SELECT passenger_count, avg(total_amount) 
  FROM trips 
  GROUP BY 1;
  ```
* Third query
  ```sql
  SELECT
     passenger_count, 
     extract(year from pickup_datetime),
     count(*)
  FROM trips
  GROUP BY 1, 2;
  ```
* Fourth query
  ```sql
  SELECT
      passenger_count,
      extract(year from pickup_datetime),
      round(trip_distance),
      count(*)
  FROM trips
  GROUP BY 1, 2, 3
  ORDER BY 2, 4 desc;
  ```

На 4 библиотеках языка Python:
1. Pandas
2. DuckDB
3. Postgres
4. SQLite
5. SQLAlchemy
