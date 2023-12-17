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

### Ход работы
Весь код был написан на языке Python с использованием pgAdmin4 и DB Browser for SQLite. Тесты проводились на платформе Apple Macbook Air m1 (16 gb / 512 gb). Все файлы с кодом находятся в репозитории, так же для запуска потребуются файлы с базой данных (.csv and .db), GitHub не позволяет загрузить из-за размера, поэтому они на [Диске](https://disk.yandex.ru/d/qFi1xFNMSjERJg). Для запуска эти файлы необходимо разместить в той же папке, где main.


---
0. Все функции для проверок библиотек лежат в одной папке - Operation_files, называются "CheckИмяБиблиотеки" и имеют одинаковую структуру: они принимают количество повторений тестов (tries) и флаг big_data (если он false => используется файл tiny, если true => big).
   В функции создаются массивы со значениями суммарного времени работы, и времени работы каждого query.
   Функция возвращает массив посчитанных медианных значений по массивам суммарного времени работы, и времени работы каждого query.

    **Пример файла для библиотеки Pandas**
      ```python
      def CheckPandas(tries, big_data=False):
        prints = 0
    
        time_sum = []
        time_first = []
        time_second = []
        time_third = []
        time_fourth = []
        pd.options.mode.chained_assignment = None
    
        if big_data:
            data = pd.read_csv('nyc_yellow_big.csv')
        else:
            data = pd.read_csv('nyc_yellow_tiny.csv')
    
        for i in range(tries):
    
            start_time = time()
            PandasFirstQuery(data, prints)
            time_first.append(time() - start_time)
            time1 = time()
            PandasSecondQuery(data, prints)
            time_second.append(time() - time1)
            time2 = time()
            PandasThirdQuery(data, prints)
            time_third.append(time() - time2)
            time3 = time()
            PandasFourthQuery(data, prints)
            time_fourth.append(time() - time3)
            finish_time = time()
    
            time_sum.append(finish_time - start_time)
    
        return [median(time_sum),
                median(time_first),
                median(time_second),
                median(time_third),
                median(time_fourth)]
      ```


---
1. Сначала были написаны все функции для всех библиотек, они располагаются в папке Operation_files.

   **Впечатления от работы с библиотеками:**

   * Pandas - неприятная реализация SQL запросов. Однако, считывание базы данных из .csv очень удобно и понятно.
  
   * DuckDB - очень удобно и очень просто: есть возможность считать базу данных из .csv одной функцией, запросы делаются обычным SQL языком.
  
   * Psycopg - требовалось подключиться к базе данных в postgres, неприятно. Query пишутся на обычном SQL.
  
   * SQLite - нужно было создать .db файл и подключиться к нему - неприятно. Не все функции из SQL работали, потребовалось найти аналоги для работы с SQLite - очень неприятно, когда не знаешь про это, а ошибка есть (описание ошибки ужасно).
  
   * SQLAlchemy - подключение к postgres - неприятно. Других проблем не было.
  
   * Python - впервые использовал серьезно, до этого чисто алгоритмические задачи. В принципе прикольно, но плюсы лучше (just kidding).

---
2. Результаты тестов.

   Представлены медианные значения по 28 запускам для файла tiny и по 17 запускам для файла big.

   - **Pandas**

     <img width="400" alt="Screenshot 2023-12-17 at 23 25 36" src="https://github.com/Kaparya/DB_benchmark/assets/124422354/d8498dbe-d6f3-4461-921b-68e1ae32dd2f"> <img width="400" alt="Screenshot 2023-12-17 at 23 26 52" src="https://github.com/Kaparya/DB_benchmark/assets/124422354/733ec34a-bf7c-4b89-8bb4-c32f99c01c07">
   
   - **DuckDB**

     <img width="400" alt="Screenshot 2023-12-17 at 23 33 16" src="https://github.com/Kaparya/DB_benchmark/assets/124422354/439bc5f0-541f-4a4d-95f8-747d8ed7c88b"> <img width="400" alt="Screenshot 2023-12-17 at 23 33 32" src="https://github.com/Kaparya/DB_benchmark/assets/124422354/b7e0670d-e739-4528-996a-ceffadde5854">


