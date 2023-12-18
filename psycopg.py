import time
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def execute_queries(queries, table_name):
    sum_times = [0, 0, 0, 0]
    for i in range(10):
        for j, query in enumerate(queries, start=1):
            t0 = time.perf_counter()
            cursor.execute(query)
            result = cursor.fetchall()
            t1 = time.perf_counter()
            sum_times[j-1] += (t1 - t0)
            if i == 9:
                print(f"Query {j} for {table_name}")
                print("Result: ", result)
                print(f"Average time: {sum_times[j-1] / 10}\n")

try:
    conn = psycopg2.connect(user="postgres",
                            password="123456",
                            host="localhost",
                            port="5432")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Creation
    sql_create_database = 'create database postgres_db'
    cursor.execute(sql_create_database)

    queries_tiny_data = [
        'SELECT "VendorID", count(*) FROM public.trips group by 1;',
        'SELECT "passenger_count", avg(total_amount) FROM public.trips GROUP BY 1;',
        'SELECT "passenger_count", extract(year from "tpep_pickup_datetime"), count(*) FROM public.trips GROUP BY 1, 2;',
        'SELECT "passenger_count", extract(year from "tpep_pickup_datetime"), round("trip_distance"), count(*) FROM public.trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'
    ]

    queries_big_data = [
        'SELECT "VendorID", count(*) FROM public.taxi group by 1;',
        'SELECT "passenger_count", avg(total_amount) FROM public.taxi GROUP BY 1;',
        'SELECT "passenger_count", extract(year from "tpep_pickup_datetime"), count(*) FROM public.taxi GROUP BY 1, 2;',
        'SELECT "passenger_count", extract(year from "tpep_pickup_datetime"), round("trip_distance"), count(*) FROM public.taxi GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'
    ]

    execute_queries(queries_tiny_data, "tiny data")
    execute_queries(queries_big_data, "big data")

except (Exception, Error) as error:
    print("Error working with PostgreSQL:", error)

finally:
    if conn:
        cursor.close()
        conn.close()
