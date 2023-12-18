import time
import sqlite3
import pandas as pd

def create_database(connection, csv_path):
    data = pd.read_csv(csv_path)
    data.to_sql('trips', connection, if_exists='replace', index=False)


def execute_query(cursor, query, average_time_list):
    t0 = time.perf_counter()
    cursor.execute(query)
    result = cursor.fetchall()
    t1 = time.perf_counter()
    average_time_list.append(t1 - t0)
    return result

def print_results(query_name, result, average_time):
    print(query_name)
    print("Result:", result)
    print("Average time:", average_time)
    print()

# Connect to databases
tiny = sqlite3.connect('nyc_yellow_tiny.db')
big = sqlite3.connect('nyc_yellow_big.db')

# Create databases
create_database(tiny, r"D:\Study\бд\laba_3\nyc_yellow_tiny.csv")
create_database(big, r"D:\Study\бд\laba_3\nyc_yellow_big.csv")

queries = [
    'SELECT VendorID, count(*) FROM trips GROUP BY 1;',
    'SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;''',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'''
]

databases = [(tiny, "for tiny data"), (big, "for big data")]

for database, database_name in databases:
    cursor = database.cursor()
    sum_times = [0] * 4

    for i in range(10):
        for j, query in enumerate(queries, start=1):
            result_name = f"result{j}"
            sum_name = f"sum{j}"
            result = execute_query(cursor, query, sum_times[j - 1])
            locals()[result_name] = result

    for j, query in enumerate(queries, start=1):
        result_name = f"result{j}"
        sum_name = f"sum{j}"
        print_results(f"{database_name} - Query {j}", locals()[result_name], sum_times[j - 1] / 10)

    cursor.close()
    database.close()
