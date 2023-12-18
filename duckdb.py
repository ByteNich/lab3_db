import time
import duckdb

def execute_query(cursor, query, iterations=10):
    total_time = 0
    for _ in range(iterations):
        t0 = time.perf_counter()
        result = cursor.execute(query).fetchall()
        t1 = time.perf_counter()
        total_time += (t1 - t0)
    average_time = total_time / iterations
    return result, average_time

def print_results(query_name, result, average_time):
    print(query_name)
    print("Result:", result)
    print("Average time:", average_time)
    print()

def run_queries(connection, csv_file, queries):
    cursor = connection.cursor()
    cursor.execute(f"CREATE TABLE trips AS SELECT * FROM read_csv_auto('{csv_file}');")

    total_sum = [0] * len(queries)

    for i, query in enumerate(queries):
        result, average_time = execute_query(cursor, query)
        total_sum[i] = average_time
        print_results(f"Query {i + 1}", result, average_time)

    cursor.close()
    connection.close()

    return total_sum

# Define queries
queries = [
    'SELECT VendorID, count(*) FROM trips GROUP BY 1;',
    'SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;''',
    '''SELECT passenger_count, strftime('%Y', tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;'''
]


# Connect to databases
tiny = duckdb.connect('nyc_yellow_tiny.duckdb')
big = duckdb.connect('nyc_yellow_big.duckdb')

# Run queries for tiny data
print('For tiny data')
total_sum_tiny = run_queries(tiny, 'nyc_yellow_tiny.csv', queries)

# Run queries for big data
print('For big data')
total_sum_big = run_queries(big, 'nyc_yellow_big.csv', queries)

# Print total average times
for i, query_name in enumerate(['First', 'Second', 'Third', 'Fourth']):
    total_average_tiny = total_sum_tiny[i] / 10
    total_average_big = total_sum_big[i] / 10
    print(f"{query_name} query - Tiny: {total_average_tiny}, Big: {total_average_big}")
