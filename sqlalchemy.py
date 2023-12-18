import time
import sqlalchemy as db

def execute_query(engine, query):
    t0 = time.perf_counter()
    result = engine.execute(query)
    t1 = time.perf_counter()
    return result, t1 - t0


def print_results(query_name, result, average_time):
    print(query_name)
    print("Result: ", end='')
    for r in result: 
        print(r)
    print("Average time: ", average_time)
    print()

def run_queries(engine, queries):
    sum_times = [0] * len(queries)
    results = [None] * len(queries)

    for i in range(10):
        for j, query in enumerate(queries):
            results[j], query_time = execute_query(engine, query)
            sum_times[j] += query_time

    return results, [time / 10 for time in sum_times]

tengine = db.create_engine("sqlite:///nyc_yellow_tiny.db")
tiny = tengine.connect()
bengine = db.create_engine("sqlite:///nyc_yellow_big.db")
big = bengine.connect()

queries = [q1, q2, q3, q4]

print('For tiny data')
tiny_results, tiny_average_times = run_queries(tiny, queries)

for i, query in enumerate(queries):
    print_results(f"{i + 1} query", tiny_results[i], tiny_average_times[i])

print('For big data')
big_results, big_average_times = run_queries(big, queries)

for i, query in enumerate(queries):
    print_results(f"{i + 1} query", big_results[i], big_average_times[i])
