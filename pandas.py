import time
import pandas as pd

def run_queries(data, num_runs=10):
    sum_times = [0, 0, 0, 0]

    for i in range(num_runs):
        # Query 1
        t0 = time.perf_counter()
        result1 = data.groupby(["VendorID"]).size()
        t1 = time.perf_counter()
        sum_times[0] += (t1 - t0)

        # Query 2
        t0 = time.perf_counter()
        result2 = data.groupby(["passenger_count"])["total_amount"].mean()
        t1 = time.perf_counter()
        sum_times[1] += (t1 - t0)

        # Query 3
        t0 = time.perf_counter()
        data['Year'] = pd.to_datetime(data['tpep_pickup_datetime']).dt.year
        result3 = data.groupby(['passenger_count', 'Year']).size()
        t1 = time.perf_counter()
        sum_times[2] += (t1 - t0)

        # Query 4
        t0 = time.perf_counter()
        data['Year'] = pd.to_datetime(data['tpep_pickup_datetime']).dt.year
        data['trip_distance'] = data['trip_distance'].round()
        result4 = data.groupby(['passenger_count', 'Year', 'trip_distance']).size().reset_index(name='count').sort_values(['Year', 'count'], ascending=[True, False])
        t1 = time.perf_counter()
        sum_times[3] += (t1 - t0)

        if i == num_runs - 1:
            print("First query")
            print("Result: ", result1)
            print("Average time: ", sum_times[0] / num_runs)
            print()
            print("Second query")
            print("Result: ", result2)
            print("Average time: ", sum_times[1] / num_runs)
            print()
            
            print("Third query")
            print("Result: ", result3)
            print("Average time: ", sum_times[2] / num_runs)
            print()
            print("Fourth query")
            print("Result: ", result4)
            print("Average time: ", sum_times[3] / num_runs)

tiny = pd.read_csv(r"D:\Study\бд\laba_3\nyc_yellow_tiny.csv")
big = pd.read_csv(r"D:\Study\бд\laba_3\nyc_yellow_big.csv")

print('for tiny data')
run_queries(tiny)

print('for big data')
run_queries(big)
