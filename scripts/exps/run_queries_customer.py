import psycopg2
import os
import sys
import random
from time import time, sleep
from pprint import pprint

USE_BAO = True
PG_CONNECTION_STR = "dbname=stats_test user=postgres host=localhost"


# Function to read queries from files
def read_queries(query_paths):
    queries = []
    for fp in query_paths:
        with open(fp) as f:
            query = f.read()
        queries.append((fp, query))
    return queries


# Function to execute a query
def run_query(sql, bao_select=False, bao_reward=False):
    start = time()
    while True:
        try:
            conn = psycopg2.connect(PG_CONNECTION_STR)
            cur = conn.cursor()
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 300000")
            cur.execute(sql)
            cur.fetchall()
            conn.close()
            break
        except Exception as e:
            print(f"Error executing query: {e}")
            sleep(1)
            continue
    stop = time()
    return stop - start


# Function to chunk the queries
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main():
    # Assuming the script is called with paths to the query folders
    query_folder = sys.argv[1]

    # Read all queries
    train_queries = read_queries(
        [os.path.join(query_folder, f) for f in os.listdir(query_folder) if f.startswith("train_")])
    test_queries = read_queries(
        [os.path.join(query_folder, f) for f in os.listdir(query_folder) if f.startswith("test_")])

    print("Read", len(train_queries), "training queries.")
    print("Read", len(test_queries), "testing queries.")
    print("Using Bao:", USE_BAO)

    # Pre-train with training queries
    print("Executing training queries for initial training")
    for fp, q in train_queries:
        pg_time = run_query(q, bao_reward=True)
        print("x", "x", time(), fp, pg_time, "PG", flush=True)

    # Determine chunk size for testing queries
    chunk_size = 25 if len(test_queries) >= 25 else len(test_queries)
    bao_chunks = list(chunks(test_queries, chunk_size))

    print("bao_chunks", bao_chunks)

    for c_idx, chunk in enumerate(bao_chunks):
        if USE_BAO:
            os.system("cd bao_server && python3 baoctl.py --retrain")
            os.system("sync")
        for q_idx, (fp, q) in enumerate(chunk):
            q_time = run_query(q, bao_reward=USE_BAO, bao_select=USE_BAO)
            print(c_idx, q_idx, time(), fp, q_time, flush=True)


if __name__ == "__main__":
    main()


"""
python run_queries.py /code/AI4QueryOptimizer/baseline/BaoForPostgreSQL/experiments/query/sample_query_stats_STATS_origin

"""

