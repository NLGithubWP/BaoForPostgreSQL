import os
import sys
import time
import psycopg2
import glob

# Check for the correct number of arguments
if len(sys.argv) != 3:
    print("Usage: python script.py QUERY_FOLDER DB_NAME")
    sys.exit(1)

# Configuration
QUERY_FOLDER = sys.argv[1]
DB_NAME = sys.argv[2]
MAX_TIME = 0

PG_CONNECTION_STR = f"dbname={DB_NAME} user=postgres password=123 host=localhost"

# List of test queries to skip
SKIP_QUERIES = [
    "test_query_133.sql",
    "test_query_112.sql",
    "test_query_103.sql",
    "test_query_105.sql",
    "test_query_104.sql",
    "test_query_48.sql",
    "test_query_50.sql",
    "test_query_51.sql",
    "test_query_31.sql",
    "test_query_119.sql",
    "test_query_43.sql",
    # this is slow
    "test_query_60.sql",
    "test_query_54.sql",
    "test_query_53.sql",
    "test_query_69.sql",
]


def execute_sql_file(sql_file):
    global MAX_TIME
    start_time = time.time()

    print(f"Executing query from file: {sql_file}")

    print(f"Executing query from file: {sql_file}\n")

    try:
        conn = psycopg2.connect(PG_CONNECTION_STR)
        cur = conn.cursor()
        with open(sql_file, 'r') as file:
            query = file.read()
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error executing query from file {sql_file}: {e}")

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time for {sql_file}: {execution_time:.2f} seconds")

    if execution_time > MAX_TIME:
        MAX_TIME = execution_time


# Iterate over all SQL files in the folder and execute them
for sql_file in glob.glob(os.path.join(QUERY_FOLDER, '*.sql')):
    base_name = os.path.basename(sql_file)

    if base_name.startswith('train_') or base_name in SKIP_QUERIES:
        print(f"Skipping {base_name}")

        continue

    execute_sql_file(sql_file)

print(f"Maximum execution time: {MAX_TIME:.2f} seconds")
