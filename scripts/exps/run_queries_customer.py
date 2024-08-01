import psycopg2
import os
import sys
import random
from time import time, sleep
from pprint import pprint
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Run queries with optional Bao optimization.")
    parser.add_argument('query_folder', type=str, help='Path to the folder containing query files.')
    parser.add_argument('--dbname', type=str, default='stats_test', help='Database name to connect to.')
    parser.add_argument('--use_bao', action='store_true', help='Enable Bao optimization.')
    return parser.parse_args()


def read_queries(query_paths):
    queries = []
    for fp in query_paths:
        with open(fp) as f:
            query = f.read()
        queries.append((fp, query))
    return queries


def run_query(fq, sql, conn_str, bao_select=False, bao_reward=False):
    print(f"[debug] run_query for file: {fq}")
    start = time()
    while True:
        try:
            print(f"[debug] Connectting for file: {fq}")
            conn = psycopg2.connect(conn_str)
            cur = conn.cursor()
            print(f"[debug] Connected, Executing query file: {fq}")
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 500000")
            cur.execute(sql)
            print(f"[debug] Done with exeuction query file: {fq}")
            cur.fetchall()
            conn.close()
            print(f"[debug] Discinnected")
            break
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"[debug] errored {e}")
            sleep(0.1)
            continue
    stop = time()
    return stop - start


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main():
    args = parse_args()

    PG_CONNECTION_STR = f"dbname={args.dbname} user=postgres password=123 host=localhost"
    USE_BAO = args.use_bao

    # Read all queries
    # train_queries = read_queries(
    #     [os.path.join(args.query_folder, f) for f in os.listdir(args.query_folder) if f.startswith("train_")])

    train_queries = read_queries(
        [os.path.join(args.query_folder, f) for f in
         [
             "train_query_1.sql",
             "train_query_2.sql",
             "train_query_3.sql",
             "train_query_4.sql",
             "train_query_5.sql",
             "train_query_7.sql",
             "train_query_35.sql",
             "train_query_67.sql",
             "train_query_41.sql",
             "train_query_61.sql",
             "train_query_66.sql",
             "train_query_74.sql",
             "train_query_76.sql",
             "train_query_82.sql",
             "train_query_99.sql",
             "train_query_98.sql",
             "train_query_96.sql",
             "train_query_95.sql",
             "train_query_94.sql",
             "train_query_93.sql",
             "train_query_92.sql",
             "train_query_91.sql",
             "train_query_90.sql",
             "train_query_89.sql",
             "train_query_88.sql",
         ]
         ])

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
        "test_query_43.sql"
        # this is slow
        "test_query_60.sql",
        "test_query_54.sql",
        "test_query_53.sql",
        "test_query_69.sql",
        "test_query_44.sql",
    ]

    test_queries = read_queries(
        [os.path.join(args.query_folder, f) for f in os.listdir(args.query_folder) if
         f.startswith("test_") and f not in SKIP_QUERIES])

    print("Read", len(train_queries), "training queries.")
    print("Read", len(test_queries), "testing queries.")
    print("Using Bao:", USE_BAO)

    # Pre-train with training queries
    print("---- Executing training queries for initial training ---- ")
    for fp, q in train_queries[:25]:
        print(f"[debug] just begin one")
        pg_time = run_query(fp, q, PG_CONNECTION_STR, bao_reward=True)
        print("x", "x", time(), fp, pg_time, "PG", flush=True)
        print(f"[debug] just end one")

    # Determine chunk size for testing queries
    chunk_size = 25 if len(test_queries) >= 25 else len(test_queries)
    bao_chunks = list(chunks(test_queries, chunk_size))

    print("bao_chunks", bao_chunks)

    print("---begin online learning---")

    for c_idx, chunk in enumerate(bao_chunks):
        if USE_BAO:
            os.system("cd bao_server && CUDA_VISIBLE_DEVICES=1 python3 baoctl.py --retrain")
            os.system("sync")
        for q_idx, (fp, q) in enumerate(chunk):
            q_time = run_query(fp, q, PG_CONNECTION_STR, bao_reward=USE_BAO, bao_select=USE_BAO)
            print(c_idx, q_idx, time(), fp, q_time, flush=True)


if __name__ == "__main__":
    main()

"""
python scripts/exps/run_queries_customer.py ./experiments/query/sample_query_stats_STATS_origin --dbname=stats_test --use_bao | tee ./experiments/result/logs/bao_run_origin_test.txt

python scripts/exps/run_queries_customer.py ./experiments/query/sample_query_stats_STATS_origin --dbname=stats --use_bao | tee ./experiments/result/logs/bao_run_origin.txt
python scripts/exps/run_queries_customer.py ./experiments/query/sample_query_stats_severe_drift --dbname=stats_severe --use_bao | tee ./experiments/result/logs/bao_run_severe.txt
python scripts/exps/run_queries_customer.py ./experiments/query/sample_query_stats_mild_drift --dbname=stats_mid --use_bao | tee ./experiments/result/logs/bao_run_mild.txt

"""
