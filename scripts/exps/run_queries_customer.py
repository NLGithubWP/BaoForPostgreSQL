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
    start = time()
    while True:
        try:
            conn = psycopg2.connect(conn_str)
            cur = conn.cursor()
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 500000")
            cur.execute(sql)
            cur.fetchall()
            conn.close()
            break
        except Exception as e:
            print(f"Error executing query: {e}")
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

    Executed_query = [
        "test_query_106.sql",
        "test_query_142.sql",
        "test_query_115.sql",
        "test_query_84.sql",
        "test_query_52.sql",
        "test_query_23.sql",
        "test_query_125.sql",
        "test_query_62.sql",
        "test_query_144.sql",
        "test_query_76.sql",
        "test_query_111.sql",
        "test_query_120.sql",
        "test_query_1.sql",
        "test_query_137.sql",
        "test_query_56.sql",
        "test_query_17.sql",
        "test_query_27.sql",
        "test_query_71.sql",
        "test_query_86.sql",
        "test_query_101.sql",
        "test_query_58.sql",
        "test_query_40.sql",
        "test_query_39.sql",
        "test_query_21.sql",
        "test_query_83.sql",
        "test_query_99.sql",
        "test_query_30.sql",
        "test_query_114.sql",
        "test_query_97.sql",
        "test_query_85.sql",
        "test_query_13.sql",
        "test_query_46.sql",
        "test_query_66.sql",
        "test_query_9.sql",
        "test_query_126.sql",
        "test_query_59.sql",
        "test_query_74.sql",
        "test_query_60.sql",
        "test_query_34.sql",
        "test_query_102.sql",
        "test_query_139.sql",
        "test_query_132.sql",
        "test_query_95.sql",
        "test_query_122.sql",
        "test_query_81.sql",
        "test_query_93.sql",
        "test_query_18.sql",
        "test_query_146.sql",
        "test_query_117.sql",
        "test_query_113.sql",
        "test_query_128.sql",
        "test_query_47.sql",
        "test_query_107.sql",
        "test_query_12.sql",
        "test_query_100.sql",
        "test_query_75.sql",
        "test_query_33.sql",
        "test_query_68.sql",
        "test_query_145.sql",
        "test_query_89.sql"
    ]

    Executed_query2 = [
        "test_query_123.sql",
        "test_query_43.sql",
        "test_query_110.sql",
        "test_query_79.sql",
        "test_query_87.sql",
        "test_query_94.sql",
        "test_query_11.sql",
        "test_query_129.sql",
        "test_query_28.sql",
        "test_query_55.sql",
        "test_query_61.sql",
        "test_query_90.sql",
        "test_query_65.sql",
        "test_query_25.sql",
        "test_query_20.sql",
        "test_query_29.sql",
        "test_query_32.sql",
        "test_query_143.sql",
        "test_query_42.sql",
        "test_query_96.sql",
        "test_query_35.sql",
        "test_query_14.sql",
        "test_query_26.sql",
        "test_query_49.sql",
        "test_query_19.sql"
    ]

    test_queries = read_queries(
        [os.path.join(args.query_folder, f) for f in os.listdir(args.query_folder) if
         f.startswith("test_") and f not in SKIP_QUERIES and f not in Executed_query and f not in Executed_query2])

    print("Read", len(train_queries), "training queries.", flush=True)
    print("Read", len(test_queries), "testing queries.", flush=True)
    print("Using Bao:", USE_BAO, flush=True)

    # Pre-train with training queries
    print("---- Executing training queries for initial training ---- ", flush=True)
    # for fp, q in train_queries[:25]:
    #     pg_time = run_query(fp, q, PG_CONNECTION_STR, bao_reward=True)
    #     print("x", "x", time(), fp, pg_time, "PG", flush=True)

    # Determine chunk size for testing queries
    chunk_size = 25 if len(test_queries) >= 25 else len(test_queries)
    bao_chunks = list(chunks(test_queries, chunk_size))

    print(f"---begin online learning with {len(bao_chunks)} and each with {len(bao_chunks[0])}---", flush=True)

    for c_idx, chunk in enumerate(bao_chunks):
        if USE_BAO:
            print("---- start retraining ---- ", flush=True)
            os.system("cd bao_server && CUDA_VISIBLE_DEVICES='' python3 baoctl.py --retrain")
            os.system("sync")
        print("---- start online evaluating ---- ", flush=True)
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
