import re
import os
import random
import argparse


def clean_sql(sql):
    # Remove any content after the first semicolon
    return sql.split(';')[0] + ';'


def extract_queries(filename):
    with open(filename, 'r') as file:
        content = file.read()

    # Split the content based on training and evaluation markers
    parts = re.split(r'---+\s*training-part\s*---+|---+\s*evaluation-part\s*---+', content)

    if len(parts) < 3:
        print("Error: The file does not contain both training and evaluation parts.")
        return [], []

    training_part = parts[1]
    evaluation_part = parts[2]

    # Extract and clean queries from the training part
    training_queries = []
    for line in training_part.splitlines():
        if line.strip().startswith('train_query:'):
            query = line.strip()[len('train_query:'):].strip()
            if query and query.lower().startswith('select'):
                cleaned_query = clean_sql(query)
                training_queries.append(cleaned_query)

    # Extract and clean queries from the evaluation part
    evaluation_queries = []
    for line in evaluation_part.splitlines():
        if line.strip().startswith('test_query:'):
            query = line.strip()[len('test_query:'):].strip()
            if query:
                cleaned_query = clean_sql(query)
                evaluation_queries.append(cleaned_query)

    return training_queries, evaluation_queries


def save_queries_to_files(queries, prefix, query_type):
    if not os.path.exists(prefix):
        os.makedirs(prefix)
    for i, query in enumerate(queries, 1):
        filename = os.path.join(prefix, f'{query_type}_query_{i}.sql')
        with open(filename, 'w') as file:
            file.write(query)
    print(f"Saved {len(queries)} {query_type} queries to {prefix} directory.")


def main(input_file):
    training_queries, evaluation_queries = extract_queries(input_file)

    # Derive the output folder name from the input file path
    base_name = os.path.basename(os.path.dirname(input_file))
    output_folder = f'./sample_query_stats_{base_name}'

    # Randomly select 100 training queries if available
    if len(training_queries) > 100:
        training_queries = random.sample(training_queries, 100)

    save_queries_to_files(training_queries, output_folder, 'train')
    save_queries_to_files(evaluation_queries, output_folder, 'test')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract and save SQL queries.')
    parser.add_argument('input_file', type=str, help='Path to the workload.sql file')
    args = parser.parse_args()

    main(args.input_file)
