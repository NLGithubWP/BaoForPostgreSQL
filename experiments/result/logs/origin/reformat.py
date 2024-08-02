import re

# Path to the log file
log_path = 'pg_run_from_script.txt'

# Load the log file
with open(log_path, 'r') as file:
    log_data = file.read()

# Pattern to capture query ID and execution time
pattern = r"test_query_(\d+)\.sql.*?(\d*\.?\d*) seconds"

# Find all matches with the pattern
matches = re.findall(pattern, log_data)

# Convert matches to dictionary with integer query ID and float execution time
query_data = {int(query_id): float(exec_time) if exec_time else '' for query_id, exec_time in matches}

# Determine the range of query IDs
min_id = min(query_data.keys())
max_id = max(query_data.keys())

# Fill in missing query IDs
all_query_data = [(query_id, query_data.get(query_id, '')) for query_id in range(min_id, max_id + 1)]

# Sort by query ID (already in order due to range)
sorted_by_id = all_query_data

# Sort by execution time, skipping empty entries for sorting purposes
sorted_by_latency = sorted((item for item in all_query_data if item[1] != ''), key=lambda x: x[1])

# File paths for output
id_path = 'queries_sorted_by_id.txt'
latency_path = 'queries_sorted_by_latency.txt'

# Writing the sorted data by query ID
with open(id_path, 'w') as f:
    for query_id, exec_time in sorted_by_id:
        f.write(f"{query_id} {exec_time}\n")

# Writing the sorted data by latency
with open(latency_path, 'w') as f:
    for query_id, exec_time in sorted_by_latency:
        f.write(f"{query_id} {exec_time}\n")
