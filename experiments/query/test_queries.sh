#!/bin/bash

# Check for the correct number of arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 QUERY_FOLDER DB_NAME"
  exit 1
fi

# Configuration
DB_HOST="localhost"
DB_USER="postgres"
QUERY_FOLDER="$1"
DB_NAME="$2"
LOG_FILE="./execution_log.txt"
MAX_TIME=0

# Create or clear the log file
: > "$LOG_FILE"

# Function to execute a single SQL file and record the time taken
execute_sql_file() {
  local sql_file=$1
  local start_time=$(date +%s.%N)

  echo "Executing query from file: $sql_file" | tee -a "$LOG_FILE"
  psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -f "$sql_file"

  local end_time=$(date +%s.%N)
  local execution_time=$(echo "$end_time - $start_time" | bc)

  echo "Execution time for $sql_file: $execution_time seconds" | tee -a "$LOG_FILE"

  # Update max execution time if current execution time is greater
  if (( $(echo "$execution_time > $MAX_TIME" | bc -l) )); then
    MAX_TIME=$execution_time
  fi
}

# Iterate over all SQL files in the folder and execute them
for sql_file in "$QUERY_FOLDER"/*.sql; do
  execute_sql_file "$sql_file"
done

echo "Maximum execution time: $MAX_TIME seconds" | tee -a "$LOG_FILE"


# ./execute_sql_files.sh ./experiments/query/sample_query_stats_STATS_origin stats_test