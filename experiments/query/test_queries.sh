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

# List of test queries to skip
SKIP_QUERIES=(
  "test_query_133.sql"
  "test_query_112.sql"
  "test_query_103.sql"
  "test_query_105.sql"
  "test_query_104.sql"
  "test_query_48.sql"
  "test_query_50.sql"
  "test_query_51.sql"
  "test_query_31.sql"
  "test_query_119.sql"
  "test_query_43.sql"
  "test_query_60.sql"
      # this is slow
  "test_query_60.sql"
  "test_query_54.sql"
  "test_query_53.sql"
  "test_query_69.sql"
)

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
  # Get the base name of the file
  base_name=$(basename "$sql_file")

  # Skip files that begin with train_ or are in the SKIP_QUERIES list
  if [[ $base_name == train_* ]] || [[ " ${SKIP_QUERIES[@]} " =~ " $base_name " ]]; then
    echo "Skipping $base_name" | tee -a "$LOG_FILE"
    continue
  fi

  execute_sql_file "$sql_file"
done

echo "Maximum execution time: $MAX_TIME seconds" | tee -a "$LOG_FILE"


