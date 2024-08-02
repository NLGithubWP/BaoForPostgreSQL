#!/bin/bash

# Function to execute SQL files
execute_sql_files() {
    local folder="$1"
    shift
    local sql_files=("$@")
    for sql_file in "${sql_files[@]}"; do
        echo "Executing query: $sql_file"
        psql -U postgres -d "$DB_NAME" -f "$folder/$sql_file"
    done
}

# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <sql_folder> <database_name>"
    exit 1
fi

# Get the command-line arguments
sql_folder="$1"
DB_NAME="$2"

# Connect to the database
psql -U postgres -c "\c $DB_NAME"

# Set initial configurations
psql -U postgres -c "SET enable_bao TO true;"
psql -U postgres -c "SET enable_bao_selection TO false;"
psql -U postgres -c "SET enable_bao_rewards TO true;"
psql -U postgres -c "SET bao_num_arms TO 5;"
psql -U postgres -c "SET statement_timeout TO 500000;"

# List of training SQL files
train_sql_files=(
    "train_query_1.sql"
    "train_query_3.sql"
    "train_query_4.sql"
    "train_query_5.sql"
    "train_query_7.sql"
    "train_query_35.sql"
    "train_query_67.sql"
    "train_query_41.sql"
    "train_query_61.sql"
    "train_query_66.sql"
    "train_query_74.sql"
#    "train_query_76.sql"
    "train_query_82.sql"
    "train_query_99.sql"
    "train_query_98.sql"
    "train_query_96.sql"
    "train_query_95.sql"
    "train_query_94.sql"
    "train_query_93.sql"
    "train_query_92.sql"
    "train_query_91.sql"
    "train_query_90.sql"
    "train_query_89.sql"
    "train_query_88.sql"
)

# Execute training SQL files
echo "Executing training SQL files..."
execute_sql_files "$sql_folder" "${train_sql_files[@]}"

# Wait for user input
echo "Please retrain the model and press Enter to continue..."
read -r

# Set configurations for testing
psql -U postgres -c "SET enable_bao TO true;"
psql -U postgres -c "SET enable_bao_selection TO true;"
psql -U postgres -c "SET enable_bao_rewards TO true;"
psql -U postgres -c "SET bao_num_arms TO 5;"
psql -U postgres -c "SET statement_timeout TO 500000;"

# Enable timing
psql -U postgres -c "\timing"

# Execute test SQL files
echo "Executing test SQL files..."
count=0
test_sql_files=($(find "$sql_folder" -name "test_*.sql"))

for sql_file in "${test_sql_files[@]}"; do
    echo "Executing test query: $sql_file"
    result=$(psql -U postgres -d "$DB_NAME" -f "$sql_file" 2>&1 | grep "Time:")
    echo result
    if [[ $result =~ Time:\ ([0-9]+\.[0-9]+)\ ms ]]; then
        time_ms="${BASH_REMATCH[1]}"
        time_sec=$(echo "scale=6; $time_ms / 1000" | bc)
        echo "Execution time for $sql_file: $time_sec seconds"
    fi
    count=$((count + 1))
    if [ $count -eq 25 ]; then
        echo "25 SQL files executed. Press Enter to continue..."
        read -r
    fi
done

echo "All SQL files executed."
