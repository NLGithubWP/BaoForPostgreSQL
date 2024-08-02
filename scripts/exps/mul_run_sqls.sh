#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <sql_folder> <database_name>"
    exit 1
fi

# Get the command-line arguments
sql_folder="$1"
DB_NAME="$2"

# Create a psql script file
psql_script='run_queries.sql'

# Start writing to the psql script file
cat <<EOL > $psql_script
-- Set initial configurations
SET enable_bao TO true;
SET enable_bao_selection TO false;
SET enable_bao_rewards TO true;
SET bao_num_arms TO 5;
SET statement_timeout TO 500000;

-- Execute training SQL files
\i :sql_folder/train_query_1.sql
\i :sql_folder/train_query_3.sql
\i :sql_folder/train_query_4.sql
\i :sql_folder/train_query_5.sql
\i :sql_folder/train_query_7.sql
\i :sql_folder/train_query_35.sql
\i :sql_folder/train_query_67.sql
\i :sql_folder/train_query_41.sql
\i :sql_folder/train_query_61.sql
\i :sql_folder/train_query_66.sql
\i :sql_folder/train_query_74.sql
\i :sql_folder/train_query_82.sql
\i :sql_folder/train_query_99.sql
\i :sql_folder/train_query_98.sql
\i :sql_folder/train_query_96.sql
\i :sql_folder/train_query_95.sql
\i :sql_folder/train_query_94.sql
\i :sql_folder/train_query_93.sql
\i :sql_folder/train_query_92.sql
\i :sql_folder/train_query_91.sql
\i :sql_folder/train_query_90.sql
\i :sql_folder/train_query_89.sql
\i :sql_folder/train_query_88.sql

-- Pause for 30 seconds
\echo 'Pausing for 30 seconds...'
\! sleep 30

-- Set configurations for testing
SET enable_bao TO true;
SET enable_bao_selection TO true;
SET enable_bao_rewards TO true;
SET bao_num_arms TO 5;
SET statement_timeout TO 500000;

-- Enable timing
\timing

-- Execute test SQL files
\echo 'Executing test SQL files...'
EOL

# Loop to add test queries
for i in $(seq 1 146); do
    echo "\\echo 'Executing test_query_${i}.sql'" >> $psql_script
    echo "\\i :sql_folder/test_query_${i}.sql" >> $psql_script
    if (( i % 25 == 0 )); then
        cat <<EOL >> $psql_script

-- Pause for 30 seconds
\echo 'Pausing for 30 seconds...'
\! sleep 30

EOL
    fi
done

# Finish the psql script
echo "\\echo 'All SQL files executed.'" >> $psql_script

# Execute the generated psql script
psql -U postgres -d "$DB_NAME" -v sql_folder="$sql_folder" -f $psql_script

# Remove the psql script file
rm -f $psql_script
