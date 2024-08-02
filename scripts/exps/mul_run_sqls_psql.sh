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
-- Disable all bao related configurations
SET enable_bao TO false;
SET enable_bao_selection TO false;
SET enable_bao_rewards TO false;

-- Enable timing
\timing

-- Execute test SQL files
\echo 'Executing test SQL files...'
EOL

# Loop to add test queries
for i in $(seq 1 146); do
    echo "\\echo 'Executing test_query_${i}.sql'" >> $psql_script
    echo "\\i :sql_folder/test_query_${i}.sql" >> $psql_script
done

# Finish the psql script
echo "\\echo 'All SQL files executed.'" >> $psql_script

# Execute the generated psql script
psql -U postgres -d "$DB_NAME" -v sql_folder="$sql_folder" -f $psql_script

# Remove the psql script file
rm -f $psql_script
