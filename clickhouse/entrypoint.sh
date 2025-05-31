#!/bin/bash

# Start ClickHouse server in the background
clickhouse-server --config-file=/etc/clickhouse-server/config.xml &

# Wait for ClickHouse to start
sleep 10

# Execute the SQL migration file
clickhouse-client --query="$(cat /migrations/001_create_table.sql)"

# Keep the container running
tail -f /dev/null