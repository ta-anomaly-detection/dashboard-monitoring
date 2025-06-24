#!/bin/bash

echo "Waiting for JobManager..."
sleep 10

# Step 1: Run initialization
echo "Running init script..."
/flink/bin/flink run -py init.py

# Step 2: Submit Flink job
echo "Submitting Flink job..."
/flink/bin/flink run -py /taskscripts/app.py -m jobmanager:8081
