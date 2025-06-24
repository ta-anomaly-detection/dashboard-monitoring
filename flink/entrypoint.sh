#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Step 1: Run initialization
echo "Running init script..."
/flink/bin/flink run -py init.py

# Step 2: Submit Flink job
echo "Submitting Flink job..."
/flink/bin/flink run -py /taskscripts/app.py --jobmanager jobmanager:8081
