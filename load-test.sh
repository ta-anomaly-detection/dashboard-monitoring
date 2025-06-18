#!/bin/bash

# Log start time
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
echo "Start time: $START_TIME"

# Run `hey` POST request
hey -n 10000 -c 20 \
  -m POST \
  -T "application/json" \
  -H "Authorization: bfb8ed93-a1ae-4ee8-b4ec-a50ab54ebfa7" \
  -d '{"first_name":"John","last_name":"Doe","email":"john@example.com","phone":"08123456789"}' \
  http://localhost:3000/api/contacts

# Log end time
END_TIME=$(date '+%Y-%m-%d %H:%M:%S')
echo "End time: $END_TIME"
