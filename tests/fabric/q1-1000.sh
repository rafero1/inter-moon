#!/bin/bash

BASE_URL="http://localhost:9654"

# execute query many times
ITERATIONS=$1
for i in $(seq 1 $ITERATIONS)
do
    curl --http0.9 -v -d "INSERT INTO user_files (user_id, file) VALUES ("$i", 'test.pdf')" "$BASE_URL"
done
