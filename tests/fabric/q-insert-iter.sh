#!/bin/bash

BASE_URL="http://localhost:9654"

times=()
# execute query many times
ITERATIONS=$1
for i in $(seq 1 $ITERATIONS)
do
    start=$(date +%s%N)
    curl -v -d "INSERT INTO user_files (user_id, file) VALUES ("$i", 'test.pdf')" "$BASE_URL"
    end=$(date +%s%N)
    times+=($((end-start)))
done

# print average in ms
echo "scale=2; $( IFS=+; echo "(${times[*]})" ) / $ITERATIONS / 1000000" | bc
