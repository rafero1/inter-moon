#!/bin/bash

ITERATIONS=$1
USER_ID=${2:-5}
BASE_URL="http://localhost:9654"

times=()
for i in $(seq 1 $ITERATIONS)
do
    start=$(date +%s%N)
    curl -v -d "SELECT * FROM user_files WHERE user_id = "$USER_ID"" "$BASE_URL"
    end=$(date +%s%N)
    times+=($((end-start)))
done

# print average in ms
echo "scale=2; $( IFS=+; echo "(${times[*]})" ) / $ITERATIONS / 1000000" | bc
