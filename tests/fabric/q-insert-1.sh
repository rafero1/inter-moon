#!/bin/bash

USER_ID=${1:-5}
FILE=${2:-"test.pdf"}
BASE_URL="http://localhost:9654"

start=$(date +%s%N)

curl -v -d "INSERT INTO user_files (user_id, file) VALUES ($USER_ID, '$FILE')" "$BASE_URL"

end=$(date +%s%N)

# print average in ms
echo "scale=2; ($end - $start) / 1000000" | bc
