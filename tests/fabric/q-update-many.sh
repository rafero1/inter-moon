#!/bin/bash

BASE_URL="http://localhost:9654"

start=$(date +%s%N)

curl -v -d "UPDATE user_files SET file = 'test2.5.pdf' WHERE user_id < 100" "$BASE_URL"

end=$(date +%s%N)

# print average in ms
echo "scale=2; ($end - $start) / 1000000" | bc
