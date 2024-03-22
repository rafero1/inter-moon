#!/bin/bash

USER_ID=${1:-5}
BASE_URL="http://localhost:9654"

start=$(date +%s%N)

curl -v -d "SELECT * FROM user_files WHERE user_id = $USER_ID" "$BASE_URL"

end=$(date +%s%N)

# print average in ms
echo "scale=2; ($end - $start) / 1000000" | bc
