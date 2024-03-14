#!/bin/bash

USER_ID=${1:-5}
FILE=${2:-"test.pdf"}
BASE_URL="http://localhost:9654"

curl -v -d "UPDATE user_files SET file = '$FILE' WHERE user_id = $USER_ID" "$BASE_URL"
