#!/bin/bash

USER_ID=${1:-5}
BASE_URL="http://localhost:9654"

curl -v -d "SELECT * FROM user_files WHERE user_id = $USER_ID" "$BASE_URL"
