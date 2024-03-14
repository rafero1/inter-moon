#!/bin/bash

USER_ID=${1:-5}
FILE=${2:-"test.pdf"}
BASE_URL="http://localhost:9654"

curl -v -d "INSERT INTO user_files (user_id, file) VALUES ($USER_ID, '$FILE')" "$BASE_URL"
