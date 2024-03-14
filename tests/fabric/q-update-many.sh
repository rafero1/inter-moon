#!/bin/bash

BASE_URL="http://localhost:9654"

curl -v -d "UPDATE user_files SET file = 'test2.5.pdf' WHERE user_id < 100" "$BASE_URL"
