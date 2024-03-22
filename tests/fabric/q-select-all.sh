#!/bin/bash

start=$(date +%s%N)

curl -v -d "SELECT * FROM user_files" localhost:9654

end=$(date +%s%N)

echo "scale=2; ($end - $start) / 1000000" | bc
