# Delete all containers and images with the name "fabric-moon" or the name passed as an argument
NAME="${1:-fabric-moon}"
docker ps -aqf "name=$NAME" | xargs -I {} docker rm -f {}
docker images -q "$NAME" | xargs -I {} docker rmi -f {}
