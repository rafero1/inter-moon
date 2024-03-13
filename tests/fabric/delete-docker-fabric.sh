docker rm -f $(docker ps -aqf "name=.example.com")
docker rmi -f $(docker images -aqf "reference=*_cc*")
