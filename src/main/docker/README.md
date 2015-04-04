

Prune exited containers:
    docker rm $(docker ps -a -q -f status=exited)
    docker rmi $(docker images | grep "^<none>" | awk '{print $3}')
