#!/usr/bin/env bash

# Pull the name if we have one.
basename=env_${LOGNAME}_default
if [ -n "$1" ]
then
    basename=$1
fi

# Stop the containers
docker kill ${basename}_storm ${basename}_zookeeper

# Remove them.
docker rm ${basename}_storm ${basename}_zookeeper

