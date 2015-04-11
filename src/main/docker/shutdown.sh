#!/usr/bin/env bash

# Pull the name if we have one.
basename=${LOGNAME}
if [ -n "$1" ]
then
    basename=$1
fi

# Stop the containers
echo Killing...
docker kill ${basename}_storm ${basename}_zookeeper ${basename}_hadoop

# Remove them.
echo Removing...
docker rm ${basename}_storm ${basename}_zookeeper ${basename}_hadoop

