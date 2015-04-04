#!/usr/bin/env bash

# Pull the name if we have one.
basename=env_${LOGNAME}_default
if [ -n "$1" ]
then
    basename=$1
fi

# Start Zookeeper
docker run -d --name ${basename}_zookeeper piggybanksqueal/zookeeper

# Start Storm
docker run -d -P --name ${basename}_storm --link ${basename}_zookeeper:zookeeper piggybanksqueal/storm
