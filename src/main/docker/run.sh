#!/usr/bin/env bash

# Pull the name if we have one.
basename=${LOGNAME}
if [ -n "$1" ]
then
    basename=$1
fi

# Start hadoop
docker run -d -P --name ${basename}_hadoop piggybanksqueal/hadoop

# Start Zookeeper
docker run -d --name ${basename}_zookeeper piggybanksqueal/zookeeper

# Start Storm
docker run -d -P --name ${basename}_storm --link ${basename}_zookeeper:zookeeper --link ${basename}_hadoop:hadoop piggybanksqueal/storm
