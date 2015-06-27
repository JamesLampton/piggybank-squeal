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

# Start rabbitmq
docker run -d -P -e RABBITMQ_NODENAME=my-rabbit --name ${basename}_rabbit rabbitmq:3-management

# Start Storm
docker run -d -P --name ${basename}_storm --link ${basename}_zookeeper:zookeeper --link ${basename}_hadoop:hadoop --link ${basename}_rabbit:rabbit piggybanksqueal/storm

# Tell them how to start the client
echo Run your client: docker run -i -t --rm=true --name ${basename}_client --link ${basename}_storm:nimbus --link ${basename}_hadoop:hadoop --link ${basename}_rabbit:rabbit piggybanksqueal/client
