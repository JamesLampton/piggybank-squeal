#!/bin/bash

# Pull the name if we have one.
basename=${LOGNAME}
if [ -n "$1" ]
then
    basename=$1
fi

twitterRMQPWD=${twitterRMQPWD:-guest}

docker exec ${basename}_rabbit bash -c 'rm /root/.erlang.cookie && ln -s /var/lib/rabbitmq/.erlang.cookie /root/'
docker exec ${basename}_rabbit rabbitmqctl add_vhost twitter
docker exec ${basename}_rabbit rabbitmqctl add_user twitterspout $twitterRMQPWD
docker exec ${basename}_rabbit rabbitmqctl set_permissions -p twitter twitterspout '.*' '.*' '.*'
docker exec ${basename}_rabbit rabbitmqctl set_permissions -p twitter guest '.*' '.*' '.*'

echo '#' User twitterspout added to vhost /twitter:
echo export twitterRMQPWD=\"$twitterRMQPWD\"
