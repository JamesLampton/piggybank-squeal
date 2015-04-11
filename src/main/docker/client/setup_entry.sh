#!/usr/bin/env bash

sed -i "s/@HADOOP_IP@/$HADOOP_PORT_8088_TCP_ADDR/g" /opt/hadoop/etc/hadoop/core-site.xml
sed -i "s/@HADOOP_IP@/$HADOOP_PORT_8088_TCP_ADDR/g" /opt/hadoop/etc/hadoop/yarn-site.xml

export PATH=$PATH:/opt/hadoop/bin/:/opt/storm/bin:/opt/pig/bin

if [ -z "$1" ]
then
    echo Running bash
    /bin/bash
else
    $@
fi
