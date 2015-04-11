#!/usr/bin/env bash

MIRROR_LIST="http://www.apache.org/dyn/closer.cgi"

MIRROR=$(curl $MIRROR_LIST | grep strong | grep href | head -1 | cut -f 2 -d'"')
echo USING MIRROR: $MIRROR

echo Pull zk
ZOOKEEPER_VERSION=3.4.6
test -f zookeeper-$ZOOKEEPER_VERSION.tar.gz || curl -SsfLO "$MIRROR/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz"

echo Pull storm
STORM_VERSION=0.9.3
test -f apache-storm-$STORM_VERSION.tar.gz || curl -SsfLO "$MIRROR/storm/apache-storm-$STORM_VERSION/apache-storm-$STORM_VERSION.tar.gz"

echo Pull Hadoop
HADOOP_VERSION=2.6.0
test -f hadoop-$HADOOP_VERSION.tar.gz || curl -SsfLO "$MIRROR/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"

echo Pull pig
PIG_VERSION=0.13.0
test -f pig-$PIG_VERSION.tar.gz || curl -SsfLO "$MIRROR/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz"

