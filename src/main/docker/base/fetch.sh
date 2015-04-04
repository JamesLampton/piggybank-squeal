#!/usr/bin/env bash

echo Pull zk
ZOOKEEPER_VERSION=3.4.6
test -f zookeeper-$ZOOKEEPER_VERSION.tar.gz || curl -SsfLO "http://www.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz"

echo Pull storm
STORM_VERSION=0.9.3
test -f apache-storm-$STORM_VERSION.tar.gz || curl -SsfLO "http://www.apache.org/dist/storm/apache-storm-$STORM_VERSION/apache-storm-$STORM_VERSION.tar.gz"

echo Pull pig
PIG_VERSION=0.13.0
test -f pig-$PIG_VERSION.tar.gz || curl -SsfLO "http://www.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz"

echo Pull Hadoop
