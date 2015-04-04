#!/usr/bin/env bash

# Pull zookeeper
pushd zookeeper
ZOOKEEPER_VERSION=3.4.6
test -f zookeeper-$ZOOKEEPER_VERSION.tar.gz || curl -SsfLO "http://www.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz"
popd

# Pull storm dependencies
pushd storm
STORM_VERSION=0.9.3
test -f apache-storm-$STORM_VERSION.tar.gz || curl -SsfLO "http://www.apache.org/dist/storm/apache-storm-$STORM_VERSION/apache-storm-$STORM_VERSION.tar.gz"
popd
