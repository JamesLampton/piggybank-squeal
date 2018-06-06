#!/usr/bin/env bash

MIRROR_LIST="http://www.apache.org/dyn/closer.cgi"

#MIRROR=$(curl $MIRROR_LIST | grep strong | grep href | head -1 | cut -f 2 -d'"')
MIRROR=http://archive.apache.org/dist
echo USING MIRROR: $MIRROR

echo Pull Hadoop
HADOOP_VERSION=2.6.5
echo Pulling and extracting Hadoop...
curl -SsfL "$MIRROR/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" | tar -xvz -C /
mv /hadoop-$HADOOP_VERSION /hadoop

echo Pull pig
PIG_VERSION=0.13.0
echo Pulling and extracting Pig
curl -SsfL "$MIRROR/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz" | tar -xvz -C /
mv /pig-$PIG_VERSION /pig
