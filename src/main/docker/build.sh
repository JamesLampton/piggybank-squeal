#!/usr/bin/env bash

docker build -t piggybanksqueal/base base/
docker build -t piggybanksqueal/zookeeper zookeeper/
docker build -t piggybanksqueal/storm storm/
docker build -t piggybanksqueal/client client/

