#!/usr/bin/env bash

docker build -t piggybanksqueal/base base/ || exit
docker build -t piggybanksqueal/zookeeper zookeeper/ || exit
docker build -t piggybanksqueal/storm storm/ || exit
docker build -t piggybanksqueal/hadoop hadoop/ || exit
docker build -t piggybanksqueal/client client/ || exit

