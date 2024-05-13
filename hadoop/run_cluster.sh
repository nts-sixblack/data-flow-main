#!/bin/bash

# running image to container, -d to run it in daemon mode
docker-compose -f docker-compose.yml up -d --build


docker exec -it namenode bash
hadoop fs -mkdir -p logistic
hdfs dfs -put /hadoop-data/input/* logistic
exit
