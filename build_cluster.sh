#!/bin/bash

# create new network
docker network create hadoop_network

# Run Hadoop
cd hadoop && ./build_cluster.sh && cd ..

# Run Airflow Cluster
cd airflow &&  ./build_airflow.sh && cd ..

# Run Spark Cluster
cd spark && ./build_cluster.sh && cd ..

