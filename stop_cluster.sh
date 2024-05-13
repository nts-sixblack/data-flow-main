#!/bin/bash

# Stop Hadoop
cd hadoop && ./delete_cluster.sh && cd ..

# Stop Airflow Cluster
cd airflow &&  ./delete_airflow.sh && cd ..

# Stop Spark Cluster
cd spark && ./delete_cluster.sh && cd ..

# Stop MySql
cd mysql && ./delete_mysql.sh && cd ..

