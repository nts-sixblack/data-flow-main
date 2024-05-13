#!/bin/bash

# Run Hadoop
cd hadoop && ./run_cluster.sh && cd ..

# Run Airflow Cluster
cd airflow && ./run_airflow.sh && cd ..

# Run Spark Cluster
cd spark && ./run_cluster.sh && cd ..

# Run MySql
cd mysql && ./run_mysql.sh && cd ..
