# builder step used to download and configure spark environment
FROM hadoop-base:3.3.6 AS builder

WORKDIR $SPARK_HOME

ENV SPARK_MASTER_PORT=7077 \
  SPARK_MASTER_WEBUI_PORT=8081 \
  SPARK_LOG_DIR=/opt/spark/logs \
  SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
  SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
  SPARK_WORKER_WEBUI_PORT=8081 \
  SPARK_WORKER_PORT=7000 \
  SPARK_MASTER="spark://spark-master:7077" \
  SPARK_WORKLOAD="master"

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

EXPOSE 8081 7077 6066

RUN mkdir -p $SPARK_LOG_DIR && \
  touch $SPARK_MASTER_LOG && \
  touch $SPARK_WORKER_LOG && \
  ln -sf /dev/stdout $SPARK_MASTER_LOG && \
  ln -sf /dev/stdout $SPARK_WORKER_LOG

ADD start-spark.sh start-spark.sh
COPY spark-excel_2.12-3.5.0_0.20.3.jar "$SPARK_HOME/jars/spark-excel_2.12-3.5.0_0.20.3.jar"
COPY mysql-connector-java-8.0.29.jar "$SPARK_HOME/jars/mysql-connector-java-8.0.29.jar"

# Install python packages
ADD requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

RUN chmod a+x start-spark.sh

CMD ["start-spark.sh"]
