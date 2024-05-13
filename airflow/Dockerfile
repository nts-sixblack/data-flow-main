FROM apache/airflow:2.6.2

# --------------------------------------------------------
# Airflow
# --------------------------------------------------------
USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim procps default-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# --------------------------------------------------------
# JAVA
# --------------------------------------------------------
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# --------------------------------------------------------
# HADOOP
# --------------------------------------------------------
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_FILE=hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_PREFIX=/opt/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV USER=root
ENV HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
ENV PATH $HADOOP_PREFIX/bin/:$PATH
ENV PATH $HADOOP_HOME/bin/:$PATH

# comment if download direct from internet
COPY $HADOOP_FILE /tmp/hadoop.tar.gz

RUN set -x \
    # uncomment if download direct from internet \
    # && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xvf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz*

RUN ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop
RUN mkdir /opt/hadoop-$HADOOP_VERSION/logs
RUN mkdir /hadoop-data

ENV HADOOP_CONF_DIR=/etc/hadoop
RUN export CLASSPATH=$(hadoop classpath --glob)

# --------------------------------------------------------
# SPARK
# --------------------------------------------------------

ENV SPARK_VERSION spark-3.5.1
ENV SPARK_FILE=${SPARK_VERSION}-bin-hadoop3.tgz
ENV SPARK_URL https://dlcdn.apache.org/spark/${SPARK_VERSION}/${SPARK_VERSION}-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/$SPARK_VERSION
ENV PATH $SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYTHONHASHSEED=1

# comment if download direct from internet
COPY $SPARK_FILE /tmp/spark.tar.gz

RUN set -x \
    # && curl -fSL "${SPARK_URL}" -o /tmp/spark.tar.gz \
    && tar -xvzf /tmp/spark.tar.gz -C /opt/ \
    && rm /tmp/spark.tar.gz* \
    && mv /opt/${SPARK_VERSION}-bin-hadoop3 /opt/${SPARK_VERSION}

COPY spark-excel_2.12-3.5.0_0.20.3.jar "$SPARK_HOME/jars/spark-excel_2.12-3.5.0_0.20.3.jar"
COPY mysql-connector-java-8.0.29.jar "$SPARK_HOME/jars/mysql-connector-java-8.0.29.jar"


USER airflow

RUN pip install --default-timeout=100 --upgrade pip

ADD requirements.txt /requirements.txt

# run install
RUN pip3 install -r /requirements.txt
