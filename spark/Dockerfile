FROM apache/spark:3.5.1-scala2.12-java11-ubuntu

USER root

# Install Python and required tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Create directory for JARs
RUN mkdir -p /opt/spark/extra-jars

# Download required JARs with compatible versions
RUN wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.8.0/iceberg-spark-runtime-3.5_2.12-1.8.0.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.102.5/nessie-spark-extensions-3.5_2.12-0.102.5.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -P /opt/spark/extra-jars \
    https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

# Set proper permissions
RUN chmod -R 755 /opt/spark/extra-jars

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /app

USER root