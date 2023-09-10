# Use an official Python runtime as the base image
FROM python:3.8-slim-buster

# Set environment variables
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/opt/spark

# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk wget && \
    rm -rf /var/lib/apt/lists/* 

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" && \
    tar -xzf "spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz" && \
    mv "spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION" /opt/spark && \
    rm "spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz"

# Install PySpark
RUN pip install --no-cache-dir pyspark==$SPARK_VERSION datetime

# Set the working directory in the container to /usr/src/app
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . /usr/src/app

# Specify the command to run on container start
CMD ["python", "./test.py"]
