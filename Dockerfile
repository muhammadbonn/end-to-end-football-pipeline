# Dockerfile

FROM apache/airflow:2.8.1

USER root

# Install system dependencies and Java (Crucial for PySpark!)
RUN apt-get update && apt-get install -y \
    build-essential \
    default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Copy requirements and install python packages
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt