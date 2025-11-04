# syntax=docker/dockerfile:1
###########################################
# Stage 1: Build Python 3.11.6 from source
###########################################
FROM ubuntu:22.04 AS python-build
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON_VERSION=3.11.6
ENV PREFIX=/usr/local
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    libsqlite3-dev \
    libbz2-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src
RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
 && tar -xzf Python-${PYTHON_VERSION}.tgz
WORKDIR /usr/src/Python-${PYTHON_VERSION}
RUN ./configure --enable-optimizations --prefix=${PREFIX} \
 && make -j"$(nproc)" \
 && make altinstall
RUN ln -sf ${PREFIX}/bin/python3.11 /usr/local/bin/python \
 && ln -sf ${PREFIX}/bin/pip3.11 /usr/local/bin/pip

###########################################
# Stage 2: Get entrypoint from official Spark
###########################################
FROM apache/spark:3.5.6 AS spark-official

###########################################
# Stage 3: Spark + Delta + Cloud connectors
###########################################
FROM ubuntu:22.04 AS spark-base
ARG SPARK_VERSION=3.5.6
ARG HADOOP_VERSION=3
ARG DELTA_VERSION=3.2.1
ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install Java + basic utilities
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    wget \
    bash \
    tini \
    ca-certificates \
    procps \
 && rm -rf /var/lib/apt/lists/*

# Copy compiled Python
COPY --from=python-build /usr/local /usr/local

# Copy entrypoint script from official Spark image
COPY --from=spark-official /opt/entrypoint.sh /opt/entrypoint.sh
COPY --from=spark-official /opt/decom.sh /opt/decom.sh
RUN chmod +x /opt/entrypoint.sh /opt/decom.sh

# Download Apache Spark prebuilt for Hadoop 3
WORKDIR /opt
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
 && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
 && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Add useful connectors (Delta, AWS, Azure, MySQL)
WORKDIR $SPARK_HOME/jars
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.375/aws-java-sdk-bundle-1.12.375.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar && \
    wget https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.24.0/azure-storage-blob-12.24.0.jar && \
    wget https://repo1.maven.org/maven2/com/azure/azure-identity/1.7.0/azure-identity-1.7.0.jar && \
    wget https://repo1.maven.org/maven2/com/azure/azure-core/1.42.0/azure-core-1.42.0.jar && \
    wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/delta/delta-kernel-api/${DELTA_VERSION}/delta-kernel-api-${DELTA_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar

###########################################
# Stage 4: Final runtime image for K8s + Jupyter
###########################################
###########################################
# Stage 4: Final runtime image for K8s + Jupyter
###########################################
FROM spark-base AS final

# Create non-root user WITH home directory
RUN groupadd -r -g 185 spark && \
    useradd -m -r -u 185 -g 185 -d /home/spark spark

# Set HOME and Jupyter runtime dir
ENV HOME=/home/spark \
    JUPYTER_PORT=8888 \
    JUPYTER_DIR=/opt/spark/work-dir/notebooks \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# ✅ Install PySpark + JupyterLab + common libs
RUN pip install --no-cache-dir \
    pyspark==3.5.6 \
    pandas \
    numpy \
    jupyterlab==4.2.5

# Prepare directories Jupyter expects and fix ownership
RUN mkdir -p ${JUPYTER_DIR} \
    && mkdir -p ${HOME}/.local/share/jupyter/runtime \
    && mkdir -p ${HOME}/.jupyter \
    && chown -R spark:spark /home/spark /opt/spark

USER 185
WORKDIR ${JUPYTER_DIR}

# Expose port for Jupyter
EXPOSE 8888

# EntryPoint: use ServerApp.root_dir (notebook_dir is deprecated)
ENTRYPOINT ["bash","-lc","jupyter lab --ip=0.0.0.0 --port=${JUPYTER_PORT} --no-browser --ServerApp.root_dir=${JUPYTER_DIR} --ServerApp.token='' --ServerApp.password=''"]