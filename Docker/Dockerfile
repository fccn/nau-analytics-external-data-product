FROM nauedu/nau-analytics-base-spark:latest

# Env for Jupyter + PySpark
ENV HOME=/home/spark \
    JUPYTER_PORT=8888 \
    JUPYTER_DIR=/opt/spark/work-dir/notebooks \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    PYTHONPATH="${SPARK_HOME}/python"

USER root

# garante dirs + permissões antes de instalar
RUN mkdir -p "${JUPYTER_DIR}" /home/spark \
    && chown -R 185:185 /home/spark /opt/spark

# PySpark + JupyterLab + libs
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir jupyterlab==4.2.5

USER 185
WORKDIR ${JUPYTER_DIR}

EXPOSE 8888

ENTRYPOINT ["bash","-lc","jupyter lab --ip=0.0.0.0 --port=${JUPYTER_PORT} --no-browser --ServerApp.root_dir=${JUPYTER_DIR} --ServerApp.token='' --ServerApp.password=''"]