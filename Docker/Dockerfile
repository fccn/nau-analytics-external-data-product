FROM nauedu/nau-analytics-base-spark:latest

# Env for Jupyter + PySpark
ENV HOME=/home/spark \
    JUPYTER_PORT=8888 \
    JUPYTER_DIR=/opt/spark/work-dir/notebooks \
    PYSPARK_PYTHON=/usr/local/bin/python3.11 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.11 \
    PYTHONPATH="${SPARK_HOME}/python"

# PySpark + JupyterLab + libs
RUN pip install --no-cache-dir \
    jupyterlab==4.2.5

# Dirs Jupyter + notebooks
RUN mkdir -p "${JUPYTER_DIR}" \
    && mkdir -p "${HOME}/.local/share/jupyter/runtime" \
    && mkdir -p "${HOME}/.jupyter" \
    && chown -R spark:spark /home/spark /opt/spark

USER 185
WORKDIR ${JUPYTER_DIR}

EXPOSE 8888

# Default: start JupyterLab (K8s manifest pode override se quiser usar só spark-submit)
ENTRYPOINT ["bash","-lc","jupyter lab --ip=0.0.0.0 --port=${JUPYTER_PORT} --no-browser --ServerApp.root_dir=${JUPYTER_DIR} --ServerApp.token='' --ServerApp.password=''"]