FROM nauedu/nau-analytics-base-spark:latest
USER root
RUN pip install --no-cache-dir git+https://github.com/fccn/nau-analytics-utils.git@main#subdirectory=common_libs/utils
USER 185
# Copy your application code
COPY src/ /opt/spark/work-dir/src/

WORKDIR /opt/spark/work-dir

USER 185   
