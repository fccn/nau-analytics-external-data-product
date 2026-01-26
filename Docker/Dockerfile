FROM nauedu/nau-analytics-base-spark:latest
RUN pip install --no-cache-dir git+https://github.com/fccn/nau-analytics-utils.git@main#subdirectory=common_libs/utils
# Copy your application code
COPY src/ /opt/spark/work-dir/src/

WORKDIR /opt/spark/work-dir

USER 185   
