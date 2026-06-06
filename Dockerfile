FROM docker.io/library/spark:3.5.1

USER root
WORKDIR /opt/spark/work-dir

COPY pyproject.toml README.md ./
COPY src ./src
COPY spark ./spark
COPY dags ./dags

# Keep the runtime image small and deterministic. The Spark image already
# contains Python and Spark; we only install this lab package into that runtime.
RUN python3 -m pip install --no-cache-dir .

USER 185
