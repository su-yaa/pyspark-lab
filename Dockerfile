FROM docker.io/library/spark:3.5.1

USER root
WORKDIR /opt/spark/work-dir

# The base Spark image does not include Hadoop's S3A connector. We bake the
# connector into the job image so Spark can write results to MinIO without
# relying on runtime downloads from the cluster.
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

COPY pyproject.toml README.md ./
COPY src ./src
COPY jobs ./jobs
COPY dags ./dags

# Keep the runtime image small and deterministic. The Spark image already
# contains Python and Spark; we only install this lab package into that runtime.
RUN python3 -m pip install --no-cache-dir .

USER 185
