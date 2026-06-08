FROM docker.io/library/spark:3.5.1

USER root
WORKDIR /opt/spark/work-dir

RUN python3 -c "from urllib.request import urlretrieve; urlretrieve('https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar', '/opt/spark/jars/hadoop-aws-3.3.4.jar')"
RUN python3 -c "from urllib.request import urlretrieve; urlretrieve('https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar', '/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar')"
RUN python3 -c "from urllib.request import urlretrieve; urlretrieve('https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar', '/opt/spark/jars/wildfly-openssl-1.0.7.Final.jar')"

COPY pyproject.toml README.md ./
COPY src ./src
COPY spark ./spark
COPY dags ./dags

# Keep runtime pod startup deterministic: S3A dependencies are baked into the
# Spark classpath instead of resolved by Ivy every time a driver pod starts.
RUN python3 -m pip install --no-cache-dir .

USER 185
