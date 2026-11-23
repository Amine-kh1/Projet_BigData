# One image for the Driver and the Executors prevents Python version mismatches.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
COPY requirements-notebook.txt /tmp/requirements-notebook.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && python3 -m pip install --no-cache-dir -r /tmp/requirements-notebook.txt \
    && curl --fail --location --retry 3 \
       https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
       --output /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && curl --fail --location --retry 3 \
       https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
       --output /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    && mkdir -p /workspace/evidence/spark-events \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements-notebook.txt

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/workspace:/workspace/scripts \
    PATH=/opt/spark/bin:/opt/spark/sbin:${PATH} \
    PYTHONUNBUFFERED=1

WORKDIR /workspace
# Every Compose service explicitly selects its foreground process.
ENTRYPOINT []
