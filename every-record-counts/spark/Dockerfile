ARG SPARK_BASE_IMAGE
FROM ${SPARK_BASE_IMAGE}
ARG ICEBERG_VERSION
# The Iceberg runtime for Spark 4.1 / Scala 2.13, and the bundle with Iceberg's S3FileIO.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
USER root
RUN mkdir -p /opt/lakehouse && chmod 755 /opt/lakehouse
USER spark
COPY --chmod=644 spark-defaults.conf.template /opt/lakehouse/spark-defaults.conf.template
COPY --chmod=755 start.sh /opt/lakehouse/start.sh
ENTRYPOINT ["/opt/lakehouse/start.sh"]
