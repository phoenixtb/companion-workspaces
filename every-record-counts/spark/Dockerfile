ARG SPARK_BASE_IMAGE
FROM ${SPARK_BASE_IMAGE}
ARG ICEBERG_VERSION
# The Iceberg runtime for Spark 4.1 / Scala 2.13, and the bundle with Iceberg's S3FileIO.
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
# Part 8: Delta Lake for Spark 4.1 and its UniForm module (delta-iceberg, which supports Spark 4.1 only), plus
# Hadoop's S3A file system so Delta can write to SeaweedFS. S3A needs AWS SDK v2: iceberg-aws-bundle 1.12.0 already
# has SDK 2.54.17's S3 client; the transfer manager and Apache HTTP client of the same version complete it.
# Kept out of /opt/spark/jars so Iceberg-only sessions are unchanged; Part 8's jobs load them (spark/delta.conf.template).
ARG DELTA_VERSION
ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MVN}/io/delta/delta-spark_4.1_2.13/${DELTA_VERSION}/delta-spark_4.1_2.13-${DELTA_VERSION}.jar /opt/lakehouse/delta-jars/
ADD --chmod=644 ${MVN}/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar /opt/lakehouse/delta-jars/
ADD --chmod=644 ${MVN}/io/delta/delta-iceberg_2.13/${DELTA_VERSION}/delta-iceberg_2.13-${DELTA_VERSION}.jar /opt/lakehouse/delta-jars/
ADD --chmod=644 ${MVN}/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar /opt/lakehouse/delta-jars/
ADD --chmod=644 ${MVN}/software/amazon/awssdk/s3-transfer-manager/2.54.17/s3-transfer-manager-2.54.17.jar /opt/lakehouse/delta-jars/
ADD --chmod=644 ${MVN}/software/amazon/awssdk/apache-client/2.54.17/apache-client-2.54.17.jar /opt/lakehouse/delta-jars/
USER root
RUN mkdir -p /opt/lakehouse && chmod 755 /opt/lakehouse /opt/lakehouse/delta-jars
USER spark
COPY --chmod=644 spark-defaults.conf.template delta.conf.template /opt/lakehouse/
COPY --chmod=755 start.sh /opt/lakehouse/start.sh
ENTRYPOINT ["/opt/lakehouse/start.sh"]
