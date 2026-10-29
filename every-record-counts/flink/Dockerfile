ARG FLINK_IMAGE
FROM ${FLINK_IMAGE}
# Part 26: Flink SQL's Kafka connector, the fat "sql" jar for Flink 2.2 (no released build supports Flink 2.3 yet).
# Jobs and the SQL client both need it, so it goes into /opt/flink/lib on every node; checksum from Maven Central.
ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 --checksum=sha256:5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616 \
    ${MVN}/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
