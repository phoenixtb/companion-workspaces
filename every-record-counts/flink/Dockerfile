ARG FLINK_IMAGE
FROM ${FLINK_IMAGE}
# Part 26: Flink SQL's Kafka connector, the fat "sql" jar for Flink 2.2 (no released build supports Flink 2.3 yet).
# Jobs and the SQL client both need it, so it goes into /opt/flink/lib on every node; checksum from Maven Central.
ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 --checksum=sha256:5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616 \
    ${MVN}/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/
# Part 27: Flink SQL's avro-confluent format (the fat "sql" jar, Avro and a shaded Confluent registry client inside).
# It reads the 5-byte Confluent framing and fetches writer schemas by ID from Apicurio's /apis/ccompat/v7.
ADD --chmod=644 --checksum=sha256:9f37aacbbcf7c7b5dd34dd673b16f83c4f18787337cea8e6378c71a7e7405c9a \
    ${MVN}/org/apache/flink/flink-sql-avro-confluent-registry/2.2.1/flink-sql-avro-confluent-registry-2.2.1.jar /opt/flink/lib/
# Part 28: the Iceberg sink and catalog for Flink 2.2 (Iceberg 1.12.0), and the AWS bundle for S3FileIO, which reads
# and writes the lake's files on SeaweedFS. Both from Maven Central, checksums of the published jars.
ADD --chmod=644 --checksum=sha256:c79bbee8792b1115c321e4be99fab3fb36db0bf8fa2bba24b481e6da14416de3 \
    ${MVN}/org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar /opt/flink/lib/
ADD --chmod=644 --checksum=sha256:dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957 \
    ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /opt/flink/lib/
# The Iceberg runtime calls Hadoop's Configuration even for a REST catalog with S3FileIO, and Flink's image ships no
# Hadoop (without it: ClassNotFoundException: org.apache.hadoop.conf.Configuration). Hadoop's shaded client jars, the
# version Iceberg 1.12.0 builds against.
ADD --chmod=644 --checksum=sha256:6443332781f26a7199009d9517cd1eb013fbf54ca1c9759a2a9e487542b1d52a \
    ${MVN}/org/apache/hadoop/hadoop-client-api/3.4.3/hadoop-client-api-3.4.3.jar /opt/flink/lib/
ADD --chmod=644 --checksum=sha256:bd20267b0555766ae84697dd888f074ca326d8e2ec3c0008928a6ac2966c67de \
    ${MVN}/org/apache/hadoop/hadoop-client-runtime/3.4.3/hadoop-client-runtime-3.4.3.jar /opt/flink/lib/
