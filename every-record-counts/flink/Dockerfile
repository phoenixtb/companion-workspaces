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
