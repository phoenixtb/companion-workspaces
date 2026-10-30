// Part 29: a Flink 2.2 DataStream job in Java: keyed state, event-time timers, a side output, checkpoints, Iceberg.
// Flink 2.2 supports Java 11 and 17 (21 is experimental, 25 unsupported), so this module targets 17 while the
// Kafka producers (Part 27) stay on 25. The job runs on the lab's session cluster: `make flink-jobs` builds two jars,
// flink-jobs-v1.jar and flink-jobs-v2.jar (see the bottom of this file).
plugins {
    java
    alias(libs.plugins.shadow)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    // compileOnly: already in the cluster's /opt/flink/lib (flink/Dockerfile). Bundling them again would put two
    // copies on the classpath; Flink loads org.apache.flink.* from its own lib first anyway.
    compileOnly(libs.flink.streaming.java)
    compileOnly(libs.flink.connector.base)         // DeliveryGuarantee (inside flink-dist on the cluster)
    compileOnly(libs.flink.table.runtime)          // RowData and its type info, what the Iceberg sink takes
    compileOnly(libs.flink.sql.connector.kafka)    // the exact jar the cluster runs: KafkaSource, KafkaSink
    compileOnly(libs.iceberg.flink.runtime)        // FlinkSink, the REST catalog
    compileOnly(libs.hadoop.client.api.flink)      // CatalogLoader.rest takes a Hadoop Configuration
    // Bundled: the Apicurio deserializer (Part 27's serdes) with the Kafka client and Avro it needs. The cluster's
    // copies of both are shaded inside connector jars, so these don't clash.
    implementation(libs.apicurio.avro.serde)
    implementation(libs.kafka.clients)
    implementation(libs.avro)
}

tasks.shadowJar {
    mergeServiceFiles()
}

tasks.named("build") {
    dependsOn(tasks.shadowJar)
}

// The state class and the version-specific rule come from src/state-v1 or src/state-v2 (Step 5's upgrade):
// ./gradlew :flink-jobs:shadowJar -PjobVersion=2  ->  build/libs/flink-jobs-v2.jar
val jobVersion = providers.gradleProperty("jobVersion").getOrElse("1")
sourceSets.main {
    java.srcDir("src/state-v$jobVersion/java")
}
tasks.shadowJar {
    archiveFileName = "flink-jobs-v$jobVersion.jar"
}
