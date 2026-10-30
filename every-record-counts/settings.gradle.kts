// Java modules of the Every Record Counts lab. Python is the main language; Java appears where the engine is
// JVM-native (the Iceberg API here; Kafka clients in Season 3). Open this folder in your IDE.
plugins {
    // Downloads a matching JDK when no installed one fits the Java 25 toolchain.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
    }
}

rootProject.name = "every-record-counts"

include("iceberg-api-demo")   // Part 7
include("kafka-backbone")     // Part 27
include("flink-jobs")         // Part 29 (Java 17: Flink 2.2 does not run on Java 25)
