// The companion build for "The Log Never Lies: Change Data Capture with Debezium".
// Open this folder (not the monorepo root) in your IDE.
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

rootProject.name = "cdc-with-debezium"

include("naive-poller")   // Part 1
