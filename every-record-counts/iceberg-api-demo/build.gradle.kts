// Part 7: an Iceberg commit at its lowest level, through the Java API against the lab's Polaris catalog.
plugins {
    application
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

dependencies {
    implementation(libs.iceberg.core)
    implementation(libs.iceberg.data)
    implementation(libs.iceberg.parquet)
    implementation(libs.iceberg.aws)            // S3FileIO
    runtimeOnly(libs.iceberg.aws.bundle)        // the AWS SDK it calls, shaded
    runtimeOnly(libs.hadoop.client.api)
    runtimeOnly(libs.hadoop.client.runtime)
    runtimeOnly(libs.slf4j.simple)              // shows Iceberg's own log lines, including commit retries
}

application {
    mainClass = "com.larkspur.iceberg.CommitAnatomy"
}

tasks.named<JavaExec>("run") {
    // zstd-jni loads a native library; Caffeine (inside Iceberg) still uses sun.misc.Unsafe. Allow both quietly.
    jvmArgs("--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow")
}
