// Part 1: an updated_at poller and a logical replication slot watch the same writes.
plugins {
    application
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

dependencies {
    implementation(libs.postgresql)

    testImplementation(platform(libs.junit.bom))
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass = "com.dispatchly.part1.PollerVsLog"
}

tasks.test {
    useJUnitPlatform()
}
