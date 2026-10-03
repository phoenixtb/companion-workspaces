plugins {
    id("kfs.spring-service")
    id("kfs.spring-jpa")
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-flyway")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    // Part 14: events, a reactive HTTP client for suspend functions, and coroutine support.
    implementation(project(":shared:product-events"))
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-webclient")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("io.micrometer:context-propagation") // carries ThreadLocals (MDC, tracing) across threads
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("tools.jackson.module:jackson-module-kotlin")
    runtimeOnly("org.postgresql:postgresql")
    testImplementation("org.springframework.boot:spring-boot-starter-data-jpa-test")
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.testcontainers:testcontainers-kafka")
}
