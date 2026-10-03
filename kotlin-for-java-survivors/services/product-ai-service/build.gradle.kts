plugins {
    id("kfs.spring-service")
}

val libs = the<VersionCatalogsExtension>().named("libs")

dependencyManagement {
    imports {
        mavenBom("org.springframework.ai:spring-ai-bom:${libs.findVersion("spring-ai").get().requiredVersion}")
    }
    dependencies {
        // One Qdrant client for both AI libraries, matching the server's minor version.
        dependency("io.qdrant:client:${libs.findVersion("qdrant-client").get().requiredVersion}")
    }
}

dependencies {
    implementation(project(":shared:product-events"))
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-webmvc")
    implementation("org.springframework.boot:spring-boot-starter-kafka")
    implementation("org.springframework.boot:spring-boot-starter-restclient")
    implementation("tools.jackson.module:jackson-module-kotlin")
    // Spring AI: a chat and an embedding model from Ollama, Qdrant as the vector store, and the RAG building blocks.
    implementation("org.springframework.ai:spring-ai-starter-model-ollama")
    implementation("org.springframework.ai:spring-ai-starter-vector-store-qdrant")
    implementation("org.springframework.ai:spring-ai-rag")

    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
    testImplementation("org.springframework.boot:spring-boot-starter-kafka-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.springframework.ai:spring-ai-spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.testcontainers:testcontainers-qdrant")
}
