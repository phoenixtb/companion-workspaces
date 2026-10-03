// The product-event contract, shared by the catalog (producer) and the AI service (consumer).
plugins {
    id("kfs.kotlin-jvm")
    id("kfs.kotlinx-serialization")
}

dependencies {
    api(libs.kotlinx.serialization.json)
    compileOnly(libs.kafka.clients) // the services bring their own, managed by Spring Boot
    testImplementation(libs.kafka.clients)
}
