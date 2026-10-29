// Part 27: Java producers for Larkspur's POS and clickstream topics, Avro through Apicurio Registry.
plugins {
    application
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

dependencies {
    implementation(libs.kafka.clients)
    implementation(libs.apicurio.avro.serde)   // AvroKafkaSerializer: registers/looks up schemas, frames each record
    implementation(libs.apicurio.sdk)          // the registry's error types, to print why a schema was refused
    implementation(libs.avro)
    implementation(libs.jackson.databind)       // reads the raw zone's JSON lines
    runtimeOnly(libs.slf4j.simple)
}

application {
    mainClass = "com.larkspur.backbone.Backbone"
}

tasks.named<JavaExec>("run") {
    workingDir = rootDir   // paths like data/raw/... and kafka/schemas/... are relative to the lab folder
    jvmArgs("--enable-native-access=ALL-UNNAMED", "--sun-misc-unsafe-memory-access=allow")
}
