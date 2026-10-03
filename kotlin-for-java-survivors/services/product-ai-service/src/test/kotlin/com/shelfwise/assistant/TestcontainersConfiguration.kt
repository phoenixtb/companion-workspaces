package com.shelfwise.assistant

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.test.context.DynamicPropertyRegistrar
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.qdrant.QdrantContainer

// Kafka and Qdrant, connected by @ServiceConnection. Qdrant's connection details come from
// spring-ai-spring-boot-testcontainers, which knows the vector store's properties.
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {
    @Bean
    @ServiceConnection
    fun kafka() = KafkaContainer("apache/kafka-native:4.2.2")

    @Bean
    @ServiceConnection
    fun qdrant() = QdrantContainer("qdrant/qdrant:v1.19.1")

    // The LangChain4j adapter reads assistant.qdrant.* (Spring AI's service connection doesn't reach it).
    @Bean
    fun qdrantLocation(qdrant: QdrantContainer) = DynamicPropertyRegistrar { registry ->
        registry.add("assistant.qdrant.host") { qdrant.host }
        registry.add("assistant.qdrant.port") { qdrant.grpcPort }
    }
}
