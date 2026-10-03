package com.shelfwise.catalog

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer

// PostgreSQL and Kafka for the test context. @ServiceConnection hands their addresses and
// credentials to Spring Boot, so there are no datasource or bootstrap-server properties to sync.
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {
    @Bean
    @ServiceConnection
    fun postgres() = PostgreSQLContainer("postgres:18-alpine")

    @Bean
    @ServiceConnection
    fun kafka() = KafkaContainer("apache/kafka-native:4.2.2") // KRaft, GraalVM-native: starts in about a second
}
