package com.shelfwise.catalog

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.postgresql.PostgreSQLContainer

// One PostgreSQL container for the test context. @ServiceConnection hands its URL and credentials
// to Spring Boot, so there are no datasource properties to keep in sync.
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {
    @Bean
    @ServiceConnection
    fun postgres() = PostgreSQLContainer("postgres:18-alpine")
}
