package com.shelfwise.catalog.config

import com.shelfwise.catalog.quotes.SupplierQuoteClient
import com.shelfwise.events.ProductEvents
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.web.service.registry.HttpServiceGroup
import org.springframework.web.service.registry.ImportHttpServices

// The supplier client is generated from its interface. WEB_CLIENT because its function is suspend:
// an unspecified client type means RestClient, which Spring refuses to use for suspend functions.
@Configuration(proxyBeanMethods = false)
@ImportHttpServices(group = "suppliers", types = [SupplierQuoteClient::class], clientType = HttpServiceGroup.ClientType.WEB_CLIENT)
class IntegrationConfiguration {
    // Created at startup by Boot's KafkaAdmin. Three partitions; the SKU key decides which.
    @Bean
    fun productEventsTopic(): NewTopic = TopicBuilder.name(ProductEvents.TOPIC).partitions(3).build()
}
