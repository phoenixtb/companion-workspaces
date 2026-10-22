package com.dispatchly.shipments;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaTopics {

    // The broker doesn't auto-create topics, so the service declares the one it writes to.
    @Bean
    NewTopic shipmentEvents() {
        return TopicBuilder.name("shipment-events").partitions(3).replicas(1).build();
    }
}
