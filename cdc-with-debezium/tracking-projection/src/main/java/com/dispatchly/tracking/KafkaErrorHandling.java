package com.dispatchly.tracking;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

@Configuration
class KafkaErrorHandling {

    // Boot wires a CommonErrorHandler bean into the listener container. Failed records are retried
    // twice, half a second apart, then published to <topic>-dlt with the exception in its headers.
    // A record that isn't valid JSON will never parse, so it skips the retries.
    @Bean
    DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
        var handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(500L, 2));
        handler.addNotRetryableExceptions(JacksonException.class);
        return handler;
    }

    // The broker doesn't auto-create topics. Same partition count as the source topic, because the
    // recoverer publishes to the same partition number by default.
    @Bean
    NewTopic shipmentsDeadLetters() {
        return TopicBuilder.name("dispatchly.public.shipments-dlt").partitions(3).replicas(1).build();
    }
}
