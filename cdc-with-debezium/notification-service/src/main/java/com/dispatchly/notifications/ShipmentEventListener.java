package com.dispatchly.notifications;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
class ShipmentEventListener {

    private static final Logger log = LoggerFactory.getLogger(ShipmentEventListener.class);

    private final Inbox inbox;

    ShipmentEventListener(Inbox inbox) {
        this.inbox = inbox;
    }

    // The inbox row and the SMS share a transaction: if sending throws, the row rolls back and the
    // record is retried. A crash after sending but before the commit sends the SMS twice, so
    // at-least-once still applies to the side effect itself.
    @Transactional
    @KafkaListener(topics = "dispatchly.shipment.events", groupId = "notification-service")
    void on(ConsumerRecord<String, String> record) {
        var eventId = UUID.fromString(header(record, "id"));
        var eventType = header(record, "eventType");
        if (!inbox.firstTime(eventId, eventType)) {
            log.info("duplicate {} {} for {} ignored", eventType, eventId, record.key());
            return;
        }
        log.info("SMS for {}: {} (event {})", record.key(), record.value(), eventId);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
