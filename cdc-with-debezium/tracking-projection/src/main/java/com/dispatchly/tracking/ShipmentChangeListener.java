package com.dispatchly.tracking;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

@Component
class ShipmentChangeListener {

    private static final Logger log = LoggerFactory.getLogger(ShipmentChangeListener.class);

    private final JsonMapper changeEventMapper;
    private final TrackingView view;

    ShipmentChangeListener(JsonMapper changeEventMapper, TrackingView view) {
        this.changeEventMapper = changeEventMapper;
        this.view = view;
    }

    @KafkaListener(topics = "dispatchly.public.shipments", groupId = "tracking-projection")
    void on(ConsumerRecord<String, String> record) {
        if (record.value() == null) {
            return;   // tombstone after a delete: only there for log compaction; the d event did the work
        }
        var change = changeEventMapper.readValue(record.value(), ShipmentChange.class);
        var applied = switch (change.op()) {
            case "c", "u", "r" -> view.upsert(change.after(), change.source());
            case "d" -> view.markDeleted(change.before().id(), change.source());
            default -> throw new IllegalArgumentException("Unexpected op " + change.op());
        };
        var row = change.after() != null ? change.after() : change.before();
        log.info("{} op={} id={} {} v{} lsn={}", applied ? "applied" : "skipped", change.op(), row.id(),
                row.status(), row.version(), change.source().lsn());
    }
}
