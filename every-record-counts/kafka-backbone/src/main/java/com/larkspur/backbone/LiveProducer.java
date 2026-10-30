package com.larkspur.backbone;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Part 30: one day of till uploads replayed live, at the pace the tills uploaded them (sped up {@code --speed} times).
 *
 * <p>Each basket is sent when its {@code uploaded_at}, scaled, comes round on the wall clock, so the topic fills the
 * way a live one does. Not transactional: a till stream sends each basket as it comes, and the record's timestamp
 * (CreateTime, set at send) is the moment the event was produced, which every tier measures its freshness from.
 */
final class LiveProducer {

    static void run(String day, String topic, double speed, int limit) throws InterruptedException {
        Schema schema = Lab.schema("pos_basket_event.avsc");
        List<JsonNode> events = Lab.readJsonLines(Path.of("data/raw/pos/events", "upload_date=" + day)).stream()
                .sorted(Comparator.comparing(e -> e.get("uploaded_at").asText()))
                .limit(limit > 0 ? limit : Long.MAX_VALUE)
                .toList();
        long first = Instant.parse(events.get(0).get("uploaded_at").asText()).toEpochMilli();
        long last = Instant.parse(events.get(events.size() - 1).get("uploaded_at").asText()).toEpochMilli();

        Map<String, Object> config = Lab.producerConfig();
        config.put(ProducerConfig.LINGER_MS_CONFIG, 0);   // send at once: latency is what this replay measures
        // Register the schema with its nested records inlined, not as references: ClickHouse's AvroConfluent reader
        // fetches one schema by ID and can't resolve references (Part 30). Apicurio supports this for Avro only.
        config.put("apicurio.registry.dereference-schema", true);
        AtomicLong failed = new AtomicLong();
        long start = System.currentTimeMillis();
        System.out.printf("%,d basket events from upload_date=%s -> %s at %.0fx: %.1f minutes, started %s%n",
                events.size(), day, topic, speed, (last - first) / speed / 60_000, Instant.ofEpochMilli(start));
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(config)) {
            int sent = 0;
            for (JsonNode e : events) {
                long due = start + (long) ((Instant.parse(e.get("uploaded_at").asText()).toEpochMilli() - first)
                        / speed);
                long wait = due - System.currentTimeMillis();
                if (wait > 0) {
                    Thread.sleep(wait);
                }
                producer.send(new ProducerRecord<>(topic, e.get("till_id").asText(), Lab.toAvro(e, schema)),
                        (meta, ex) -> {
                            if (ex != null) {
                                failed.incrementAndGet();
                            }
                        });
                if (++sent % 2_000 == 0) {
                    System.out.printf("%s sent %,d%n", Instant.now(), sent);
                }
            }
            producer.flush();
            System.out.printf("%,d sent, %d failed, finished %s after %.1f minutes%n", sent, failed.get(),
                    Instant.now(), (System.currentTimeMillis() - start) / 60_000.0);
        }
    }

    private LiveProducer() {}
}
