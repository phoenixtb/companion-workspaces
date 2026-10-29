package com.larkspur.backbone;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.apicurio.registry.rest.client.models.RuleViolationProblemDetails;

/**
 * One day of app and web events onto {@value Lab#CLICK_TOPIC}, keyed by visitor, with a chosen schema version.
 *
 * <p>{@code v1} has {@code page}; {@code v2_rename} is the web team's first try (page renamed to page_path);
 * {@code v2} keeps {@code page} and adds an optional {@code page_path}. Idempotent, not transactional.
 */
final class ClickProducer {

    static void run(String day, String version) {
        Schema schema = Lab.schema("click_event_" + version + ".avsc");
        List<JsonNode> events = Lab.readJsonLines(Path.of("data/raw/clickstream/events", "event_date=" + day));
        AtomicLong failed = new AtomicLong();
        long sent = 0;
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(Lab.producerConfig())) {
            for (JsonNode raw : events) {
                ObjectNode e = raw.deepCopy();
                String path = e.hasNonNull("page_path") ? e.get("page_path").asText() : e.path("page").asText(null);
                switch (version) {
                    case "v1" -> e.put("page", path);
                    case "v2_rename" -> e.put("page_path", path);
                    case "v2" -> e.put("page", path);   // page keeps the path; page_path stays as the client sent it
                    default -> throw new IllegalArgumentException("unknown schema " + version);
                }
                Object value = Lab.toAvro(e, schema);
                try {
                    producer.send(new ProducerRecord<>(Lab.CLICK_TOPIC, e.get("anonymous_id").asText(), value),
                            (meta, err) -> { if (err != null) failed.incrementAndGet(); });
                    sent++;
                } catch (RuntimeException err) {   // the serializer runs inside send(): a refused schema lands here
                    System.out.printf("schema %s refused on the first record:%n%s", version, why(err));
                    break;
                }
            }
            producer.flush();
        }
        System.out.printf("schema %s: %,d of %,d events from event_date=%s -> %s (%d failed)%n",
                version, sent, events.size(), day, Lab.CLICK_TOPIC, failed.get());
    }

    /** The registry's answer, from the exception chain: Apicurio's client wraps it without a readable message. */
    private static String why(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof RuleViolationProblemDetails rv) {
                // The detail repeats the title and the causes; print each once.
                StringBuilder sb = new StringBuilder(String.format("  HTTP %d: %s%n", rv.getStatus(), rv.getTitle()));
                rv.getCauses().forEach(v -> sb.append(
                        String.format("  - %s at %s%n", v.getDescription(), v.getContext())));
                return sb.toString();
            }
        }
        return "  " + t + System.lineSeparator();
    }

    private ClickProducer() {}
}
