package com.larkspur.backbone;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.apicurio.registry.serde.avro.AvroKafkaSerializer;

/** What the producers share: the lab's addresses, producer settings, and raw-zone JSON to Avro records. */
final class Lab {

    static final String POS_TOPIC = "larkspur.pos.basket-events";
    static final String CLICK_TOPIC = "larkspur.clickstream.events";
    static final String PRICE_TOPIC = "larkspur.catalog.price-changes";   // Part 28
    static final String BOOTSTRAP = System.getenv().getOrDefault("LAB_KAFKA", "localhost:9092");
    static final String REGISTRY =
            System.getenv().getOrDefault("LAB_REGISTRY", "http://localhost:8085/apis/registry/v3");
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The settings that decide whether a record arrives once, in order, and readable by every consumer. */
    static Map<String, Object> producerConfig() {
        Map<String, Object> c = new HashMap<>();
        c.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        c.put(ProducerConfig.ACKS_CONFIG, "all");                 // written to every in-sync replica before success
        c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);    // broker drops a retried batch it already has
        c.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        c.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        c.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        c.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class.getName());
        c.put("apicurio.registry.url", REGISTRY);
        c.put("apicurio.registry.auto-register", true);   // lab convenience; production registers from CI
        // Left at their 3.3.3 defaults, and what makes the bytes Confluent-compatible for Flink (Part 28):
        // apicurio.registry.headers.enabled=false (schema ID in the payload, not in headers) and
        // apicurio.registry.use-id=contentId with the 4-byte Default4ByteIdHandler: magic byte 0, 4-byte ID, Avro.
        return c;
    }

    static Schema schema(String file) {
        try {
            return new Schema.Parser().parse(Path.of("kafka/schemas", file).toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Every JSON line of the gzipped files under a raw-zone folder, in file order. */
    static List<JsonNode> readJsonLines(Path dir) {
        List<JsonNode> out = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path f : files.filter(p -> p.toString().endsWith(".jsonl.gz")).sorted().toList()) {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(Files.newInputStream(f)), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        out.add(JSON.readTree(line));
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    /** A JSON object as an Avro record of the given schema; fails on a missing required field, as Avro would. */
    static Object toAvro(JsonNode node, Schema schema) {
        return switch (schema.getType()) {
            case RECORD -> {
                GenericRecord r = new GenericData.Record(schema);
                for (Schema.Field f : schema.getFields()) {
                    JsonNode v = node.get(f.name());
                    if ((v == null || v.isNull()) && !nullable(f.schema())) {
                        throw new IllegalArgumentException("missing required field " + f.name());
                    }
                    r.put(f.name(), v == null || v.isNull() ? null : toAvro(v, f.schema()));
                }
                yield r;
            }
            case UNION -> toAvro(node, schema.getTypes().stream()
                    .filter(t -> t.getType() != Schema.Type.NULL).findFirst().orElseThrow());
            case ARRAY -> {
                List<Object> items = new ArrayList<>();
                node.forEach(item -> items.add(toAvro(item, schema.getElementType())));
                yield items;
            }
            case STRING -> node.asText();
            case INT -> node.asInt();
            case BOOLEAN -> node.asBoolean();
            case LONG -> "timestamp-millis".equals(logicalType(schema))
                    ? Instant.parse(node.asText()).toEpochMilli()   // event and upload times stay UTC instants
                    : node.asLong();
            default -> throw new IllegalArgumentException("type not used here: " + schema.getType());
        };
    }

    private static boolean nullable(Schema s) {
        return s.getType() == Schema.Type.UNION && s.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
    }

    private static String logicalType(Schema s) {
        return s.getLogicalType() == null ? null : s.getLogicalType().getName();
    }

    static String option(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return fallback;
    }

    private Lab() {}
}
