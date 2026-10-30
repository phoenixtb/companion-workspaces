package com.larkspur.backbone;

import java.nio.file.Path;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Part 28: the shelf-price history onto {@value Lab#PRICE_TOPIC}, keyed by SKU, oldest first. Every version stays in
 * the topic (no compaction): an as-of join replayed from the start needs the old prices, not just the current ones.
 */
final class PriceProducer {

    static void run(String dir) {
        Schema schema = Lab.schema("price_change.avsc");
        List<JsonNode> changes = Lab.readJsonLines(Path.of(dir));
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(Lab.producerConfig())) {
            for (JsonNode c : changes) {
                producer.send(new ProducerRecord<>(Lab.PRICE_TOPIC, c.get("sku").asText(), Lab.toAvro(c, schema)));
            }
            producer.flush();
        }
        System.out.printf("%,d price changes from %s -> %s, keyed by sku%n", changes.size(), dir, Lab.PRICE_TOPIC);
    }

    private PriceProducer() {}
}
