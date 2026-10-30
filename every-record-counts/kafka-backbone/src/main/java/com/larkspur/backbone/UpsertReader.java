package com.larkspur.backbone;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;

/**
 * Part 28: what an upsert topic holds. Reads it from the beginning and replays it the way a reader of a compacted
 * topic would: the latest value per key wins, and a record with a null value (a tombstone) deletes the key.
 */
final class UpsertReader {

    static void run(String topic) {
        Map<String, Object> config = new LinkedHashMap<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Lab.BOOTSTRAP,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName()));
        config.put("apicurio.registry.url", Lab.REGISTRY);   // Flink registered both schemas through ccompat
        long upserts = 0;
        long tombstones = 0;
        Map<String, GenericRecord> table = new TreeMap<>();
        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(config)) {
            List<TopicPartition> parts = consumer.partitionsFor(topic).stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            Map<TopicPartition, Long> end = consumer.endOffsets(parts);
            while (parts.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                for (ConsumerRecord<Object, Object> r : consumer.poll(Duration.ofMillis(500))) {
                    GenericRecord key = (GenericRecord) r.key();
                    String k = key.get("country") + " " + key.get("store_id");
                    if (r.value() == null) {
                        tombstones++;
                        table.remove(k);
                    } else {
                        upserts++;
                        table.put(k, (GenericRecord) r.value());
                    }
                }
            }
        }
        System.out.printf("%s: %,d records = %,d upserts + %,d tombstones; %d keys alive at the end%n",
                topic, upserts + tombstones, upserts, tombstones, table.size());
        table.forEach((k, v) -> System.out.printf("  %-11s sales_minor %,15d  baskets %,4d%n",
                k, (Long) v.get("sales_minor"), (Long) v.get("baskets")));
    }

    private UpsertReader() {}
}
