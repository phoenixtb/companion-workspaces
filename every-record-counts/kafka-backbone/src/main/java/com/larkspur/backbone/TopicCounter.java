package com.larkspur.backbone;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Reads a topic from the beginning twice, once per isolation level, and counts what each consumer gets. */
final class TopicCounter {

    static void run(String topic) {
        long endOffsets = 0;
        for (String isolation : List.of("read_uncommitted", "read_committed")) {
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Lab.BOOTSTRAP,
                    ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName()))) {
                List<TopicPartition> parts = consumer.partitionsFor(topic).stream()
                        .map(p -> new TopicPartition(topic, p.partition())).toList();
                consumer.assign(parts);   // no group: nothing is committed, nothing rebalances
                consumer.seekToBeginning(parts);
                // For read_committed this is the last stable offset: nothing past an open transaction.
                Map<TopicPartition, Long> end = consumer.endOffsets(parts);
                long count = 0;
                while (parts.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                    count += consumer.poll(Duration.ofMillis(500)).count();
                }
                if (isolation.equals("read_uncommitted")) {
                    endOffsets = end.values().stream().mapToLong(Long::longValue).sum();
                    System.out.printf("%s: %d partitions, end offsets sum to %,d%n", topic, parts.size(), endOffsets);
                    System.out.println("end offset per partition: " + parts.stream()
                            .map(p -> String.format("%,d", end.get(p))).collect(Collectors.joining(" ")));
                }
                System.out.printf("%-16s consumer reads %,d records%n", isolation, count);
            }
        }
    }

    private TopicCounter() {}
}
