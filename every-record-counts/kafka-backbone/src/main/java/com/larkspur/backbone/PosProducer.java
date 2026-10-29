package com.larkspur.backbone;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One day of till uploads from the raw zone onto {@value Lab#POS_TOPIC}, keyed by till.
 *
 * <p>Transactional: each batch of 1,000 baskets commits or aborts as a unit, the way a till's upload should. With
 * {@code --abort N}, batch N is sent, aborted (as if the producer died mid-upload) and sent again in a new transaction.
 */
final class PosProducer {

    private static final int BATCH = 1_000;

    static void run(String day, int abortBatch) {
        Schema schema = Lab.schema("pos_basket_event.avsc");
        List<JsonNode> events = Lab.readJsonLines(Path.of("data/raw/pos/events", "upload_date=" + day));

        Map<String, Object> config = Lab.producerConfig();
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "pos-uploader-" + day);   // fences a zombie with the same ID
        long started = System.nanoTime();
        int committed = 0;
        int aborted = 0;
        int abortedRecords = 0;
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(config)) {
            producer.initTransactions();
            for (int from = 0, batchNo = 1; from < events.size(); from += BATCH, batchNo++) {
                List<JsonNode> batch = events.subList(from, Math.min(from + BATCH, events.size()));
                if (batchNo == abortBatch) {
                    send(producer, schema, batch);
                    producer.flush();            // the records are in the log ...
                    producer.abortTransaction(); // ... and now marked aborted
                    aborted++;
                    abortedRecords += batch.size();
                }
                send(producer, schema, batch);
                producer.commitTransaction();
                committed++;
            }
        }
        System.out.printf("%,d basket events from upload_date=%s -> %s, keyed by till_id%n",
                events.size(), day, Lab.POS_TOPIC);
        System.out.printf("transactions: %d committed, %d aborted (%,d records written, then aborted and re-sent)%n",
                committed, aborted, abortedRecords);
        System.out.printf("took %.1f s%n", (System.nanoTime() - started) / 1e9);
    }

    private static void send(KafkaProducer<String, Object> producer, Schema schema, List<JsonNode> batch) {
        producer.beginTransaction();
        for (JsonNode e : batch) {
            producer.send(new ProducerRecord<>(Lab.POS_TOPIC, e.get("till_id").asText(), Lab.toAvro(e, schema)));
        }
    }

    private PosProducer() {}
}
