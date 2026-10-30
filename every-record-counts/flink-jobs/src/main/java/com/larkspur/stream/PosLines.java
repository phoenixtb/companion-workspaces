package com.larkspur.stream;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;

/**
 * Basket events from the POS topic (Avro, written by Part 27's producers through Apicurio Registry) to one
 * {@link SaleLine} per basket line. Apicurio's own deserializer reads the schema ID in each record and fetches the
 * writer schema from the registry, so a new schema version is read without redeploying the job.
 */
public class PosLines implements DeserializationSchema<SaleLine> {

    // A store's business date is its local date (Part 5). Every store in a country shares one time zone.
    private static final Map<String, ZoneId> ZONES = Map.of(
            "GB", ZoneId.of("Europe/London"), "IN", ZoneId.of("Asia/Kolkata"), "SG", ZoneId.of("Asia/Singapore"));

    private final String topic;
    private final String registryUrl;
    private transient AvroKafkaDeserializer<GenericRecord> avro;

    public PosLines(String topic, String registryUrl) {
        this.topic = topic;
        this.registryUrl = registryUrl;
    }

    @Override
    public void open(InitializationContext context) {
        avro = new AvroKafkaDeserializer<>();
        avro.configure(Map.of("apicurio.registry.url", registryUrl), false);
    }

    @Override
    public void deserialize(byte[] message, Collector<SaleLine> out) throws IOException {
        GenericRecord e = avro.deserialize(topic, message);
        long eventTs = millis(e.get("event_ts"));
        String storeId = e.get("store_id").toString();
        String businessDate = Instant.ofEpochMilli(eventTs).atZone(ZONES.get(storeId.substring(0, 2)))
                .toLocalDate().toString();
        for (Object o : (List<?>) e.get("lines")) {
            GenericRecord l = (GenericRecord) o;
            SaleLine s = new SaleLine();
            s.eventId = e.get("event_id").toString();
            s.basketId = e.get("basket_id").toString();
            s.storeId = storeId;
            s.sku = l.get("sku").toString();
            s.qty = (Integer) l.get("qty");
            s.eventTs = eventTs;
            s.uploadedAt = millis(e.get("uploaded_at"));
            s.businessDate = businessDate;
            out.collect(s);
        }
    }

    @Override
    public SaleLine deserialize(byte[] message) {
        throw new UnsupportedOperationException("one basket is several lines: Flink calls deserialize(bytes, out)");
    }

    @Override
    public boolean isEndOfStream(SaleLine next) {
        return false;
    }

    @Override
    public TypeInformation<SaleLine> getProducedType() {
        return TypeInformation.of(SaleLine.class);
    }

    private static long millis(Object v) {   // timestamp-millis: a Long, or an Instant if a conversion is registered
        return v instanceof Instant i ? i.toEpochMilli() : (Long) v;
    }
}
