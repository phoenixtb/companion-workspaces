package com.dispatchly.tracking;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;

// The value of a Debezium change event for public.shipments (JsonConverter, schemas.enable=false).
// Field names follow the topic's JSON, mapped from snake_case by ChangeEventJson.
public record ShipmentChange(Row before, Row after, Source source, String op, long tsMs) {

    // A row image. With REPLICA IDENTITY DEFAULT, a delete's "before" carries only the key, so
    // everything except id may be null.
    public record Row(
            long id,
            String trackingCode,
            String status,
            String hubId,
            Instant eta,
            Long version,
            BigDecimal codAmount,
            String deliveryNotes) {}

    public record Source(long lsn, @JsonProperty("txId") long txId, long tsMs, String snapshot) {}
}
