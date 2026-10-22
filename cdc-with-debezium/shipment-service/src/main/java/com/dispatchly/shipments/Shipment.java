package com.dispatchly.shipments;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;

// One row of the shipments table. Spring Data JDBC increments version on every save
// and refuses the update if someone else saved in between (optimistic locking).
@Table("shipments")
public record Shipment(
        @Id Long id,
        String trackingCode,
        ShipmentStatus status,
        String hubId,
        Instant eta,
        @Version long version,
        Instant updatedAt,
        BigDecimal codAmount,
        String deliveryNotes) {

    static Shipment create(String trackingCode, String hubId, Instant eta, BigDecimal codAmount) {
        return new Shipment(null, trackingCode, ShipmentStatus.CREATED, hubId, eta, 0, Instant.now(), codAmount, null);
    }

    Shipment moveTo(ShipmentStatus newStatus, String newHubId) {
        return new Shipment(id, trackingCode, newStatus, newHubId, eta, version, Instant.now(), codAmount, deliveryNotes);
    }
}
