package com.dispatchly.shipments;

import java.time.Instant;

// The event the notification service still consumes from the legacy shipment-events topic.
public record ShipmentStatusChanged(String trackingCode, ShipmentStatus status, String hubId, Instant occurredAt) {
}
