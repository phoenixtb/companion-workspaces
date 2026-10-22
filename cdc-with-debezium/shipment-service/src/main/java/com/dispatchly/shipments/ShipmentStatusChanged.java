package com.dispatchly.shipments;

import java.time.Instant;

// The published contract for notifications: written to the outbox, routed to dispatchly.shipment.events.
public record ShipmentStatusChanged(String trackingCode, ShipmentStatus status, String hubId, Instant occurredAt) {
}
