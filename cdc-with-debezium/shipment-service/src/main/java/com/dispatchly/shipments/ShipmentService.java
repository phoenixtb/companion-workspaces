package com.dispatchly.shipments;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ShipmentService {

    private final ShipmentRepository shipmentRepository;
    private final Outbox outbox;

    ShipmentService(ShipmentRepository shipmentRepository, Outbox outbox) {
        this.shipmentRepository = shipmentRepository;
        this.outbox = outbox;
    }

    public Shipment create(String trackingCode, String hubId, Instant eta, BigDecimal codAmount) {
        return shipmentRepository.save(Shipment.create(trackingCode, hubId, eta, codAmount));
    }

    // One transaction, one database: the status change and the event commit together or not at all.
    @Transactional
    public Shipment changeStatus(String trackingCode, ShipmentStatus status, String hubId) {
        var shipment = shipmentRepository.save(find(trackingCode).moveTo(status, hubId));
        outbox.publish("shipment", trackingCode, "ShipmentStatusChanged",
                new ShipmentStatusChanged(trackingCode, status, hubId, shipment.updatedAt()));
        return shipment;
    }

    public void cancel(String trackingCode) {
        shipmentRepository.delete(find(trackingCode));
    }

    private Shipment find(String trackingCode) {
        return shipmentRepository.findByTrackingCode(trackingCode)
                .orElseThrow(() -> new NoSuchElementException("No shipment " + trackingCode));
    }
}
