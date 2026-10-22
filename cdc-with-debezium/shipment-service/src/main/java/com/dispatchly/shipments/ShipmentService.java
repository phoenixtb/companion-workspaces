package com.dispatchly.shipments;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ShipmentService {

    private final ShipmentRepository shipmentRepository;
    private final KafkaTemplate<String, ShipmentStatusChanged> kafkaTemplate;

    ShipmentService(ShipmentRepository shipmentRepository, KafkaTemplate<String, ShipmentStatusChanged> kafkaTemplate) {
        this.shipmentRepository = shipmentRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public Shipment create(String trackingCode, String hubId, Instant eta, BigDecimal codAmount) {
        return shipmentRepository.save(Shipment.create(trackingCode, hubId, eta, codAmount));
    }

    // The legacy dual write from Part 1's incident. tracking-projection no longer depends on it
    // (it reads the WAL through Debezium); notifications still do, until Part 3 replaces it.
    public Shipment changeStatus(String trackingCode, ShipmentStatus status, String hubId) {
        var shipment = find(trackingCode).moveTo(status, hubId);
        var event = new ShipmentStatusChanged(trackingCode, status, hubId, shipment.updatedAt());
        shipmentRepository.save(shipment);              // commits
        kafkaTemplate.send("shipment-events", event);   // lost if the pod dies first
        return find(trackingCode);
    }

    public void cancel(String trackingCode) {
        shipmentRepository.delete(find(trackingCode));
    }

    private Shipment find(String trackingCode) {
        return shipmentRepository.findByTrackingCode(trackingCode)
                .orElseThrow(() -> new NoSuchElementException("No shipment " + trackingCode));
    }
}
