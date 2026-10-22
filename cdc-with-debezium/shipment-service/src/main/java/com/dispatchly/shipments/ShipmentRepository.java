package com.dispatchly.shipments;

import java.util.Optional;
import org.springframework.data.repository.ListCrudRepository;

public interface ShipmentRepository extends ListCrudRepository<Shipment, Long> {

    Optional<Shipment> findByTrackingCode(String trackingCode);
}
