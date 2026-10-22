package com.dispatchly.shipments;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/shipments")
class ShipmentController {

    record NewShipment(String trackingCode, String hubId, Instant eta, BigDecimal codAmount) {}

    record StatusChange(ShipmentStatus status, String hubId) {}

    private final ShipmentService shipments;

    ShipmentController(ShipmentService shipments) {
        this.shipments = shipments;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Shipment create(@RequestBody NewShipment request) {
        return shipments.create(request.trackingCode(), request.hubId(), request.eta(), request.codAmount());
    }

    @PutMapping("/{trackingCode}/status")
    Shipment changeStatus(@PathVariable String trackingCode, @RequestBody StatusChange request) {
        return shipments.changeStatus(trackingCode, request.status(), request.hubId());
    }

    @DeleteMapping("/{trackingCode}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void cancel(@PathVariable String trackingCode) {
        shipments.cancel(trackingCode);
    }

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    String notFound(NoSuchElementException e) {
        return e.getMessage();
    }
}
