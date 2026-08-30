package it.orderflow.reconstructor.domain;

public enum OrderStatus {
    CREATED,
    CONFIRMED,
    PAID,
    INVENTORY_RESERVED,
    PACKED,
    IN_TRANSIT,
    OUT_FOR_DELIVERY,
    DELIVERED,
    CANCELLED,
    DELIVERY_FAILED
}