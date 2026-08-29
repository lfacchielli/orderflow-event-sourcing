# Order Domain and Event Lifecycle

## 1. Purpose

OrderFlow models the lifecycle of logistics orders as a sequence of immutable events.

The current state of an order is not treated as the primary source of truth. It is a projection obtained by applying, in order, every event associated with the order.

Each order is identified by an `orderId`. The same value is used as:

- the Kafka record key;
- the aggregate identifier;
- the identifier of the materialized PostgreSQL projection.

Using the order identifier as the Kafka key ensures that all events belonging to the same order are assigned to the same partition.

---

## 2. Order lifecycle

The standard successful lifecycle is:

```text
ORDER_CREATED
    -> ORDER_CONFIRMED
    -> PAYMENT_COMPLETED
    -> INVENTORY_RESERVED
    -> ORDER_PACKED
    -> SHIPMENT_STARTED
    -> HUB_REACHED
    -> OUT_FOR_DELIVERY
    -> ORDER_DELIVERED