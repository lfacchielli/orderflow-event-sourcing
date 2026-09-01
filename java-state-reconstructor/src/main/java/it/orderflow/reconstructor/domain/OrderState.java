package it.orderflow.reconstructor.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public record OrderState(
    String orderId,
    OrderStatus status,
    long version,
    String customerId,
    String currency,
    List<OrderItem> items,
    BigDecimal totalAmount,
    Destination destination,
    String currentHub,
    List<String> visitedHubs,
    int totalDelayMinutes,
    boolean hasDelay,
    Instant createdAt,
    Instant deliveredAt,
    Instant lastUpdatedAt
) {

    public OrderState {
        Objects.requireNonNull(orderId, "orderId is required");
        Objects.requireNonNull(status, "status is required");
        Objects.requireNonNull(
            customerId,
            "customerId is required"
        );
        Objects.requireNonNull(currency, "currency is required");
        Objects.requireNonNull(items, "items are required");
        Objects.requireNonNull(
            totalAmount,
            "totalAmount is required"
        );
        Objects.requireNonNull(
            destination,
            "destination is required"
        );
        Objects.requireNonNull(
            visitedHubs,
            "visitedHubs are required"
        );
        Objects.requireNonNull(
            createdAt,
            "createdAt is required"
        );
        Objects.requireNonNull(
            lastUpdatedAt,
            "lastUpdatedAt is required"
        );

        if (orderId.isBlank()) {
            throw new IllegalArgumentException(
                "orderId cannot be blank"
            );
        }

        if (version < 1) {
            throw new IllegalArgumentException(
                "version must be greater than zero"
            );
        }

        if (totalDelayMinutes < 0) {
            throw new IllegalArgumentException(
                "totalDelayMinutes cannot be negative"
            );
        }

        items = List.copyOf(items);
        visitedHubs = List.copyOf(visitedHubs);
    }

    public OrderState advance(
        OrderStatus nextStatus,
        long nextVersion,
        Instant updatedAt
    ) {
        return new OrderState(
            orderId,
            nextStatus,
            nextVersion,
            customerId,
            currency,
            items,
            totalAmount,
            destination,
            currentHub,
            visitedHubs,
            totalDelayMinutes,
            hasDelay,
            createdAt,
            deliveredAt,
            updatedAt
        );
    }

    public OrderState reachHub(
        String hubId,
        OrderStatus nextStatus,
        long nextVersion,
        Instant updatedAt
    ) {
        Objects.requireNonNull(hubId, "hubId is required");

        List<String> updatedHubs =
            new ArrayList<>(visitedHubs);

        if (!updatedHubs.contains(hubId)) {
            updatedHubs.add(hubId);
        }

        return new OrderState(
            orderId,
            nextStatus,
            nextVersion,
            customerId,
            currency,
            items,
            totalAmount,
            destination,
            hubId,
            updatedHubs,
            totalDelayMinutes,
            hasDelay,
            createdAt,
            deliveredAt,
            updatedAt
        );
    }

    public OrderState accumulateDelay(
        int delayMinutes,
        long nextVersion,
        Instant updatedAt
    ) {
        if (delayMinutes <= 0) {
            throw new IllegalArgumentException(
                "delayMinutes must be greater than zero"
            );
        }

        return new OrderState(
            orderId,
            status,
            nextVersion,
            customerId,
            currency,
            items,
            totalAmount,
            destination,
            currentHub,
            visitedHubs,
            totalDelayMinutes + delayMinutes,
            true,
            createdAt,
            deliveredAt,
            updatedAt
        );
    }

    public OrderState completeDelivery(
        String hubId,
        long nextVersion,
        Instant deliveredAt
    ) {
        OrderState stateAtDeliveryHub = reachHub(
            hubId,
            OrderStatus.DELIVERED,
            nextVersion,
            deliveredAt
        );

        return new OrderState(
            stateAtDeliveryHub.orderId,
            stateAtDeliveryHub.status,
            stateAtDeliveryHub.version,
            stateAtDeliveryHub.customerId,
            stateAtDeliveryHub.currency,
            stateAtDeliveryHub.items,
            stateAtDeliveryHub.totalAmount,
            stateAtDeliveryHub.destination,
            stateAtDeliveryHub.currentHub,
            stateAtDeliveryHub.visitedHubs,
            stateAtDeliveryHub.totalDelayMinutes,
            stateAtDeliveryHub.hasDelay,
            stateAtDeliveryHub.createdAt,
            deliveredAt,
            deliveredAt
        );
    }
}