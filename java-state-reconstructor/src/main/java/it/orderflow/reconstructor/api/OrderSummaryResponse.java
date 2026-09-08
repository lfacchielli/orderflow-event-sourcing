package it.orderflow.reconstructor.api;

import it.orderflow.reconstructor.domain.OrderState;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderSummaryResponse(
    String orderId,
    String status,
    long version,
    String customerId,
    BigDecimal totalAmount,
    String currency,
    String destinationCity,
    String currentHub,
    int totalDelayMinutes,
    boolean hasDelay,
    Instant lastUpdatedAt
) {

    public static OrderSummaryResponse from(
        OrderState state
    ) {
        return new OrderSummaryResponse(
            state.orderId(),
            state.status().name(),
            state.version(),
            state.customerId(),
            state.totalAmount(),
            state.currency(),
            state.destination().city(),
            state.currentHub(),
            state.totalDelayMinutes(),
            state.hasDelay(),
            state.lastUpdatedAt()
        );
    }
}