package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.OrderState;

import java.time.Instant;
import java.util.Objects;

public record OrderSnapshot(
    String orderId,
    long aggregateVersion,
    OrderState state,
    Instant createdAt
) {

    public OrderSnapshot {
        Objects.requireNonNull(
            orderId,
            "orderId is required"
        );
        Objects.requireNonNull(
            state,
            "state is required"
        );
        Objects.requireNonNull(
            createdAt,
            "createdAt is required"
        );

        if (orderId.isBlank()) {
            throw new IllegalArgumentException(
                "orderId cannot be blank"
            );
        }

        if (aggregateVersion < 1) {
            throw new IllegalArgumentException(
                "aggregateVersion must be greater than zero"
            );
        }

        if (!orderId.equals(state.orderId())) {
            throw new IllegalArgumentException(
                "Snapshot orderId must match state orderId"
            );
        }

        if (aggregateVersion != state.version()) {
            throw new IllegalArgumentException(
                "Snapshot version must match state version"
            );
        }
    }
}