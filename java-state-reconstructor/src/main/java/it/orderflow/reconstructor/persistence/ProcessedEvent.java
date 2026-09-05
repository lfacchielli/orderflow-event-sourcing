package it.orderflow.reconstructor.persistence;

import java.util.Objects;
import java.util.UUID;

public record ProcessedEvent(
    UUID eventId,
    String orderId,
    long aggregateVersion,
    String topicName,
    int partition,
    long offset
) {

    public ProcessedEvent {
        Objects.requireNonNull(
            eventId,
            "eventId is required"
        );
        Objects.requireNonNull(
            orderId,
            "orderId is required"
        );
        Objects.requireNonNull(
            topicName,
            "topicName is required"
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

        if (topicName.isBlank()) {
            throw new IllegalArgumentException(
                "topicName cannot be blank"
            );
        }

        if (partition < 0) {
            throw new IllegalArgumentException(
                "partition cannot be negative"
            );
        }

        if (offset < 0) {
            throw new IllegalArgumentException(
                "offset cannot be negative"
            );
        }
    }
}