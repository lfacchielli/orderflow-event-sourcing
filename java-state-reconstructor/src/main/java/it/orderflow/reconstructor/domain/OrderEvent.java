package it.orderflow.reconstructor.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record OrderEvent(
    UUID eventId,
    EventType eventType,
    String aggregateId,
    long aggregateVersion,
    Instant occurredAt,
    String producerId,
    ProducerType producerType,
    String correlationId,
    JsonNode payload
) {

    public OrderEvent {
        Objects.requireNonNull(
            eventId,
            "eventId is required"
        );
        Objects.requireNonNull(
            eventType,
            "eventType is required"
        );
        Objects.requireNonNull(
            aggregateId,
            "aggregateId is required"
        );
        Objects.requireNonNull(
            occurredAt,
            "occurredAt is required"
        );
        Objects.requireNonNull(
            producerId,
            "producerId is required"
        );
        Objects.requireNonNull(
            producerType,
            "producerType is required"
        );
        Objects.requireNonNull(
            correlationId,
            "correlationId is required"
        );
        Objects.requireNonNull(
            payload,
            "payload is required"
        );

        if (aggregateId.isBlank()) {
            throw new IllegalArgumentException(
                "aggregateId cannot be blank"
            );
        }

        if (aggregateVersion < 1) {
            throw new IllegalArgumentException(
                "aggregateVersion must be greater than zero"
            );
        }

        if (producerId.isBlank()) {
            throw new IllegalArgumentException(
                "producerId cannot be blank"
            );
        }

        if (correlationId.isBlank()) {
            throw new IllegalArgumentException(
                "correlationId cannot be blank"
            );
        }
    }
}