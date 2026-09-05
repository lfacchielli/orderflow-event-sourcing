package it.orderflow.reconstructor.processing;

import java.util.Objects;

public record KafkaRecordMetadata(
    String topic,
    int partition,
    long offset
) {

    public KafkaRecordMetadata {
        Objects.requireNonNull(topic, "topic is required");

        if (topic.isBlank()) {
            throw new IllegalArgumentException(
                "topic cannot be blank"
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