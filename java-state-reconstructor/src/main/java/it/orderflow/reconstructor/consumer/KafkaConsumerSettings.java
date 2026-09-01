package it.orderflow.reconstructor.consumer;

import java.time.Duration;
import java.util.Objects;

public record KafkaConsumerSettings(
    String bootstrapServers,
    String groupId,
    String clientId,
    String topic,
    Duration pollTimeout
) {

    public KafkaConsumerSettings {
        Objects.requireNonNull(
            bootstrapServers,
            "bootstrapServers is required"
        );
        Objects.requireNonNull(
            groupId,
            "groupId is required"
        );
        Objects.requireNonNull(
            clientId,
            "clientId is required"
        );
        Objects.requireNonNull(topic, "topic is required");
        Objects.requireNonNull(
            pollTimeout,
            "pollTimeout is required"
        );

        if (bootstrapServers.isBlank()) {
            throw new IllegalArgumentException(
                "bootstrapServers cannot be blank"
            );
        }

        if (groupId.isBlank()) {
            throw new IllegalArgumentException(
                "groupId cannot be blank"
            );
        }

        if (topic.isBlank()) {
            throw new IllegalArgumentException(
                "topic cannot be blank"
            );
        }

        if (
            pollTimeout.isZero()
                || pollTimeout.isNegative()
        ) {
            throw new IllegalArgumentException(
                "pollTimeout must be positive"
            );
        }
    }
}