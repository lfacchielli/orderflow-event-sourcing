package it.orderflow.reconstructor.consumer;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaConsumerSettingsTest {

    @Test
    void createsValidSettings() {
        KafkaConsumerSettings settings =
            new KafkaConsumerSettings(
                "localhost:9092",
                "order-state-reconstructor",
                "order-state-reconstructor-1",
                "order-events",
                Duration.ofMillis(500)
            );

        assertEquals(
            "localhost:9092",
            settings.bootstrapServers()
        );
        assertEquals(
            "order-state-reconstructor",
            settings.groupId()
        );
        assertEquals(
            "order-state-reconstructor-1",
            settings.clientId()
        );
        assertEquals(
            "order-events",
            settings.topic()
        );
        assertEquals(
            Duration.ofMillis(500),
            settings.pollTimeout()
        );
    }

    @Test
    void createsExpectedDefaultSettings() {
        KafkaConsumerSettings settings =
            KafkaConsumerSettings.defaultSettings();

        assertEquals(
            "localhost:9092",
            settings.bootstrapServers()
        );
        assertEquals(
            "order-state-reconstructor",
            settings.groupId()
        );
        assertEquals(
            "order-state-reconstructor-1",
            settings.clientId()
        );
        assertEquals(
            "order-events",
            settings.topic()
        );
        assertEquals(
            Duration.ofMillis(500),
            settings.pollTimeout()
        );
    }

    @Test
    void rejectsBlankBootstrapServers() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new KafkaConsumerSettings(
                " ",
                "test-group",
                "test-client",
                "order-events",
                Duration.ofMillis(500)
            )
        );
    }

    @Test
    void rejectsBlankGroupId() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new KafkaConsumerSettings(
                "localhost:9092",
                " ",
                "test-client",
                "order-events",
                Duration.ofMillis(500)
            )
        );
    }

    @Test
    void rejectsBlankClientId() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new KafkaConsumerSettings(
                "localhost:9092",
                "test-group",
                " ",
                "order-events",
                Duration.ofMillis(500)
            )
        );
    }

    @Test
    void rejectsBlankTopic() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new KafkaConsumerSettings(
                "localhost:9092",
                "test-group",
                "test-client",
                " ",
                Duration.ofMillis(500)
            )
        );
    }

    @Test
    void rejectsZeroPollTimeout() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new KafkaConsumerSettings(
                "localhost:9092",
                "test-group",
                "test-client",
                "order-events",
                Duration.ZERO
            )
        );
    }

    @Test
    void rejectsNegativePollTimeout() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new KafkaConsumerSettings(
                "localhost:9092",
                "test-group",
                "test-client",
                "order-events",
                Duration.ofMillis(-1)
            )
        );
    }

    @Test
    void rejectsNullPollTimeout() {
        assertThrows(
            NullPointerException.class,
            () -> new KafkaConsumerSettings(
                "localhost:9092",
                "test-group",
                "test-client",
                "order-events",
                null
            )
        );
    }
}