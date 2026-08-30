package it.orderflow.reconstructor.serialization;

import it.orderflow.reconstructor.domain.EventType;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.ProducerType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderEventDeserializerTest {

    private final OrderEventDeserializer deserializer =
        new OrderEventDeserializer();

    @Test
    void deserializesPythonGeneratedOrderCreatedEvent()
        throws IOException, URISyntaxException {

        String json = readResource("order-created.json");

        OrderEvent event = deserializer.deserialize(json);

        assertEquals(
            EventType.ORDER_CREATED,
            event.eventType()
        );
        assertEquals(
            "ORD-2001",
            event.aggregateId()
        );
        assertEquals(
            1,
            event.aggregateVersion()
        );
        assertEquals(
            ProducerType.ECOMMERCE,
            event.producerType()
        );
        assertEquals(
            "ecommerce-node-01",
            event.producerId()
        );
        assertEquals(
            "CORR-2001",
            event.correlationId()
        );
        assertEquals(
            Instant.parse("2026-08-29T10:00:00Z"),
            event.occurredAt()
        );
    }

    @Test
    void preservesOrderCreatedPayload() throws Exception {
        String json = readResource("order-created.json");

        OrderEvent event = deserializer.deserialize(json);

        assertEquals(
            "CUS-501",
            event.payload()
                .get("customerId")
                .asText()
        );
        assertEquals(
            "EUR",
            event.payload()
                .get("currency")
                .asText()
        );
        assertEquals(
            55.0,
            event.payload()
                .get("totalAmount")
                .asDouble()
        );

        assertTrue(
            event.payload()
                .get("items")
                .isArray()
        );
        assertEquals(
            2,
            event.payload()
                .get("items")
                .size()
        );

        assertEquals(
            "PRD-100",
            event.payload()
                .get("items")
                .get(0)
                .get("productId")
                .asText()
        );
        assertEquals(
            2,
            event.payload()
                .get("items")
                .get(0)
                .get("quantity")
                .asInt()
        );
        assertEquals(
            20.0,
            event.payload()
                .get("items")
                .get(0)
                .get("unitPrice")
                .asDouble()
        );

        assertEquals(
            "Firenze",
            event.payload()
                .get("destination")
                .get("city")
                .asText()
        );
        assertEquals(
            "IT",
            event.payload()
                .get("destination")
                .get("country")
                .asText()
        );
    }

    @Test
    void rejectsBlankJson() {
        IllegalArgumentException exception =
            assertThrows(
                IllegalArgumentException.class,
                () -> deserializer.deserialize(" ")
            );

        assertEquals(
            "Event JSON cannot be null or blank",
            exception.getMessage()
        );
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(
            EventDeserializationException.class,
            () -> deserializer.deserialize(
                "{invalid-json}"
            )
        );
    }

    @Test
    void rejectsUnknownEventType() {
        String json = """
            {
              "eventId": "0af76519-1df2-4bda-932b-253e363635c1",
              "eventType": "UNKNOWN_EVENT",
              "aggregateId": "ORD-9999",
              "aggregateVersion": 1,
              "occurredAt": "2026-08-29T10:00:00Z",
              "producerId": "unknown-node",
              "producerType": "ECOMMERCE",
              "correlationId": "CORR-9999",
              "payload": {}
            }
            """;

        assertThrows(
            EventDeserializationException.class,
            () -> deserializer.deserialize(json)
        );
    }

    private String readResource(String resourceName)
        throws IOException, URISyntaxException {

        var resource = getClass()
            .getClassLoader()
            .getResource(resourceName);

        if (resource == null) {
            throw new IllegalStateException(
                "Resource not found: " + resourceName
            );
        }

        return Files.readString(
            Path.of(resource.toURI())
        );
    }
}