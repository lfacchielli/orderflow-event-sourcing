package it.orderflow.reconstructor.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderStateProjectorTest {

    private final ObjectMapper objectMapper =
        new ObjectMapper();

    private final OrderEventDeserializer deserializer =
        new OrderEventDeserializer();

    private final OrderStateProjector projector =
        new OrderStateProjector();

    @Test
    void createsInitialStateFromOrderCreated()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();

        OrderState state = projector.apply(
            null,
            events.get(0)
        );

        assertEquals("ORD-2001", state.orderId());
        assertEquals(OrderStatus.CREATED, state.status());
        assertEquals(1, state.version());
        assertEquals("CUS-501", state.customerId());
        assertEquals("EUR", state.currency());
        assertEquals(
            new BigDecimal("55.0"),
            state.totalAmount()
        );
        assertEquals(2, state.items().size());
        assertEquals(
            "Firenze",
            state.destination().city()
        );
        assertEquals(
            "IT",
            state.destination().country()
        );
        assertNull(state.currentHub());
        assertTrue(state.visitedHubs().isEmpty());
        assertEquals(0, state.totalDelayMinutes());
        assertFalse(state.hasDelay());
        assertEquals(
            Instant.parse("2026-08-29T10:00:00Z"),
            state.createdAt()
        );
        assertNull(state.deliveredAt());
    }

    @Test
    void reconstructsExpectedFinalState()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();
        OrderState state = null;

        for (OrderEvent event : events) {
            state = projector.apply(state, event);
        }

        assertEquals("ORD-2001", state.orderId());
        assertEquals(OrderStatus.DELIVERED, state.status());
        assertEquals(10, state.version());
        assertEquals("CUS-501", state.customerId());
        assertEquals("EUR", state.currency());

        assertEquals(
            0,
            state.totalAmount()
                .compareTo(new BigDecimal("55.00"))
        );

        assertEquals("HUB-FIRENZE", state.currentHub());
        assertEquals(
            List.of(
                "HUB-MODENA",
                "HUB-BOLOGNA",
                "HUB-FIRENZE"
            ),
            state.visitedHubs()
        );
        assertEquals(35, state.totalDelayMinutes());
        assertTrue(state.hasDelay());
        assertEquals(
            Instant.parse("2026-08-29T10:00:00Z"),
            state.createdAt()
        );
        assertEquals(
            Instant.parse("2026-08-29T16:00:00Z"),
            state.deliveredAt()
        );
        assertEquals(
            Instant.parse("2026-08-29T16:00:00Z"),
            state.lastUpdatedAt()
        );
    }

    @Test
    void keepsPreviousStatesImmutable()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();

        OrderState created = projector.apply(
            null,
            events.get(0)
        );

        OrderState confirmed = projector.apply(
            created,
            events.get(1)
        );

        assertNotSame(created, confirmed);
        assertEquals(OrderStatus.CREATED, created.status());
        assertEquals(1, created.version());
        assertEquals(
            OrderStatus.CONFIRMED,
            confirmed.status()
        );
        assertEquals(2, confirmed.version());
    }

    @Test
    void accumulatesDelayWithoutChangingLifecycleStatus()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();
        OrderState state = null;

        for (int index = 0; index <= 7; index++) {
            state = projector.apply(
                state,
                events.get(index)
            );
        }

        assertEquals(OrderStatus.IN_TRANSIT, state.status());
        assertEquals(8, state.version());
        assertEquals(35, state.totalDelayMinutes());
        assertTrue(state.hasDelay());
        assertEquals("HUB-BOLOGNA", state.currentHub());
    }

    @Test
    void assignsAllVisitedHubs()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();
        OrderState state = null;

        for (OrderEvent event : events) {
            state = projector.apply(state, event);
        }

        assertEquals(
            List.of(
                "HUB-MODENA",
                "HUB-BOLOGNA",
                "HUB-FIRENZE"
            ),
            state.visitedHubs()
        );
    }

    @Test
    void rejectsEventThatCannotCreateState()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();

        assertThrows(
            InvalidStateTransitionException.class,
            () -> projector.apply(null, events.get(1))
        );
    }

    @Test
    void rejectsInvalidLifecycleTransition()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();

        OrderState created = projector.apply(
            null,
            events.get(0)
        );

        OrderEvent deliveredEvent = events.get(9);

        StateProjectionException exception =
            assertThrows(
                StateProjectionException.class,
                () -> projector.apply(
                    created,
                    deliveredEvent
                )
            );

        assertEquals(
            ProjectionErrorCode.VERSION_GAP,
            exception.errorCode()
        );
    }

    @Test
    void orderCollectionsAreImmutable()
        throws Exception {

        List<OrderEvent> events = readScenarioEvents();

        OrderState state = projector.apply(
            null,
            events.get(0)
        );

        assertThrows(
            UnsupportedOperationException.class,
            () -> state.items().clear()
        );

        assertThrows(
            UnsupportedOperationException.class,
            () -> state.visitedHubs().add("HUB-TEST")
        );
    }

    private List<OrderEvent> readScenarioEvents()
        throws IOException, URISyntaxException {

        String scenarioJson = readResource(
            "successful-delivery-with-delay.json"
        );

        JsonNode scenarioNode = objectMapper.readTree(
            scenarioJson
        );

        JsonNode eventsNode = scenarioNode.get("events");

        if (eventsNode == null || !eventsNode.isArray()) {
            throw new IllegalStateException(
                "Scenario does not contain an events array"
            );
        }

        List<OrderEvent> events = new ArrayList<>();

        for (JsonNode eventNode : eventsNode) {
            events.add(
                deserializer.deserialize(
                    eventNode.toString()
                )
            );
        }

        return List.copyOf(events);
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