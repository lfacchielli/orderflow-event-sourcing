package it.orderflow.reconstructor.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import it.orderflow.reconstructor.store.InMemoryOrderStateStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OrderEventProcessorTest {

    @Test
    void reconstructsAndStoresCompleteOrderState()
        throws Exception {

        InMemoryOrderStateStore store =
            new InMemoryOrderStateStore();

        OrderEventProcessor processor =
            new OrderEventProcessor(
                store,
                new OrderStateProjector()
            );

        OrderEventDeserializer deserializer =
            new OrderEventDeserializer();

        String scenarioJson = Files.readString(
            Path.of(
                getClass()
                    .getClassLoader()
                    .getResource(
                        "successful-delivery-with-delay.json"
                    )
                    .toURI()
            )
        );

        JsonNode events = new ObjectMapper()
            .readTree(scenarioJson)
            .get("events");

        ProcessingResult firstResult = null;

        for (JsonNode eventNode : events) {
            ProcessingResult result = processor.process(
                deserializer.deserialize(
                    eventNode.toString()
                )
            );

            if (firstResult == null) {
                firstResult = result;
            }
        }

        assertNull(firstResult.previousState());

        OrderState state = store
            .findByOrderId("ORD-2001")
            .orElseThrow();

        assertEquals(OrderStatus.DELIVERED, state.status());
        assertEquals(10, state.version());
        assertEquals(35, state.totalDelayMinutes());
        assertEquals(1, store.size());
    }
}