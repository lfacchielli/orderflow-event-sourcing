package it.orderflow.reconstructor.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class ReplayTestData {

    private ReplayTestData() {
    }

    static List<OrderEvent> scenarioEvents()
        throws Exception {

        var resource = ReplayTestData.class
            .getClassLoader()
            .getResource(
                "successful-delivery-with-delay.json"
            );

        if (resource == null) {
            throw new IllegalStateException(
                "Replay scenario resource not found"
            );
        }

        String scenarioJson = Files.readString(
            Path.of(resource.toURI())
        );

        JsonNode eventsNode = new ObjectMapper()
            .readTree(scenarioJson)
            .get("events");

        if (eventsNode == null || !eventsNode.isArray()) {
            throw new IllegalStateException(
                "Scenario does not contain an events array"
            );
        }

        OrderEventDeserializer deserializer =
            new OrderEventDeserializer();

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
}