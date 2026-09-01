package it.orderflow.reconstructor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;

public final class ScenarioReplay {

    private ScenarioReplay() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println(
                "Usage: ScenarioReplay <scenario-json-path>"
            );
            System.exit(1);
        }

        Path scenarioPath = Path.of(args[0]);
        String scenarioJson = Files.readString(scenarioPath);

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode scenarioNode = objectMapper.readTree(
            scenarioJson
        );

        JsonNode eventsNode = scenarioNode.get("events");

        if (eventsNode == null || !eventsNode.isArray()) {
            throw new IllegalArgumentException(
                "Scenario must contain an events array"
            );
        }

        OrderEventDeserializer deserializer =
            new OrderEventDeserializer();

        OrderStateProjector projector =
            new OrderStateProjector();

        OrderState state = null;

        System.out.println("OrderFlow scenario replay");
        System.out.println("");

        for (JsonNode eventNode : eventsNode) {
            OrderEvent event = deserializer.deserialize(
                eventNode.toString()
            );

            state = projector.apply(state, event);

            System.out.printf(
                "v%-2d %-22s -> %-20s%n",
                event.aggregateVersion(),
                event.eventType(),
                state.status()
            );
        }

        System.out.println("");
        System.out.println("Final reconstructed state");
        System.out.println("Order:        " + state.orderId());
        System.out.println("Status:       " + state.status());
        System.out.println("Version:      " + state.version());
        System.out.println("Current hub:  " + state.currentHub());
        System.out.println(
            "Visited hubs: " + state.visitedHubs()
        );
        System.out.println(
            "Total delay:  "
                + state.totalDelayMinutes()
                + " minutes"
        );
        System.out.println(
            "Delivered at: " + state.deliveredAt()
        );
    }
}