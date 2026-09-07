package it.orderflow.reconstructor.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.DatabaseSettings;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class SnapshotReplayIntegrationCheck {

    private static final String ORDER_ID = "ORD-2001";
    private static final long TARGET_VERSION = 10;

    private SnapshotReplayIntegrationCheck() {
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new IllegalArgumentException(
                "Scenario path is required"
            );
        }

        List<OrderEvent> events = readEvents(Path.of(args[0]));

        OrderStateProjector projector = new OrderStateProjector();
        SnapshotReplayService replayService = new SnapshotReplayService(projector);

        SnapshotReplayResult fullReplay = replayService.replay(
            null,
            events,
            TARGET_VERSION
        );

        ConnectionFactory connectionFactory = new ConnectionFactory(
            DatabaseSettings.fromEnvironment()
        );

        OrderSnapshotRepository repository = new JdbcOrderSnapshotRepository();

        try (Connection connection = connectionFactory.openConnection()) {
            OrderSnapshot snapshot = repository
                .findLatestBefore(
                    connection,
                    ORDER_ID,
                    TARGET_VERSION
                )
                .orElseThrow(() -> new IllegalStateException(
                    "No suitable snapshot found"
                ));

            SnapshotReplayResult optimizedReplay = replayService.replay(
                snapshot,
                events,
                TARGET_VERSION
            );

            OrderState fullState = fullReplay.state();
            OrderState optimizedState = optimizedReplay.state();

            System.out.println("");
            System.out.println("Full replay state:");
            System.out.println(fullState);

            System.out.println("");
            System.out.println("Snapshot replay state:");
            System.out.println(optimizedState);

            System.out.println("");
            System.out.println("Field comparison:");

            printComparison("orderId", fullState.orderId(), optimizedState.orderId());
            printComparison("status", fullState.status(), optimizedState.status());
            printComparison("version", fullState.version(), optimizedState.version());
            printComparison("customerId", fullState.customerId(), optimizedState.customerId());
            printComparison("currency", fullState.currency(), optimizedState.currency());
            printComparison("items", fullState.items(), optimizedState.items());
            printComparison("totalAmount", fullState.totalAmount(), optimizedState.totalAmount());
            printComparison("destination", fullState.destination(), optimizedState.destination());
            printComparison("currentHub", fullState.currentHub(), optimizedState.currentHub());
            printComparison("visitedHubs", fullState.visitedHubs(), optimizedState.visitedHubs());
            printComparison("totalDelayMinutes", fullState.totalDelayMinutes(), optimizedState.totalDelayMinutes());
            printComparison("hasDelay", fullState.hasDelay(), optimizedState.hasDelay());
            printComparison("createdAt", fullState.createdAt(), optimizedState.createdAt());
            printComparison("deliveredAt", fullState.deliveredAt(), optimizedState.deliveredAt());
            printComparison("lastUpdatedAt", fullState.lastUpdatedAt(), optimizedState.lastUpdatedAt());

            if (!statesAreEquivalent(fullState, optimizedState)) {
                throw new IllegalStateException(
                    "Snapshot replay differs from full replay"
                );
            }

            System.out.println("Snapshot replay comparison completed.");
            System.out.println("Order:                  " + optimizedState.orderId());
            System.out.println("Target version:         " + TARGET_VERSION);
            System.out.println("Snapshot version:       " + optimizedReplay.startingVersion());
            System.out.println("Full replay events:     " + fullReplay.appliedEvents());
            System.out.println("Snapshot replay events: " + optimizedReplay.appliedEvents());
            System.out.println("Final status:           " + optimizedState.status());
            System.out.println("States match:           true");
        }
    }

    private static List<OrderEvent> readEvents(Path scenarioPath) throws Exception {
        JsonNode eventsNode = new ObjectMapper()
            .readTree(Files.readString(scenarioPath))
            .get("events");

        if (eventsNode == null || !eventsNode.isArray()) {
            throw new IllegalArgumentException(
                "Scenario must contain an events array"
            );
        }

        OrderEventDeserializer deserializer = new OrderEventDeserializer();
        List<OrderEvent> events = new ArrayList<>();

        for (JsonNode eventNode : eventsNode) {
            events.add(
                deserializer.deserialize(eventNode.toString())
            );
        }

        return List.copyOf(events);
    }

    private static void printComparison(
        String field,
        Object expected,
        Object actual
    ) {
        boolean equal = Objects.equals(expected, actual);

        System.out.printf(
            "%-20s equal=%-5s full=%s | snapshot=%s%n",
            field,
            equal,
            expected,
            actual
        );
    }

    private static boolean statesAreEquivalent(
        OrderState first,
        OrderState second
    ) {
        boolean amountsMatch = (first.totalAmount() == null && second.totalAmount() == null)
            || (first.totalAmount() != null && second.totalAmount() != null 
                && first.totalAmount().compareTo(second.totalAmount()) == 0);

        return Objects.equals(first.orderId(), second.orderId())
            && first.status() == second.status()
            && first.version() == second.version()
            && Objects.equals(first.customerId(), second.customerId())
            && Objects.equals(first.currency(), second.currency())
            && Objects.equals(first.items(), second.items())
            && amountsMatch
            && Objects.equals(first.destination(), second.destination())
            && Objects.equals(first.currentHub(), second.currentHub())
            && Objects.equals(first.visitedHubs(), second.visitedHubs())
            && first.totalDelayMinutes() == second.totalDelayMinutes()
            && first.hasDelay() == second.hasDelay()
            && Objects.equals(first.createdAt(), second.createdAt())
            && Objects.equals(first.deliveredAt(), second.deliveredAt())
            && Objects.equals(first.lastUpdatedAt(), second.lastUpdatedAt());
    }
}