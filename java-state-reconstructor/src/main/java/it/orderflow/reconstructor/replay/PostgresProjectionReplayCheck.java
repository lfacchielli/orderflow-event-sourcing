package it.orderflow.reconstructor.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.DatabaseSettings;
import it.orderflow.reconstructor.persistence.JdbcOrderStateRepository;
import it.orderflow.reconstructor.persistence.JdbcProcessedEventRepository;
import it.orderflow.reconstructor.persistence.OrderStateRepository;
import it.orderflow.reconstructor.persistence.ProcessedEvent;
import it.orderflow.reconstructor.persistence.ProcessedEventRepository;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class PostgresProjectionReplayCheck {

    private static final String DELETE_PROCESSED_EVENTS_SQL = """
        DELETE FROM orderflow.processed_events
        WHERE order_id = ?
        """;

    private static final String DELETE_ORDER_STATE_SQL = """
        DELETE FROM orderflow.order_states
        WHERE order_id = ?
        """;

    private PostgresProjectionReplayCheck() {
    }

    public static void main(String[] args)
        throws Exception {

        if (args.length != 1) {
            System.err.println(
                "Usage: PostgresProjectionReplayCheck "
                    + "<scenario-json-path>"
            );
            System.exit(1);
        }

        Path scenarioPath = Path.of(args[0]);
        List<OrderEvent> events =
            readScenarioEvents(scenarioPath);

        String orderId = events.get(0).aggregateId();

        ConnectionFactory connectionFactory =
            new ConnectionFactory(
                DatabaseSettings.fromEnvironment()
            );

        OrderStateRepository stateRepository =
            new JdbcOrderStateRepository();

        ProcessedEventRepository processedRepository =
            new JdbcProcessedEventRepository();

        OrderReplayService replayService =
            new OrderReplayService(
                new OrderStateProjector()
            );

        OrderState originalState;

        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            originalState = stateRepository
                .findById(connection, orderId)
                .orElseThrow(
                    () -> new IllegalStateException(
                        "Original projection not found for "
                            + orderId
                    )
                );
        }

        System.out.println("Original projection found.");
        printState("Before deletion", originalState);

        deleteProjection(
            connectionFactory,
            orderId
        );

        verifyProjectionDeleted(
            connectionFactory,
            stateRepository,
            orderId
        );

        System.out.println("");
        System.out.println(
            "Projection and processed-event records deleted."
        );

        ReplayResult replayResult =
            replayService.replayAll(events);

        OrderState reconstructedState =
            replayResult.state();

        persistReconstructedProjection(
            connectionFactory,
            stateRepository,
            processedRepository,
            reconstructedState,
            events
        );

        OrderState storedReconstructedState;

        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            storedReconstructedState = stateRepository
                .findById(connection, orderId)
                .orElseThrow(
                    () -> new IllegalStateException(
                        "Reconstructed projection was not saved"
                    )
                );
        }

        printState(
            "After replay",
            storedReconstructedState
        );

        if (
            !equivalent(
                originalState,
                storedReconstructedState
            )
        ) {
            throw new IllegalStateException(
                "Reconstructed projection differs "
                    + "from the original projection"
            );
        }

        System.out.println("");
        System.out.println(
            "PostgreSQL projection replay completed."
        );
        System.out.println(
            "Applied events: "
                + replayResult.appliedEvents()
        );
        System.out.println(
            "States equivalent: true"
        );
    }

    private static void deleteProjection(
        ConnectionFactory connectionFactory,
        String orderId
    ) throws Exception {
        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            connection.setAutoCommit(false);

            try (
                PreparedStatement deleteEvents =
                    connection.prepareStatement(
                        DELETE_PROCESSED_EVENTS_SQL
                    );
                PreparedStatement deleteState =
                    connection.prepareStatement(
                        DELETE_ORDER_STATE_SQL
                    )
            ) {
                deleteEvents.setString(1, orderId);
                int deletedEvents =
                    deleteEvents.executeUpdate();

                deleteState.setString(1, orderId);
                int deletedStates =
                    deleteState.executeUpdate();

                connection.commit();

                System.out.println(
                    "Deleted processed events: "
                        + deletedEvents
                );
                System.out.println(
                    "Deleted order states: "
                        + deletedStates
                );
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    private static void verifyProjectionDeleted(
        ConnectionFactory connectionFactory,
        OrderStateRepository stateRepository,
        String orderId
    ) throws Exception {
        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            if (
                stateRepository
                    .findById(connection, orderId)
                    .isPresent()
            ) {
                throw new IllegalStateException(
                    "Projection was not deleted"
                );
            }
        }
    }

    private static void persistReconstructedProjection(
        ConnectionFactory connectionFactory,
        OrderStateRepository stateRepository,
        ProcessedEventRepository processedRepository,
        OrderState reconstructedState,
        List<OrderEvent> events
    ) throws Exception {
        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            connection.setAutoCommit(false);

            try {
                stateRepository.save(
                    connection,
                    reconstructedState
                );

                long offset = 0;

                for (OrderEvent event : events) {
                    processedRepository.save(
                        connection,
                        new ProcessedEvent(
                            event.eventId(),
                            event.aggregateId(),
                            event.aggregateVersion(),
                            "projection-replay",
                            0,
                            offset
                        )
                    );

                    offset++;
                }

                connection.commit();
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    private static List<OrderEvent> readScenarioEvents(
        Path scenarioPath
    ) throws Exception {
        String scenarioJson =
            Files.readString(scenarioPath);

        JsonNode eventsNode = new ObjectMapper()
            .readTree(scenarioJson)
            .get("events");

        if (
            eventsNode == null
                || !eventsNode.isArray()
                || eventsNode.isEmpty()
        ) {
            throw new IllegalArgumentException(
                "Scenario must contain a non-empty "
                    + "events array"
            );
        }

        OrderEventDeserializer deserializer =
            new OrderEventDeserializer();

        List<OrderEvent> events =
            new ArrayList<>();

        for (JsonNode eventNode : eventsNode) {
            events.add(
                deserializer.deserialize(
                    eventNode.toString()
                )
            );
        }

        return List.copyOf(events);
    }

    private static boolean equivalent(
        OrderState first,
        OrderState second
    ) {
        return Objects.equals(
                first.orderId(),
                second.orderId()
            )
            && first.status() == second.status()
            && first.version() == second.version()
            && Objects.equals(
                first.customerId(),
                second.customerId()
            )
            && Objects.equals(
                first.currency(),
                second.currency()
            )
            && first.items().equals(second.items())
            && sameAmount(
                first.totalAmount(),
                second.totalAmount()
            )
            && Objects.equals(
                first.destination(),
                second.destination()
            )
            && Objects.equals(
                first.currentHub(),
                second.currentHub()
            )
            && first.visitedHubs().equals(
                second.visitedHubs()
            )
            && first.totalDelayMinutes()
                == second.totalDelayMinutes()
            && first.hasDelay() == second.hasDelay()
            && Objects.equals(
                first.createdAt(),
                second.createdAt()
            )
            && Objects.equals(
                first.deliveredAt(),
                second.deliveredAt()
            )
            && Objects.equals(
                first.lastUpdatedAt(),
                second.lastUpdatedAt()
            );
    }

    private static boolean sameAmount(
        BigDecimal first,
        BigDecimal second
    ) {
        return first.compareTo(second) == 0;
    }

    private static void printState(
        String label,
        OrderState state
    ) {
        System.out.println("");
        System.out.println(label);
        System.out.println(
            "Order:        " + state.orderId()
        );
        System.out.println(
            "Status:       " + state.status()
        );
        System.out.println(
            "Version:      " + state.version()
        );
        System.out.println(
            "Current hub:  " + state.currentHub()
        );
        System.out.println(
            "Visited hubs: " + state.visitedHubs()
        );
        System.out.println(
            "Total delay:  "
                + state.totalDelayMinutes()
        );
    }
}