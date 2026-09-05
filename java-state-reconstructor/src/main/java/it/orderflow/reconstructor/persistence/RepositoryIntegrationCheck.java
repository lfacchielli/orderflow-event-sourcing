package it.orderflow.reconstructor.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Optional;

public final class RepositoryIntegrationCheck {

    private RepositoryIntegrationCheck() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println(
                "Usage: RepositoryIntegrationCheck "
                    + "<scenario-json-path>"
            );
            System.exit(1);
        }

        Path scenarioPath = Path.of(args[0]);
        String scenarioJson = Files.readString(scenarioPath);

        JsonNode scenario = new ObjectMapper()
            .readTree(scenarioJson);

        JsonNode eventNodes = scenario.get("events");

        if (eventNodes == null || !eventNodes.isArray()) {
            throw new IllegalArgumentException(
                "Scenario must contain an events array"
            );
        }

        OrderEventDeserializer deserializer =
            new OrderEventDeserializer();

        OrderStateProjector projector =
            new OrderStateProjector();

        DatabaseSettings settings =
            DatabaseSettings.fromEnvironment();

        ConnectionFactory connectionFactory =
            new ConnectionFactory(settings);

        OrderStateRepository stateRepository =
            new JdbcOrderStateRepository();

        ProcessedEventRepository processedRepository =
            new JdbcProcessedEventRepository();

        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            connection.setAutoCommit(false);

            try {
                OrderState state = null;
                long offset = 0;

                for (JsonNode eventNode : eventNodes) {
                    OrderEvent event =
                        deserializer.deserialize(
                            eventNode.toString()
                        );

                    if (
                        processedRepository.exists(
                            connection,
                            event.eventId()
                        )
                    ) {
                        offset++;
                        continue;
                    }

                    state = projector.apply(state, event);

                    stateRepository.save(
                        connection,
                        state
                    );

                    processedRepository.save(
                        connection,
                        new ProcessedEvent(
                            event.eventId(),
                            event.aggregateId(),
                            event.aggregateVersion(),
                            "integration-check",
                            0,
                            offset
                        )
                    );

                    offset++;
                }

                connection.commit();

                Optional<OrderState> storedState =
                    stateRepository.findById(
                        connection,
                        "ORD-2001"
                    );

                if (storedState.isEmpty()) {
                    throw new IllegalStateException(
                        "Stored state not found"
                    );
                }

                OrderState result = storedState.get();

                System.out.println(
                    "PostgreSQL projection completed."
                );
                System.out.println(
                    "Order:        " + result.orderId()
                );
                System.out.println(
                    "Status:       " + result.status()
                );
                System.out.println(
                    "Version:      " + result.version()
                );
                System.out.println(
                    "Current hub:  " + result.currentHub()
                );
                System.out.println(
                    "Visited hubs: " + result.visitedHubs()
                );
                System.out.println(
                    "Total delay:  "
                        + result.totalDelayMinutes()
                );
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }
}