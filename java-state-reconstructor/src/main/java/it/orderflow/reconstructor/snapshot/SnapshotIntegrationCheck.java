package it.orderflow.reconstructor.snapshot;

import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.DatabaseSettings;
import it.orderflow.reconstructor.persistence.JdbcOrderStateRepository;
import it.orderflow.reconstructor.persistence.OrderStateRepository;

import java.sql.Connection;
import java.time.Instant;

public final class SnapshotIntegrationCheck {

    private static final String ORDER_ID = "ORD-2001";

    private SnapshotIntegrationCheck() {
    }

    public static void main(String[] args)
        throws Exception {

        ConnectionFactory connectionFactory =
            new ConnectionFactory(
                DatabaseSettings.fromEnvironment()
            );

        OrderStateRepository stateRepository =
            new JdbcOrderStateRepository();

        OrderSnapshotRepository snapshotRepository =
            new JdbcOrderSnapshotRepository();

        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            connection.setAutoCommit(false);

            try {
                OrderState state = stateRepository
                    .findById(connection, ORDER_ID)
                    .orElseThrow(
                        () -> new IllegalStateException(
                            "Order state not found: " + ORDER_ID
                        )
                    );

                OrderSnapshot snapshot = new OrderSnapshot(
                    state.orderId(),
                    state.version(),
                    state,
                    Instant.now()
                );

                snapshotRepository.save(
                    connection,
                    snapshot
                );

                connection.commit();

                OrderSnapshot storedSnapshot =
                    snapshotRepository
                        .findLatest(
                            connection,
                            ORDER_ID
                        )
                        .orElseThrow(
                            () -> new IllegalStateException(
                                "Stored snapshot not found"
                            )
                        );

                verifySnapshot(
                    state,
                    storedSnapshot
                );

                printResult(storedSnapshot);
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }

    private static void verifySnapshot(
        OrderState expectedState,
        OrderSnapshot snapshot
    ) {
        if (
            !expectedState.orderId().equals(
                snapshot.orderId()
            )
        ) {
            throw new IllegalStateException(
                "Snapshot order identifier mismatch"
            );
        }

        if (
            expectedState.version()
                != snapshot.aggregateVersion()
        ) {
            throw new IllegalStateException(
                "Snapshot version mismatch"
            );
        }

        if (
            !expectedState.equals(snapshot.state())
        ) {
            throw new IllegalStateException(
                "Snapshot state differs from stored projection"
            );
        }
    }

    private static void printResult(
        OrderSnapshot snapshot
    ) {
        OrderState state = snapshot.state();

        System.out.println(
            "PostgreSQL snapshot check completed."
        );
        System.out.println(
            "Order:        " + snapshot.orderId()
        );
        System.out.println(
            "Version:      "
                + snapshot.aggregateVersion()
        );
        System.out.println(
            "Status:       " + state.status()
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
        System.out.println(
            "Created at:   " + snapshot.createdAt()
        );
    }
}