package it.orderflow.reconstructor.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

public final class JdbcProcessedEventRepository
    implements ProcessedEventRepository {

    private static final String EXISTS_SQL = """
        SELECT EXISTS (
            SELECT 1
            FROM orderflow.processed_events
            WHERE event_id = ?
        )
        """;

    private static final String INSERT_SQL = """
        INSERT INTO orderflow.processed_events (
            event_id,
            order_id,
            aggregate_version,
            topic_name,
            partition_number,
            record_offset
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """;

    @Override
    public boolean exists(
        Connection connection,
        UUID eventId
    ) throws SQLException {
        Objects.requireNonNull(
            connection,
            "connection is required"
        );
        Objects.requireNonNull(
            eventId,
            "eventId is required"
        );

        try (
            PreparedStatement statement =
                connection.prepareStatement(EXISTS_SQL)
        ) {
            statement.setObject(1, eventId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new SQLException(
                        "Processed-event query returned no result"
                    );
                }

                return resultSet.getBoolean(1);
            }
        }
    }

    @Override
    public void save(
        Connection connection,
        ProcessedEvent event
    ) throws SQLException {
        Objects.requireNonNull(
            connection,
            "connection is required"
        );
        Objects.requireNonNull(
            event,
            "event is required"
        );

        try (
            PreparedStatement statement =
                connection.prepareStatement(INSERT_SQL)
        ) {
            statement.setObject(1, event.eventId());
            statement.setString(2, event.orderId());
            statement.setLong(
                3,
                event.aggregateVersion()
            );
            statement.setString(4, event.topicName());
            statement.setInt(5, event.partition());
            statement.setLong(6, event.offset());

            int insertedRows = statement.executeUpdate();

            if (insertedRows != 1) {
                throw new SQLException(
                    "Expected one processed-event row, inserted "
                        + insertedRows
                );
            }
        }
    }
}