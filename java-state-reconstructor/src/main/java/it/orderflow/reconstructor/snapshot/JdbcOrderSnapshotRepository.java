package it.orderflow.reconstructor.snapshot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.orderflow.reconstructor.domain.OrderState;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public final class JdbcOrderSnapshotRepository
    implements OrderSnapshotRepository {

    private static final String FIND_LATEST_SQL = """
        SELECT
            order_id,
            aggregate_version,
            state_data,
            created_at
        FROM orderflow.order_snapshots
        WHERE order_id = ?
        ORDER BY aggregate_version DESC
        LIMIT 1
        """;

    private static final String INSERT_SQL = """
        INSERT INTO orderflow.order_snapshots (
            order_id,
            aggregate_version,
            state_data,
            created_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT (
            order_id,
            aggregate_version
        )
        DO NOTHING
        """;

    private final ObjectMapper objectMapper;

    public JdbcOrderSnapshotRepository() {
        this.objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .build();
    }

    @Override
    public Optional<OrderSnapshot> findLatest(
        Connection connection,
        String orderId
    ) throws SQLException {
        Objects.requireNonNull(
            connection,
            "connection is required"
        );
        Objects.requireNonNull(
            orderId,
            "orderId is required"
        );

        try (
            PreparedStatement statement =
                connection.prepareStatement(FIND_LATEST_SQL)
        ) {
            statement.setString(1, orderId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                OrderState state = deserializeState(
                    resultSet.getString("state_data")
                );

                return Optional.of(
                    new OrderSnapshot(
                        resultSet.getString("order_id"),
                        resultSet.getLong(
                            "aggregate_version"
                        ),
                        state,
                        resultSet
                            .getTimestamp("created_at")
                            .toInstant()
                    )
                );
            }
        }
    }

    @Override
    public void save(
        Connection connection,
        OrderSnapshot snapshot
    ) throws SQLException {
        Objects.requireNonNull(
            connection,
            "connection is required"
        );
        Objects.requireNonNull(
            snapshot,
            "snapshot is required"
        );

        try (
            PreparedStatement statement =
                connection.prepareStatement(INSERT_SQL)
        ) {
            statement.setString(
                1,
                snapshot.orderId()
            );
            statement.setLong(
                2,
                snapshot.aggregateVersion()
            );
            statement.setObject(
                3,
                jsonb(snapshot.state())
            );
            statement.setTimestamp(
                4,
                Timestamp.from(snapshot.createdAt())
            );

            statement.executeUpdate();
        }
    }

    private PGobject jsonb(OrderState state)
        throws SQLException {
        PGobject value = new PGobject();
        value.setType("jsonb");

        try {
            value.setValue(
                objectMapper.writeValueAsString(state)
            );
        } catch (JsonProcessingException exception) {
            throw new SQLException(
                "Unable to serialize snapshot state",
                exception
            );
        }

        return value;
    }

    private OrderState deserializeState(String json)
        throws SQLException {
        try {
            return objectMapper.readValue(
                json,
                OrderState.class
            );
        } catch (JsonProcessingException exception) {
            throw new SQLException(
                "Unable to deserialize snapshot state",
                exception
            );
        }
    }
}