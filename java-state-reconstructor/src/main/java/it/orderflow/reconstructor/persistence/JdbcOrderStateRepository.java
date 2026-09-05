package it.orderflow.reconstructor.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import it.orderflow.reconstructor.domain.Destination;
import it.orderflow.reconstructor.domain.OrderItem;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class JdbcOrderStateRepository
    implements OrderStateRepository {

    private static final String FIND_BY_ID_SQL = """
        SELECT
            order_id,
            status,
            aggregate_version,
            customer_id,
            currency,
            items,
            total_amount,
            destination,
            current_hub,
            visited_hubs,
            total_delay_minutes,
            has_delay,
            created_at,
            delivered_at,
            last_updated_at
        FROM orderflow.order_states
        WHERE order_id = ?
        """;

    private static final String UPSERT_SQL = """
        INSERT INTO orderflow.order_states (
            order_id,
            status,
            aggregate_version,
            customer_id,
            currency,
            items,
            total_amount,
            destination,
            current_hub,
            visited_hubs,
            total_delay_minutes,
            has_delay,
            created_at,
            delivered_at,
            last_updated_at
        )
        VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?
        )
        ON CONFLICT (order_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            aggregate_version = EXCLUDED.aggregate_version,
            customer_id = EXCLUDED.customer_id,
            currency = EXCLUDED.currency,
            items = EXCLUDED.items,
            total_amount = EXCLUDED.total_amount,
            destination = EXCLUDED.destination,
            current_hub = EXCLUDED.current_hub,
            visited_hubs = EXCLUDED.visited_hubs,
            total_delay_minutes =
                EXCLUDED.total_delay_minutes,
            has_delay = EXCLUDED.has_delay,
            created_at = EXCLUDED.created_at,
            delivered_at = EXCLUDED.delivered_at,
            last_updated_at = EXCLUDED.last_updated_at
        WHERE
            orderflow.order_states.aggregate_version
                < EXCLUDED.aggregate_version
        """;

    private final ObjectMapper objectMapper;

    public JdbcOrderStateRepository() {
        this.objectMapper = JsonMapper.builder().build();
    }

    @Override
    public Optional<OrderState> findById(
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
                connection.prepareStatement(FIND_BY_ID_SQL)
        ) {
            statement.setString(1, orderId);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
                }

                return Optional.of(mapState(resultSet));
            }
        }
    }

    @Override
    public void save(
        Connection connection,
        OrderState state
    ) throws SQLException {
        Objects.requireNonNull(
            connection,
            "connection is required"
        );
        Objects.requireNonNull(
            state,
            "state is required"
        );

        try (
            PreparedStatement statement =
                connection.prepareStatement(UPSERT_SQL)
        ) {
            statement.setString(1, state.orderId());
            statement.setString(
                2,
                state.status().name()
            );
            statement.setLong(3, state.version());
            statement.setString(4, state.customerId());
            statement.setString(5, state.currency());
            statement.setObject(
                6,
                jsonb(state.items())
            );
            statement.setBigDecimal(
                7,
                state.totalAmount()
            );
            statement.setObject(
                8,
                jsonb(state.destination())
            );
            statement.setString(9, state.currentHub());
            statement.setObject(
                10,
                jsonb(state.visitedHubs())
            );
            statement.setInt(
                11,
                state.totalDelayMinutes()
            );
            statement.setBoolean(
                12,
                state.hasDelay()
            );
            statement.setTimestamp(
                13,
                Timestamp.from(state.createdAt())
            );

            if (state.deliveredAt() == null) {
                statement.setTimestamp(14, null);
            } else {
                statement.setTimestamp(
                    14,
                    Timestamp.from(state.deliveredAt())
                );
            }

            statement.setTimestamp(
                15,
                Timestamp.from(state.lastUpdatedAt())
            );

            statement.executeUpdate();
        }
    }

    private OrderState mapState(
        ResultSet resultSet
    ) throws SQLException {
        List<OrderItem> items = readJson(
            resultSet.getString("items"),
            OrderItem[].class
        );

        Destination destination = readJsonObject(
            resultSet.getString("destination"),
            Destination.class
        );

        List<String> visitedHubs = readJson(
            resultSet.getString("visited_hubs"),
            String[].class
        );

        return new OrderState(
            resultSet.getString("order_id"),
            OrderStatus.valueOf(
                resultSet.getString("status")
            ),
            resultSet.getLong("aggregate_version"),
            resultSet.getString("customer_id"),
            resultSet.getString("currency"),
            items,
            resultSet.getBigDecimal("total_amount"),
            destination,
            resultSet.getString("current_hub"),
            visitedHubs,
            resultSet.getInt("total_delay_minutes"),
            resultSet.getBoolean("has_delay"),
            instant(resultSet, "created_at"),
            nullableInstant(resultSet, "delivered_at"),
            instant(resultSet, "last_updated_at")
        );
    }

    private PGobject jsonb(Object value)
        throws SQLException {
        PGobject jsonObject = new PGobject();
        jsonObject.setType("jsonb");

        try {
            jsonObject.setValue(
                objectMapper.writeValueAsString(value)
            );
        } catch (JsonProcessingException exception) {
            throw new SQLException(
                "Unable to serialize JSONB value",
                exception
            );
        }

        return jsonObject;
    }

    private <T> List<T> readJson(
        String json,
        Class<T[]> arrayType
    ) throws SQLException {
        T[] values = readJsonObject(json, arrayType);
        return List.copyOf(Arrays.asList(values));
    }

    private <T> T readJsonObject(
        String json,
        Class<T> type
    ) throws SQLException {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException exception) {
            throw new SQLException(
                "Unable to deserialize JSONB value",
                exception
            );
        }
    }

    private Instant instant(
        ResultSet resultSet,
        String column
    ) throws SQLException {
        return resultSet
            .getTimestamp(column)
            .toInstant();
    }

    private Instant nullableInstant(
        ResultSet resultSet,
        String column
    ) throws SQLException {
        Timestamp timestamp = resultSet.getTimestamp(column);

        return timestamp == null
            ? null
            : timestamp.toInstant();
    }
}