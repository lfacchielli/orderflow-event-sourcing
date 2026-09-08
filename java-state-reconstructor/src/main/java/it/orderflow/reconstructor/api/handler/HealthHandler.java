package it.orderflow.reconstructor.api.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import it.orderflow.reconstructor.api.HealthResponse;
import it.orderflow.reconstructor.api.JsonHttpResponse;
import it.orderflow.reconstructor.persistence.ConnectionFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Objects;

public final class HealthHandler implements HttpHandler {

    private static final String APPLICATION_NAME =
        "orderflow-state-reconstructor";

    private final ConnectionFactory connectionFactory;
    private final JsonHttpResponse response;

    public HealthHandler(
        ConnectionFactory connectionFactory,
        JsonHttpResponse response
    ) {
        this.connectionFactory = Objects.requireNonNull(
            connectionFactory,
            "connectionFactory is required"
        );
        this.response = Objects.requireNonNull(
            response,
            "response is required"
        );
    }

    @Override
    public void handle(HttpExchange exchange)
        throws IOException {

        try {
            String method = exchange
                .getRequestMethod()
                .toUpperCase();

            if ("OPTIONS".equals(method)) {
                response.sendOptions(exchange);
                return;
            }

            if (!"GET".equals(method)) {
                response.sendMethodNotAllowed(exchange);
                return;
            }

            boolean databaseAvailable =
                isDatabaseAvailable();

            HealthResponse body = new HealthResponse(
                databaseAvailable ? "UP" : "DEGRADED",
                APPLICATION_NAME,
                databaseAvailable ? "UP" : "DOWN",
                Instant.now()
            );

            response.send(
                exchange,
                databaseAvailable ? 200 : 503,
                body
            );
        } catch (Exception exception) {
            response.send(
                exchange,
                500,
                new HealthResponse(
                    "DOWN",
                    APPLICATION_NAME,
                    "UNKNOWN",
                    Instant.now()
                )
            );
        }
    }

    private boolean isDatabaseAvailable() {
        try (
            Connection connection =
                connectionFactory.openConnection();
            Statement statement =
                connection.createStatement();
            ResultSet resultSet =
                statement.executeQuery("SELECT 1")
        ) {
            return resultSet.next()
                && resultSet.getInt(1) == 1;
        } catch (Exception exception) {
            return false;
        }
    }
}
