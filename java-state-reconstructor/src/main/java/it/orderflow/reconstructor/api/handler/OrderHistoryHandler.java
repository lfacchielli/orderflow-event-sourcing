package it.orderflow.reconstructor.api.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import it.orderflow.reconstructor.api.HistoricalStateResponse;
import it.orderflow.reconstructor.api.JsonHttpResponse;
import it.orderflow.reconstructor.api.OrderHistoryApiPath;
import it.orderflow.reconstructor.api.QueryParameters;
import it.orderflow.reconstructor.api.SnapshotSummaryResponse;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.snapshot.OrderSnapshot;
import it.orderflow.reconstructor.snapshot.OrderSnapshotRepository;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class OrderHistoryHandler
    implements HttpHandler {

    private final ConnectionFactory connectionFactory;
    private final OrderSnapshotRepository repository;
    private final JsonHttpResponse response;

    public OrderHistoryHandler(
        ConnectionFactory connectionFactory,
        OrderSnapshotRepository repository,
        JsonHttpResponse response
    ) {
        this.connectionFactory = Objects.requireNonNull(
            connectionFactory,
            "connectionFactory is required"
        );
        this.repository = Objects.requireNonNull(
            repository,
            "repository is required"
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

            String path = exchange
                .getRequestURI()
                .getPath();

            String snapshotOrderId =
                OrderHistoryApiPath.snapshotOrderId(path);

            if (snapshotOrderId != null) {
                sendSnapshots(
                    exchange,
                    snapshotOrderId
                );
                return;
            }

            String stateOrderId =
                OrderHistoryApiPath.stateOrderId(path);

            if (stateOrderId != null) {
                sendHistoricalState(
                    exchange,
                    stateOrderId
                );
                return;
            }

            sendNotFound(
                exchange,
                "API resource not found"
            );
        } catch (IllegalArgumentException exception) {
            response.send(
                exchange,
                400,
                Map.of(
                    "status",
                    "BAD_REQUEST",
                    "message",
                    exception.getMessage()
                )
            );
        } catch (Exception exception) {
            response.send(
                exchange,
                500,
                Map.of(
                    "status",
                    "ERROR",
                    "message",
                    "Unable to read order history"
                )
            );
        }
    }

    private void sendSnapshots(
        HttpExchange exchange,
        String orderId
    ) throws Exception {
        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            List<SnapshotSummaryResponse> snapshots =
                repository.findAll(
                        connection,
                        orderId
                    )
                    .stream()
                    .map(SnapshotSummaryResponse::from)
                    .toList();

            response.send(
                exchange,
                200,
                Map.of(
                    "orderId",
                    orderId,
                    "count",
                    snapshots.size(),
                    "snapshots",
                    snapshots
                )
            );
        }
    }

    private void sendHistoricalState(
        HttpExchange exchange,
        String orderId
    ) throws Exception {
        Map<String, String> parameters =
            QueryParameters.parse(
                exchange
                    .getRequestURI()
                    .getRawQuery()
            );

        String versionValue =
            parameters.get("version");

        if (
            versionValue == null
                || versionValue.isBlank()
        ) {
            throw new IllegalArgumentException(
                "Query parameter version is required"
            );
        }

        long version;

        try {
            version = Long.parseLong(versionValue);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                "Query parameter version must be an integer"
            );
        }

        if (version < 1) {
            throw new IllegalArgumentException(
                "Query parameter version must be positive"
            );
        }

        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            Optional<OrderSnapshot> snapshot =
                repository.findByVersion(
                    connection,
                    orderId,
                    version
                );

            if (snapshot.isEmpty()) {
                response.send(
                    exchange,
                    404,
                    Map.of(
                        "status",
                        "NOT_FOUND",
                        "message",
                        "Snapshot version not found",
                        "orderId",
                        orderId,
                        "version",
                        version
                    )
                );
                return;
            }

            response.send(
                exchange,
                200,
                new HistoricalStateResponse(
                    orderId,
                    version,
                    "SNAPSHOT",
                    snapshot.get().state()
                )
            );
        }
    }

    private void sendNotFound(
        HttpExchange exchange,
        String message
    ) throws IOException {
        response.send(
            exchange,
            404,
            Map.of(
                "status",
                "NOT_FOUND",
                "message",
                message
            )
        );
    }
}