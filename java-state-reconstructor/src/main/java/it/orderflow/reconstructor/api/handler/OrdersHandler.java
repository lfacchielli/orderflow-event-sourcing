package it.orderflow.reconstructor.api.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import it.orderflow.reconstructor.api.JsonHttpResponse;
import it.orderflow.reconstructor.api.OrderApiPath;
import it.orderflow.reconstructor.api.OrderSummaryResponse;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.OrderStateRepository;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class OrdersHandler implements HttpHandler {

    private final ConnectionFactory connectionFactory;
    private final OrderStateRepository repository;
    private final JsonHttpResponse response;

    public OrdersHandler(
        ConnectionFactory connectionFactory,
        OrderStateRepository repository,
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

            if (OrderApiPath.isCollection(path)) {
                sendOrderList(exchange);
                return;
            }

            String orderId =
                OrderApiPath.extractOrderId(path);

            if (orderId == null) {
                response.send(
                    exchange,
                    404,
                    Map.of(
                        "status",
                        "NOT_FOUND",
                        "message",
                        "API resource not found"
                    )
                );
                return;
            }

            sendOrderDetail(exchange, orderId);
        } catch (Exception exception) {
            response.send(
                exchange,
                500,
                Map.of(
                    "status",
                    "ERROR",
                    "message",
                    "Unable to read order projections"
                )
            );
        }
    }

    private void sendOrderList(
        HttpExchange exchange
    ) throws Exception {
        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            List<OrderSummaryResponse> orders =
                repository.findAll(connection)
                    .stream()
                    .map(OrderSummaryResponse::from)
                    .toList();

            response.send(
                exchange,
                200,
                Map.of(
                    "count",
                    orders.size(),
                    "orders",
                    orders
                )
            );
        }
    }

    private void sendOrderDetail(
        HttpExchange exchange,
        String orderId
    ) throws Exception {
        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            Optional<OrderState> state =
                repository.findById(
                    connection,
                    orderId
                );

            if (state.isEmpty()) {
                response.send(
                    exchange,
                    404,
                    Map.of(
                        "status",
                        "NOT_FOUND",
                        "message",
                        "Order not found",
                        "orderId",
                        orderId
                    )
                );
                return;
            }

            response.send(
                exchange,
                200,
                state.get()
            );
        }
    }
}