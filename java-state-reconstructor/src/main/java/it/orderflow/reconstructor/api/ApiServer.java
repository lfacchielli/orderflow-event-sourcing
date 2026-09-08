package it.orderflow.reconstructor.api;

import com.sun.net.httpserver.HttpServer;
import it.orderflow.reconstructor.api.handler.HealthHandler;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.api.handler.OrdersHandler;
import it.orderflow.reconstructor.persistence.JdbcOrderStateRepository;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ApiServer implements AutoCloseable {

    private final HttpServer server;
    private final ExecutorService executor;

    public ApiServer(
        ApiSettings settings,
        ConnectionFactory connectionFactory
    ) throws IOException {
        Objects.requireNonNull(
            settings,
            "settings are required"
        );
        Objects.requireNonNull(
            connectionFactory,
            "connectionFactory is required"
        );

        this.server = HttpServer.create(
            new InetSocketAddress(
                settings.host(),
                settings.port()
            ),
            settings.backlog()
        );

        this.executor = Executors.newFixedThreadPool(
            settings.workerThreads()
        );

        JsonHttpResponse response =
            new JsonHttpResponse();

        server.createContext(
            "/api/health",
            new HealthHandler(
                connectionFactory,
                response
            )
        );

        server.createContext(
            "/api/orders",
            new OrdersHandler(
                connectionFactory,
                new JdbcOrderStateRepository(),
                response
            )
        );

        server.setExecutor(executor);
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdown();
    }
}
