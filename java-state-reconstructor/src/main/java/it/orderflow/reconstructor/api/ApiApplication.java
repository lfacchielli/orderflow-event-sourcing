package it.orderflow.reconstructor.api;

import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.DatabaseSettings;

import java.util.concurrent.CountDownLatch;

public final class ApiApplication {

    private ApiApplication() {
    }

    public static void main(String[] args)
        throws Exception {

        ApiSettings apiSettings =
            ApiSettings.fromEnvironment();

        DatabaseSettings databaseSettings =
            DatabaseSettings.fromEnvironment();

        ConnectionFactory connectionFactory =
            new ConnectionFactory(databaseSettings);

        ApiServer server = new ApiServer(
            apiSettings,
            connectionFactory
        );

        Runtime.getRuntime().addShutdownHook(
            new Thread(
                server::close,
                "orderflow-api-shutdown"
            )
        );

        server.start();

        System.out.println("OrderFlow API started.");
        System.out.println(
            "Address: http://localhost:"
                + server.port()
        );
        System.out.println(
            "Health:  http://localhost:"
                + server.port()
                + "/api/health"
        );

        new CountDownLatch(1).await();
    }
}
