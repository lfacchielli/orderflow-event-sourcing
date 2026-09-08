package it.orderflow.reconstructor.api;

import java.util.Objects;

public record ApiSettings(
    String host,
    int port,
    int backlog,
    int workerThreads
) {

    public ApiSettings {
        Objects.requireNonNull(host, "host is required");

        if (host.isBlank()) {
            throw new IllegalArgumentException(
                "host cannot be blank"
            );
        }

        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException(
                "port must be between 1 and 65535"
            );
        }

        if (backlog < 0) {
            throw new IllegalArgumentException(
                "backlog cannot be negative"
            );
        }

        if (workerThreads < 1) {
            throw new IllegalArgumentException(
                "workerThreads must be greater than zero"
            );
        }
    }

    public static ApiSettings fromEnvironment() {
        return new ApiSettings(
            environment("API_HOST", "0.0.0.0"),
            integerEnvironment("API_PORT", 8081),
            integerEnvironment("API_BACKLOG", 50),
            integerEnvironment("API_WORKER_THREADS", 4)
        );
    }

    private static String environment(
        String name,
        String defaultValue
    ) {
        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static int integerEnvironment(
        String name,
        int defaultValue
    ) {
        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                name + " must be an integer",
                exception
            );
        }
    }
}
