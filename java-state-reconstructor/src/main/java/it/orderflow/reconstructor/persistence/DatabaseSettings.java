package it.orderflow.reconstructor.persistence;

import java.util.Objects;

public record DatabaseSettings(
    String jdbcUrl,
    String username,
    String password
) {

    public DatabaseSettings {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl is required");
        Objects.requireNonNull(username, "username is required");
        Objects.requireNonNull(password, "password is required");

        if (jdbcUrl.isBlank()) {
            throw new IllegalArgumentException(
                "jdbcUrl cannot be blank"
            );
        }

        if (username.isBlank()) {
            throw new IllegalArgumentException(
                "username cannot be blank"
            );
        }
    }

    public static DatabaseSettings fromEnvironment() {
        return new DatabaseSettings(
            environment(
                "POSTGRES_JDBC_URL",
                "jdbc:postgresql://localhost:5432/orderflow"
            ),
            environment("POSTGRES_USER", "orderflow"),
            environment(
                "POSTGRES_PASSWORD",
                "orderflow_dev_password"
            )
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

        return value;
    }
}