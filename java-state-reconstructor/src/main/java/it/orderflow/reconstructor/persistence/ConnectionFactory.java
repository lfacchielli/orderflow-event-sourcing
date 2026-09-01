package it.orderflow.reconstructor.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

public final class ConnectionFactory {

    private final DatabaseSettings settings;

    public ConnectionFactory(DatabaseSettings settings) {
        this.settings = Objects.requireNonNull(
            settings,
            "settings are required"
        );
    }

    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(
            settings.jdbcUrl(),
            settings.username(),
            settings.password()
        );
    }
}