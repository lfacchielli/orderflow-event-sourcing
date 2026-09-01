package it.orderflow.reconstructor.persistence;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public final class DatabaseConnectionCheck {

    private DatabaseConnectionCheck() {
    }

    public static void main(String[] args) throws Exception {
        DatabaseSettings settings =
            DatabaseSettings.fromEnvironment();

        ConnectionFactory connectionFactory =
            new ConnectionFactory(settings);

        try (
            Connection connection =
                connectionFactory.openConnection();
            Statement statement =
                connection.createStatement();
            ResultSet resultSet =
                statement.executeQuery(
                    """
                    SELECT
                        current_database(),
                        current_user,
                        COUNT(*)
                    FROM orderflow.order_states
                    GROUP BY current_database(), current_user
                    """
                )
        ) {
            if (!resultSet.next()) {
                throw new IllegalStateException(
                    "Database check returned no rows"
                );
            }

            System.out.println(
                "OrderFlow PostgreSQL connection successful."
            );
            System.out.println(
                "Database:     " + resultSet.getString(1)
            );
            System.out.println(
                "User:         " + resultSet.getString(2)
            );
            System.out.println(
                "Order states: " + resultSet.getLong(3)
            );
        }
    }
}