package it.orderflow.reconstructor.persistence;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

public interface ProcessedEventRepository {

    boolean exists(
        Connection connection,
        UUID eventId
    ) throws SQLException;

    void save(
        Connection connection,
        ProcessedEvent event
    ) throws SQLException;
}