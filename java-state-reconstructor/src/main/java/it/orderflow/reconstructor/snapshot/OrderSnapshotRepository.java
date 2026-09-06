package it.orderflow.reconstructor.snapshot;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public interface OrderSnapshotRepository {

    Optional<OrderSnapshot> findLatest(
        Connection connection,
        String orderId
    ) throws SQLException;

    void save(
        Connection connection,
        OrderSnapshot snapshot
    ) throws SQLException;
}