package it.orderflow.reconstructor.persistence;

import it.orderflow.reconstructor.domain.OrderState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

public interface OrderStateRepository {

    Optional<OrderState> findById(
        Connection connection,
        String orderId
    ) throws SQLException;

    void save(
        Connection connection,
        OrderState state
    ) throws SQLException;
}