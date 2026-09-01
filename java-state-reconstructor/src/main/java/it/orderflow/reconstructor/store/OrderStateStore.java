package it.orderflow.reconstructor.store;

import it.orderflow.reconstructor.domain.OrderState;

import java.util.Optional;

public interface OrderStateStore {

    Optional<OrderState> findByOrderId(String orderId);

    void save(OrderState state);
}