package it.orderflow.reconstructor.store;

import it.orderflow.reconstructor.domain.OrderState;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryOrderStateStore
    implements OrderStateStore {

    private final Map<String, OrderState> states =
        new ConcurrentHashMap<>();

    @Override
    public Optional<OrderState> findByOrderId(
        String orderId
    ) {
        return Optional.ofNullable(
            states.get(orderId)
        );
    }

    @Override
    public void save(OrderState state) {
        states.put(state.orderId(), state);
    }

    public int size() {
        return states.size();
    }

    public void clear() {
        states.clear();
    }
}