package it.orderflow.reconstructor.processing;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.store.OrderStateStore;

import java.util.Objects;

public final class OrderEventProcessor {

    private final OrderStateStore stateStore;
    private final OrderStateProjector projector;

    public OrderEventProcessor(
        OrderStateStore stateStore,
        OrderStateProjector projector
    ) {
        this.stateStore = Objects.requireNonNull(
            stateStore,
            "stateStore is required"
        );
        this.projector = Objects.requireNonNull(
            projector,
            "projector is required"
        );
    }

    public ProcessingResult process(OrderEvent event) {
        Objects.requireNonNull(event, "event is required");

        OrderState previousState = stateStore
            .findByOrderId(event.aggregateId())
            .orElse(null);

        OrderState currentState = projector.apply(
            previousState,
            event
        );

        stateStore.save(currentState);

        return new ProcessingResult(
            previousState,
            currentState
        );
    }
}