package it.orderflow.reconstructor.projection;

import it.orderflow.reconstructor.domain.EventType;
import it.orderflow.reconstructor.domain.OrderStatus;

public final class InvalidStateTransitionException
    extends RuntimeException {

    public InvalidStateTransitionException(
        OrderStatus currentStatus,
        EventType eventType
    ) {
        super(
            "Event "
                + eventType
                + " cannot be applied to state "
                + currentStatus
        );
    }

    public InvalidStateTransitionException(
        EventType eventType
    ) {
        super(
            "Event "
                + eventType
                + " cannot create a new order state"
        );
    }
}