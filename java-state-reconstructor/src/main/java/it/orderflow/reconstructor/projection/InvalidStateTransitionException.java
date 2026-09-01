package it.orderflow.reconstructor.projection;

import it.orderflow.reconstructor.domain.EventType;
import it.orderflow.reconstructor.domain.OrderStatus;

public final class InvalidStateTransitionException
    extends StateProjectionException {

    public InvalidStateTransitionException(
        String aggregateId,
        long currentVersion,
        long eventVersion,
        OrderStatus currentStatus,
        EventType eventType
    ) {
        super(
            ProjectionErrorCode.INVALID_STATE_TRANSITION,
            "Event "
                + eventType
                + " cannot be applied to state "
                + currentStatus,
            aggregateId,
            currentVersion,
            eventVersion,
            eventType,
            currentStatus
        );
    }

    public InvalidStateTransitionException(
        String aggregateId,
        long eventVersion,
        EventType eventType
    ) {
        super(
            ProjectionErrorCode.INVALID_STATE_TRANSITION,
            "Event "
                + eventType
                + " cannot create a new order state",
            aggregateId,
            0,
            eventVersion,
            eventType,
            null
        );
    }
}