package it.orderflow.reconstructor.projection;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

public final class OrderEventGuard {

    private static final Set<OrderStatus> TERMINAL_STATUSES =
        EnumSet.of(
            OrderStatus.DELIVERED,
            OrderStatus.CANCELLED,
            OrderStatus.DELIVERY_FAILED
        );

    public void validate(
        OrderState currentState,
        OrderEvent event
    ) {
        Objects.requireNonNull(
            currentState,
            "currentState is required"
        );
        Objects.requireNonNull(
            event,
            "event is required"
        );

        validateAggregate(currentState, event);
        validateVersion(currentState, event);
        validateTerminalState(currentState, event);
    }

    private void validateAggregate(
        OrderState currentState,
        OrderEvent event
    ) {
        if (
            !currentState.orderId().equals(
                event.aggregateId()
            )
        ) {
            throw new StateProjectionException(
                ProjectionErrorCode.AGGREGATE_MISMATCH,
                "Event aggregate "
                    + event.aggregateId()
                    + " does not match state aggregate "
                    + currentState.orderId(),
                event.aggregateId(),
                currentState.version(),
                event.aggregateVersion(),
                event.eventType(),
                currentState.status()
            );
        }
    }

    private void validateVersion(
        OrderState currentState,
        OrderEvent event
    ) {
        long expectedVersion =
            currentState.version() + 1;

        if (
            event.aggregateVersion()
                <= currentState.version()
        ) {
            throw new StateProjectionException(
                ProjectionErrorCode.OLD_OR_DUPLICATE_EVENT,
                "Event version "
                    + event.aggregateVersion()
                    + " is not newer than state version "
                    + currentState.version(),
                event.aggregateId(),
                currentState.version(),
                event.aggregateVersion(),
                event.eventType(),
                currentState.status()
            );
        }

        if (
            event.aggregateVersion()
                > expectedVersion
        ) {
            throw new StateProjectionException(
                ProjectionErrorCode.VERSION_GAP,
                "Expected event version "
                    + expectedVersion
                    + " but received "
                    + event.aggregateVersion(),
                event.aggregateId(),
                currentState.version(),
                event.aggregateVersion(),
                event.eventType(),
                currentState.status()
            );
        }
    }

    private void validateTerminalState(
        OrderState currentState,
        OrderEvent event
    ) {
        if (
            TERMINAL_STATUSES.contains(
                currentState.status()
            )
        ) {
            throw new StateProjectionException(
                ProjectionErrorCode.TERMINAL_STATE,
                "Order "
                    + currentState.orderId()
                    + " is already in terminal state "
                    + currentState.status(),
                event.aggregateId(),
                currentState.version(),
                event.aggregateVersion(),
                event.eventType(),
                currentState.status()
            );
        }
    }
}