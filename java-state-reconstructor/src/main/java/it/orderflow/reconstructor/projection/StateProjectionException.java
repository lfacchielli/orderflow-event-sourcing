package it.orderflow.reconstructor.projection;

import it.orderflow.reconstructor.domain.EventType;
import it.orderflow.reconstructor.domain.OrderStatus;

public class StateProjectionException
    extends RuntimeException {

    private final ProjectionErrorCode errorCode;
    private final String aggregateId;
    private final long currentVersion;
    private final long eventVersion;
    private final EventType eventType;
    private final OrderStatus currentStatus;

    public StateProjectionException(
        ProjectionErrorCode errorCode,
        String message,
        String aggregateId,
        long currentVersion,
        long eventVersion,
        EventType eventType,
        OrderStatus currentStatus
    ) {
        super(message);

        this.errorCode = errorCode;
        this.aggregateId = aggregateId;
        this.currentVersion = currentVersion;
        this.eventVersion = eventVersion;
        this.eventType = eventType;
        this.currentStatus = currentStatus;
    }

    public ProjectionErrorCode errorCode() {
        return errorCode;
    }

    public String aggregateId() {
        return aggregateId;
    }

    public long currentVersion() {
        return currentVersion;
    }

    public long eventVersion() {
        return eventVersion;
    }

    public EventType eventType() {
        return eventType;
    }

    public OrderStatus currentStatus() {
        return currentStatus;
    }
}