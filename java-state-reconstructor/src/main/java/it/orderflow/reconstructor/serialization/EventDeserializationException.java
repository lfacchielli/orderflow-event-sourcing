package it.orderflow.reconstructor.serialization;

public final class EventDeserializationException
    extends RuntimeException {

    public EventDeserializationException(
        String message,
        Throwable cause
    ) {
        super(message, cause);
    }
}