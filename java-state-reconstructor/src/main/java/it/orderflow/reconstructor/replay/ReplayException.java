package it.orderflow.reconstructor.replay;

public final class ReplayException extends RuntimeException {

    public ReplayException(String message) {
        super(message);
    }

    public ReplayException(
        String message,
        Throwable cause
    ) {
        super(message, cause);
    }
}