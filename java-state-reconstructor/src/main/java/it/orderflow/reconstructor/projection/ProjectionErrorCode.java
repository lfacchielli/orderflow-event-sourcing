package it.orderflow.reconstructor.projection;

public enum ProjectionErrorCode {
    AGGREGATE_MISMATCH,
    OLD_OR_DUPLICATE_EVENT,
    VERSION_GAP,
    TERMINAL_STATE,
    INVALID_STATE_TRANSITION
}