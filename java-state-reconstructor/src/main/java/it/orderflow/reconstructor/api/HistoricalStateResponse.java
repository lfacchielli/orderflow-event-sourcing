package it.orderflow.reconstructor.api;

import it.orderflow.reconstructor.domain.OrderState;

public record HistoricalStateResponse(
    String orderId,
    long requestedVersion,
    String source,
    OrderState state
) {
}