package it.orderflow.reconstructor.processing;

import it.orderflow.reconstructor.domain.OrderState;

public record ProcessingResult(
    OrderState previousState,
    OrderState currentState
) {
}