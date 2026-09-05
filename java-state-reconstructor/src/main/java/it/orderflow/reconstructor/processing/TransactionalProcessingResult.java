package it.orderflow.reconstructor.processing;

import it.orderflow.reconstructor.domain.OrderState;

public record TransactionalProcessingResult(
    boolean alreadyProcessed,
    OrderState state
) {

    public static TransactionalProcessingResult duplicate(
        OrderState state
    ) {
        return new TransactionalProcessingResult(
            true,
            state
        );
    }

    public static TransactionalProcessingResult processed(
        OrderState state
    ) {
        return new TransactionalProcessingResult(
            false,
            state
        );
    }
}