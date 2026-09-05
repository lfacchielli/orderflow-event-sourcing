package it.orderflow.reconstructor.processing;

import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.OrderStateRepository;
import it.orderflow.reconstructor.persistence.ProcessedEvent;
import it.orderflow.reconstructor.persistence.ProcessedEventRepository;
import it.orderflow.reconstructor.projection.OrderStateProjector;

import java.sql.Connection;
import java.util.Objects;

public final class TransactionalOrderEventProcessor {

    private final ConnectionFactory connectionFactory;
    private final OrderStateRepository stateRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final OrderStateProjector projector;

    public TransactionalOrderEventProcessor(
        ConnectionFactory connectionFactory,
        OrderStateRepository stateRepository,
        ProcessedEventRepository processedEventRepository,
        OrderStateProjector projector
    ) {
        this.connectionFactory = Objects.requireNonNull(
            connectionFactory,
            "connectionFactory is required"
        );
        this.stateRepository = Objects.requireNonNull(
            stateRepository,
            "stateRepository is required"
        );
        this.processedEventRepository = Objects.requireNonNull(
            processedEventRepository,
            "processedEventRepository is required"
        );
        this.projector = Objects.requireNonNull(
            projector,
            "projector is required"
        );
    }

    public TransactionalProcessingResult process(
        OrderEvent event,
        KafkaRecordMetadata metadata
    ) throws Exception {
        Objects.requireNonNull(event, "event is required");
        Objects.requireNonNull(
            metadata,
            "metadata is required"
        );

        try (
            Connection connection =
                connectionFactory.openConnection()
        ) {
            connection.setAutoCommit(false);

            try {
                OrderState previousState = stateRepository
                    .findById(
                        connection,
                        event.aggregateId()
                    )
                    .orElse(null);

                if (
                    processedEventRepository.exists(
                        connection,
                        event.eventId()
                    )
                ) {
                    connection.rollback();

                    return TransactionalProcessingResult
                        .duplicate(previousState);
                }

                OrderState currentState = projector.apply(
                    previousState,
                    event
                );

                stateRepository.save(
                    connection,
                    currentState
                );

                processedEventRepository.save(
                    connection,
                    new ProcessedEvent(
                        event.eventId(),
                        event.aggregateId(),
                        event.aggregateVersion(),
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                    )
                );

                connection.commit();

                return TransactionalProcessingResult
                    .processed(currentState);
            } catch (Exception exception) {
                connection.rollback();
                throw exception;
            }
        }
    }
}