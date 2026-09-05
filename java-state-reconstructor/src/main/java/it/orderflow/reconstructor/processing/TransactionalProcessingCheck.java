package it.orderflow.reconstructor.processing;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.DatabaseSettings;
import it.orderflow.reconstructor.persistence.JdbcOrderStateRepository;
import it.orderflow.reconstructor.persistence.JdbcProcessedEventRepository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.processing.KafkaRecordMetadata;
import it.orderflow.reconstructor.processing.TransactionalOrderEventProcessor;
import it.orderflow.reconstructor.processing.TransactionalProcessingResult;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;

public final class TransactionalProcessingCheck {

    private TransactionalProcessingCheck() {
    }

    public static void main(String[] args)
        throws Exception {

        if (args.length != 1) {
            throw new IllegalArgumentException(
                "Scenario path is required"
            );
        }

        JsonNode events = new ObjectMapper()
            .readTree(
                Files.readString(Path.of(args[0]))
            )
            .get("events");

        if (events == null || !events.isArray()) {
            throw new IllegalArgumentException(
                "Scenario must contain an events array"
            );
        }

        TransactionalOrderEventProcessor processor =
            new TransactionalOrderEventProcessor(
                new ConnectionFactory(
                    DatabaseSettings.fromEnvironment()
                ),
                new JdbcOrderStateRepository(),
                new JdbcProcessedEventRepository(),
                new OrderStateProjector()
            );

        OrderEventDeserializer deserializer =
            new OrderEventDeserializer();

        int processed = 0;
        int duplicates = 0;
        long offset = 0;

        for (JsonNode eventNode : events) {
            OrderEvent event = deserializer.deserialize(
                eventNode.toString()
            );

            TransactionalProcessingResult result =
                processor.process(
                    event,
                    new KafkaRecordMetadata(
                        "transaction-check",
                        0,
                        offset
                    )
                );

            if (result.alreadyProcessed()) {
                duplicates++;
            } else {
                processed++;
            }

            offset++;
        }

        System.out.println(
            "Transactional processing completed."
        );
        System.out.println("Processed:  " + processed);
        System.out.println("Duplicates: " + duplicates);
    }
}