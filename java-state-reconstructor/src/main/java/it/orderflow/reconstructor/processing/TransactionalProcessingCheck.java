package it.orderflow.reconstructor.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.persistence.ConnectionFactory;
import it.orderflow.reconstructor.persistence.DatabaseSettings;
import it.orderflow.reconstructor.persistence.JdbcOrderStateRepository;
import it.orderflow.reconstructor.persistence.JdbcProcessedEventRepository;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import it.orderflow.reconstructor.snapshot.JdbcOrderSnapshotRepository;
import it.orderflow.reconstructor.snapshot.SnapshotPolicy;
import it.orderflow.reconstructor.snapshot.SnapshotService;

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

        Path scenarioPath = Path.of(args[0]);

        JsonNode events = new ObjectMapper()
            .readTree(Files.readString(scenarioPath))
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
            new OrderStateProjector(),
            new SnapshotService(
                new JdbcOrderSnapshotRepository(),
                new SnapshotPolicy(5)
            )
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