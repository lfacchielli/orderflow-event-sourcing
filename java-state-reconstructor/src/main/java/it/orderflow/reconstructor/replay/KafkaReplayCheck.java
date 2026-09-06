package it.orderflow.reconstructor.replay;

import it.orderflow.reconstructor.consumer.KafkaConsumerFactory;
import it.orderflow.reconstructor.consumer.KafkaConsumerSettings;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.projection.OrderStateProjector;
import it.orderflow.reconstructor.serialization.OrderEventDeserializer;
import org.apache.kafka.clients.consumer.Consumer;

import java.time.Duration;
import java.util.List;

public final class KafkaReplayCheck {

    private KafkaReplayCheck() {
    }

    public static void main(String[] args) {
        String orderId = args.length > 0
            ? args[0]
            : "ORD-3001";

        String bootstrapServers =
            environment(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092"
            );

        String topic =
            environment(
                "KAFKA_ORDER_EVENTS_TOPIC",
                "order-events"
            );

        KafkaConsumerSettings settings =
            new KafkaConsumerSettings(
                bootstrapServers,
                "order-replay-read-only",
                "order-replay-check",
                topic,
                Duration.ofMillis(500)
            );

        KafkaConsumerFactory factory =
            new KafkaConsumerFactory();

        try (
            Consumer<String, String> consumer =
                factory.create(settings)
        ) {
            KafkaOrderHistoryReader historyReader =
                new KafkaOrderHistoryReader(
                    consumer,
                    new OrderEventDeserializer(),
                    topic,
                    settings.pollTimeout()
                );

            List<OrderEvent> history =
                historyReader.readHistory(orderId);

            if (history.isEmpty()) {
                throw new ReplayException(
                    "No Kafka events found for " + orderId
                );
            }

            ReplayResult result =
                new OrderReplayService(
                    new OrderStateProjector()
                ).replayAll(history);

            OrderState state = result.state();

            System.out.println(
                "Kafka historical replay completed."
            );
            System.out.println(
                "Order:          " + state.orderId()
            );
            System.out.println(
                "Events found:   " + history.size()
            );
            System.out.println(
                "Final status:   " + state.status()
            );
            System.out.println(
                "Final version:  " + state.version()
            );
            System.out.println(
                "Current hub:    " + state.currentHub()
            );
            System.out.println(
                "Visited hubs:   " + state.visitedHubs()
            );
            System.out.println(
                "Total delay:    "
                    + state.totalDelayMinutes()
            );
            System.out.println(
                "Complete replay: "
                    + result.isCompleteReplay()
            );
            System.out.println(
                "Offsets committed: no"
            );
        }
    }

    private static String environment(
        String name,
        String defaultValue
    ) {
        String value = System.getenv(name);

        if (value == null || value.isBlank()) {
            return defaultValue;
        }

        return value.trim();
    }
}