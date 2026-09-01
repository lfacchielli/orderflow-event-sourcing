package it.orderflow.reconstructor.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public final class KafkaConsumerFactory {

    public Consumer<String, String> create(
        KafkaConsumerSettings settings
    ) {
        Properties properties = new Properties();

        properties.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            settings.bootstrapServers()
        );
        properties.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            settings.groupId()
        );
        properties.put(
            ConsumerConfig.CLIENT_ID_CONFIG,
            settings.clientId()
        );
        properties.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName()
        );
        properties.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName()
        );
        properties.put(
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            false
        );
        properties.put(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest"
        );
        properties.put(
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
            false
        );

        return new KafkaConsumer<>(properties);
    }
}