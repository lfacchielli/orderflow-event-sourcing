package it.orderflow.reconstructor.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.orderflow.reconstructor.domain.OrderEvent;

import java.util.Objects;

public final class OrderEventDeserializer {

    private final ObjectMapper objectMapper;

    public OrderEventDeserializer() {
        this(
            JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(
                    DeserializationFeature
                        .ADJUST_DATES_TO_CONTEXT_TIME_ZONE
                )
                .build()
        );
    }

    OrderEventDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(
            objectMapper,
            "objectMapper is required"
        );
    }

    public OrderEvent deserialize(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException(
                "Event JSON cannot be null or blank"
            );
        }

        try {
            return objectMapper.readValue(
                json,
                OrderEvent.class
            );
        } catch (JsonProcessingException exception) {
            throw new EventDeserializationException(
                "Unable to deserialize order event",
                exception
            );
        }
    }
}