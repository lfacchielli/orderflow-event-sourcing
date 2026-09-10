package it.orderflow.reconstructor.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;

import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

public final class JsonHttpResponse {

    private final ObjectMapper objectMapper;

    public JsonHttpResponse() {
        this.objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
            )
            .build();
    }

    public void send(
        HttpExchange exchange,
        int statusCode,
        Object body
    ) throws IOException {
        Objects.requireNonNull(
            exchange,
            "exchange is required"
        );
        Objects.requireNonNull(body, "body is required");

        byte[] response = serialize(body);

        exchange.getResponseHeaders().set(
            "Content-Type",
            "application/json; charset=UTF-8"
        );
        exchange.getResponseHeaders().set(
            "Cache-Control",
            "no-store"
        );
        exchange.getResponseHeaders().set(
            "Access-Control-Allow-Origin",
            "*"
        );
        exchange.getResponseHeaders().set(
            "Access-Control-Allow-Methods",
            "GET, OPTIONS"
        );
        exchange.getResponseHeaders().set(
            "Access-Control-Allow-Headers",
            "Content-Type"
        );

        exchange.sendResponseHeaders(
            statusCode,
            response.length
        );

        try (var output = exchange.getResponseBody()) {
            output.write(response);
        }
    }

    public void sendMethodNotAllowed(
        HttpExchange exchange
    ) throws IOException {
        exchange.getResponseHeaders().set(
            "Allow",
            "GET, OPTIONS"
        );

        send(
            exchange,
            405,
            Map.of(
                "status",
                "ERROR",
                "message",
                "Method not allowed"
            )
        );
    }

    public void sendOptions(
        HttpExchange exchange
    ) throws IOException {
        exchange.getResponseHeaders().set(
            "Access-Control-Allow-Origin",
            "*"
        );
        exchange.getResponseHeaders().set(
            "Access-Control-Allow-Methods",
            "GET, OPTIONS"
        );
        exchange.getResponseHeaders().set(
            "Access-Control-Allow-Headers",
            "Content-Type"
        );

        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    private byte[] serialize(Object body)
        throws IOException {
        try {
            return objectMapper
                .writeValueAsString(body)
                .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException exception) {
            throw new IOException(
                "Unable to serialize HTTP response",
                exception
            );
        }
    }
}
