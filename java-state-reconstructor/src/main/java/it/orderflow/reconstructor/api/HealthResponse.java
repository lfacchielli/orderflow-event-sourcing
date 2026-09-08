package it.orderflow.reconstructor.api;

import java.time.Instant;

public record HealthResponse(
    String status,
    String application,
    String database,
    Instant checkedAt
) {
}
