package it.orderflow.reconstructor.domain;

import java.util.Objects;

public record Destination(
    String city,
    String country
) {

    public Destination {
        Objects.requireNonNull(city, "city is required");
        Objects.requireNonNull(country, "country is required");

        if (city.isBlank()) {
            throw new IllegalArgumentException(
                "city cannot be blank"
            );
        }

        if (country.isBlank()) {
            throw new IllegalArgumentException(
                "country cannot be blank"
            );
        }
    }
}