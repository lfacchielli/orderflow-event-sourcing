package it.orderflow.reconstructor.domain;

import java.math.BigDecimal;
import java.util.Objects;

public record OrderItem(
    String productId,
    String productName,
    int quantity,
    BigDecimal unitPrice
) {

    public OrderItem {
        Objects.requireNonNull(productId, "productId is required");
        Objects.requireNonNull(productName, "productName is required");
        Objects.requireNonNull(unitPrice, "unitPrice is required");

        if (productId.isBlank()) {
            throw new IllegalArgumentException(
                "productId cannot be blank"
            );
        }

        if (quantity < 1) {
            throw new IllegalArgumentException(
                "quantity must be greater than zero"
            );
        }

        if (unitPrice.signum() < 0) {
            throw new IllegalArgumentException(
                "unitPrice cannot be negative"
            );
        }
    }

    public BigDecimal lineTotal() {
        return unitPrice.multiply(
            BigDecimal.valueOf(quantity)
        );
    }
}