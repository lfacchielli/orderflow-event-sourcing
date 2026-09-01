package it.orderflow.reconstructor.projection;

import com.fasterxml.jackson.databind.JsonNode;
import it.orderflow.reconstructor.domain.Destination;
import it.orderflow.reconstructor.domain.EventType;
import it.orderflow.reconstructor.domain.OrderEvent;
import it.orderflow.reconstructor.domain.OrderItem;
import it.orderflow.reconstructor.domain.OrderState;
import it.orderflow.reconstructor.domain.OrderStatus;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class OrderStateProjector {

    private final OrderEventGuard eventGuard;

    public OrderStateProjector() {
        this(new OrderEventGuard());
    }

    OrderStateProjector(OrderEventGuard eventGuard) {
        this.eventGuard = Objects.requireNonNull(
            eventGuard,
            "eventGuard is required"
        );
    }

    public OrderState apply(
        OrderState currentState,
        OrderEvent event
    ) {
        Objects.requireNonNull(event, "event is required");

        if (currentState == null) {
            return createInitialState(event);
        }

        eventGuard.validate(currentState, event);

        return switch (event.eventType()) {
            case ORDER_CREATED ->
                throw invalidTransition(currentState, event);

            case ORDER_CONFIRMED ->
                applySimpleTransition(
                    currentState,
                    event,
                    OrderStatus.CREATED,
                    OrderStatus.CONFIRMED
                );

            case PAYMENT_COMPLETED ->
                applySimpleTransition(
                    currentState,
                    event,
                    OrderStatus.CONFIRMED,
                    OrderStatus.PAID
                );

            case PAYMENT_FAILED ->
                applySimpleTransition(
                    currentState,
                    event,
                    OrderStatus.CONFIRMED,
                    OrderStatus.CONFIRMED
                );

            case INVENTORY_RESERVED ->
                applySimpleTransition(
                    currentState,
                    event,
                    OrderStatus.PAID,
                    OrderStatus.INVENTORY_RESERVED
                );

            case ORDER_PACKED ->
                applySimpleTransition(
                    currentState,
                    event,
                    OrderStatus.INVENTORY_RESERVED,
                    OrderStatus.PACKED
                );

            case SHIPMENT_STARTED ->
                applyShipmentStarted(currentState, event);

            case HUB_REACHED ->
                applyHubReached(currentState, event);

            case DELIVERY_DELAYED ->
                applyDeliveryDelayed(currentState, event);

            case OUT_FOR_DELIVERY ->
                applyOutForDelivery(currentState, event);

            case ORDER_DELIVERED ->
                applyOrderDelivered(currentState, event);

            case ORDER_CANCELLED ->
                applyOrderCancelled(currentState, event);

            case DELIVERY_FAILED ->
                applyDeliveryFailed(currentState, event);
        };
    }

    private OrderState createInitialState(OrderEvent event) {
        if (event.eventType() != EventType.ORDER_CREATED) {
            throw new InvalidStateTransitionException(
                event.aggregateId(),
                event.aggregateVersion(),
                event.eventType()
            );
        }

        JsonNode payload = event.payload();

        String customerId = requiredText(
            payload,
            "customerId"
        );
        String currency = requiredText(
            payload,
            "currency"
        );

        List<OrderItem> items = readItems(
            requiredNode(payload, "items")
        );

        BigDecimal totalAmount = requiredDecimal(
            payload,
            "totalAmount"
        );

        Destination destination = readDestination(
            requiredNode(payload, "destination")
        );

        return new OrderState(
            event.aggregateId(),
            OrderStatus.CREATED,
            event.aggregateVersion(),
            customerId,
            currency,
            items,
            totalAmount,
            destination,
            null,
            List.of(),
            0,
            false,
            event.occurredAt(),
            null,
            event.occurredAt()
        );
    }

    private OrderState applySimpleTransition(
        OrderState currentState,
        OrderEvent event,
        OrderStatus expectedStatus,
        OrderStatus nextStatus
    ) {
        requireStatus(
            currentState,
            event,
            expectedStatus
        );

        return currentState.advance(
            nextStatus,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private OrderState applyShipmentStarted(
        OrderState currentState,
        OrderEvent event
    ) {
        requireStatus(
            currentState,
            event,
            OrderStatus.PACKED
        );

        String hubId = requiredText(
            event.payload(),
            "hubId"
        );

        return currentState.reachHub(
            hubId,
            OrderStatus.IN_TRANSIT,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private OrderState applyHubReached(
        OrderState currentState,
        OrderEvent event
    ) {
        requireStatus(
            currentState,
            event,
            OrderStatus.IN_TRANSIT
        );

        String hubId = requiredText(
            event.payload(),
            "hubId"
        );

        return currentState.reachHub(
            hubId,
            OrderStatus.IN_TRANSIT,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private OrderState applyDeliveryDelayed(
        OrderState currentState,
        OrderEvent event
    ) {
        if (
            currentState.status() != OrderStatus.IN_TRANSIT
                && currentState.status()
                    != OrderStatus.OUT_FOR_DELIVERY
        ) {
            throw invalidTransition(currentState, event);
        }

        int delayMinutes = requiredInteger(
            event.payload(),
            "delayMinutes"
        );

        return currentState.accumulateDelay(
            delayMinutes,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private OrderState applyOutForDelivery(
        OrderState currentState,
        OrderEvent event
    ) {
        requireStatus(
            currentState,
            event,
            OrderStatus.IN_TRANSIT
        );

        String hubId = requiredText(
            event.payload(),
            "hubId"
        );

        return currentState.reachHub(
            hubId,
            OrderStatus.OUT_FOR_DELIVERY,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private OrderState applyOrderDelivered(
        OrderState currentState,
        OrderEvent event
    ) {
        requireStatus(
            currentState,
            event,
            OrderStatus.OUT_FOR_DELIVERY
        );

        String hubId = requiredText(
            event.payload(),
            "hubId"
        );

        return currentState.completeDelivery(
            hubId,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private OrderState applyOrderCancelled(
        OrderState currentState,
        OrderEvent event
    ) {
        requireStatus(
            currentState,
            event,
            OrderStatus.CONFIRMED
        );

        return currentState.advance(
            OrderStatus.CANCELLED,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private OrderState applyDeliveryFailed(
        OrderState currentState,
        OrderEvent event
    ) {
        requireStatus(
            currentState,
            event,
            OrderStatus.OUT_FOR_DELIVERY
        );

        return currentState.advance(
            OrderStatus.DELIVERY_FAILED,
            event.aggregateVersion(),
            event.occurredAt()
        );
    }

    private void requireStatus(
        OrderState currentState,
        OrderEvent event,
        OrderStatus expectedStatus
    ) {
        if (currentState.status() != expectedStatus) {
            throw invalidTransition(currentState, event);
        }
    }

    private InvalidStateTransitionException invalidTransition(
        OrderState currentState,
        OrderEvent event
    ) {
        return new InvalidStateTransitionException(
            currentState.orderId(),
            currentState.version(),
            event.aggregateVersion(),
            currentState.status(),
            event.eventType()
        );
    }

    private List<OrderItem> readItems(JsonNode itemsNode) {
        if (!itemsNode.isArray() || itemsNode.isEmpty()) {
            throw new IllegalArgumentException(
                "items must be a non-empty array"
            );
        }

        List<OrderItem> items = new ArrayList<>();

        for (JsonNode itemNode : itemsNode) {
            items.add(
                new OrderItem(
                    requiredText(itemNode, "productId"),
                    requiredText(itemNode, "productName"),
                    requiredInteger(itemNode, "quantity"),
                    requiredDecimal(itemNode, "unitPrice")
                )
            );
        }

        return List.copyOf(items);
    }

    private Destination readDestination(
        JsonNode destinationNode
    ) {
        return new Destination(
            requiredText(destinationNode, "city"),
            requiredText(destinationNode, "country")
        );
    }

    private JsonNode requiredNode(
        JsonNode parent,
        String fieldName
    ) {
        JsonNode value = parent.get(fieldName);

        if (value == null || value.isNull()) {
            throw new IllegalArgumentException(
                "Missing required payload field: "
                    + fieldName
            );
        }

        return value;
    }

    private String requiredText(
        JsonNode parent,
        String fieldName
    ) {
        JsonNode value = requiredNode(parent, fieldName);

        if (!value.isTextual() || value.asText().isBlank()) {
            throw new IllegalArgumentException(
                "Payload field must be non-blank text: "
                    + fieldName
            );
        }

        return value.asText();
    }

    private int requiredInteger(
        JsonNode parent,
        String fieldName
    ) {
        JsonNode value = requiredNode(parent, fieldName);

        if (!value.isIntegralNumber()) {
            throw new IllegalArgumentException(
                "Payload field must be an integer: "
                    + fieldName
            );
        }

        return value.asInt();
    }

    private BigDecimal requiredDecimal(
        JsonNode parent,
        String fieldName
    ) {
        JsonNode value = requiredNode(parent, fieldName);

        if (!value.isNumber()) {
            throw new IllegalArgumentException(
                "Payload field must be numeric: "
                    + fieldName
            );
        }

        return value.decimalValue();
    }
}