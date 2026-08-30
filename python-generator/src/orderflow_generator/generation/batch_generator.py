import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid5

from orderflow_generator.models.event import OrderEvent


EVENT_NAMESPACE = UUID("457361f4-5a6c-4b3f-9c18-44a03e19bb73")


class BatchGenerator:
    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.random = random.Random(seed)

    def generate_orders(
        self,
        order_count: int,
        start_time: datetime,
        delay_probability: float = 0.20,
        cancellation_probability: float = 0.05,
        delivery_failure_probability: float = 0.05,
    ) -> list[OrderEvent]:
        self._validate_parameters(
            order_count=order_count,
            start_time=start_time,
            delay_probability=delay_probability,
            cancellation_probability=cancellation_probability,
            delivery_failure_probability=delivery_failure_probability,
        )

        normalized_start_time = start_time.astimezone(timezone.utc)
        events: list[OrderEvent] = []

        for index in range(1, order_count + 1):
            order_id = f"ORD-{3000 + index:04d}"
            order_start_time = normalized_start_time + timedelta(
                seconds=index - 1
            )

            order_events = self._generate_order(
                order_id=order_id,
                start_time=order_start_time,
                delay_probability=delay_probability,
                cancellation_probability=cancellation_probability,
                delivery_failure_probability=delivery_failure_probability,
            )

            events.extend(order_events)

        return events

    def _generate_order(
        self,
        order_id: str,
        start_time: datetime,
        delay_probability: float,
        cancellation_probability: float,
        delivery_failure_probability: float,
    ) -> list[OrderEvent]:
        customer_id = f"CUS-{self.random.randint(100, 999)}"
        correlation_id = order_id.replace("ORD-", "CORR-")

        unit_price = Decimal(
            str(self.random.choice([12.50, 20.00, 27.50, 35.00]))
        )
        quantity = self.random.randint(1, 4)
        total_amount = (
            unit_price * Decimal(quantity)
        ).quantize(Decimal("0.01"))

        events = [
            self._event(
                order_id=order_id,
                version=1,
                event_type="ORDER_CREATED",
                occurred_at=start_time,
                producer_id="ecommerce-node-01",
                producer_type="ECOMMERCE",
                correlation_id=correlation_id,
                payload={
                    "customerId": customer_id,
                    "currency": "EUR",
                    "items": [
                        {
                            "productId": (
                                f"PRD-{self.random.randint(100, 999)}"
                            ),
                            "productName": "Synthetic Edge Product",
                            "quantity": quantity,
                            "unitPrice": float(unit_price),
                        }
                    ],
                    "totalAmount": float(total_amount),
                    "destination": {
                        "city": "Firenze",
                        "country": "IT",
                    },
                },
            ),
            self._event(
                order_id=order_id,
                version=2,
                event_type="ORDER_CONFIRMED",
                occurred_at=start_time + timedelta(minutes=2),
                producer_id="ecommerce-node-01",
                producer_type="ECOMMERCE",
                correlation_id=correlation_id,
            ),
        ]

        if self.random.random() < cancellation_probability:
            events.append(
                self._event(
                    order_id=order_id,
                    version=3,
                    event_type="ORDER_CANCELLED",
                    occurred_at=start_time + timedelta(minutes=5),
                    producer_id="ecommerce-node-01",
                    producer_type="ECOMMERCE",
                    correlation_id=correlation_id,
                    payload={
                        "reason": "CUSTOMER_REQUEST",
                    },
                )
            )
            return events

        events.extend(
            [
                self._event(
                    order_id=order_id,
                    version=3,
                    event_type="PAYMENT_COMPLETED",
                    occurred_at=start_time + timedelta(minutes=4),
                    producer_id="payment-node-01",
                    producer_type="PAYMENT",
                    correlation_id=correlation_id,
                    payload={
                        "paymentId": order_id.replace("ORD-", "PAY-"),
                        "amount": float(total_amount),
                        "currency": "EUR",
                        "method": "CARD",
                    },
                ),
                self._event(
                    order_id=order_id,
                    version=4,
                    event_type="INVENTORY_RESERVED",
                    occurred_at=start_time + timedelta(minutes=8),
                    producer_id="warehouse-modena-01",
                    producer_type="WAREHOUSE",
                    correlation_id=correlation_id,
                    payload={
                        "warehouseId": "WAREHOUSE-MODENA",
                        "reservationId": order_id.replace(
                            "ORD-",
                            "RES-",
                        ),
                    },
                ),
                self._event(
                    order_id=order_id,
                    version=5,
                    event_type="ORDER_PACKED",
                    occurred_at=start_time + timedelta(minutes=20),
                    producer_id="warehouse-modena-01",
                    producer_type="WAREHOUSE",
                    correlation_id=correlation_id,
                    payload={
                        "packageId": order_id.replace("ORD-", "PKG-"),
                        "weightKg": round(
                            self.random.uniform(0.5, 8.0),
                            2,
                        ),
                    },
                ),
                self._event(
                    order_id=order_id,
                    version=6,
                    event_type="SHIPMENT_STARTED",
                    occurred_at=start_time + timedelta(hours=1),
                    producer_id="warehouse-modena-01",
                    producer_type="WAREHOUSE",
                    correlation_id=correlation_id,
                    payload={
                        "shipmentId": order_id.replace(
                            "ORD-",
                            "SHP-",
                        ),
                        "hubId": "HUB-MODENA",
                    },
                ),
                self._event(
                    order_id=order_id,
                    version=7,
                    event_type="HUB_REACHED",
                    occurred_at=start_time
                    + timedelta(hours=2, minutes=30),
                    producer_id="hub-bologna-01",
                    producer_type="LOGISTICS_HUB",
                    correlation_id=correlation_id,
                    payload={
                        "hubId": "HUB-BOLOGNA",
                        "city": "Bologna",
                    },
                ),
            ]
        )

        next_version = 8
        delivery_time = start_time + timedelta(hours=5, minutes=15)

        if self.random.random() < delay_probability:
            delay_minutes = self.random.choice([10, 20, 35, 60])

            events.append(
                self._event(
                    order_id=order_id,
                    version=next_version,
                    event_type="DELIVERY_DELAYED",
                    occurred_at=start_time + timedelta(hours=3),
                    producer_id="hub-bologna-01",
                    producer_type="LOGISTICS_HUB",
                    correlation_id=correlation_id,
                    payload={
                        "hubId": "HUB-BOLOGNA",
                        "delayMinutes": delay_minutes,
                        "reason": self.random.choice(
                            [
                                "TRAFFIC",
                                "WEATHER",
                                "OPERATIONAL_DELAY",
                            ]
                        ),
                    },
                )
            )

            next_version += 1
            delivery_time += timedelta(minutes=delay_minutes)

        events.append(
            self._event(
                order_id=order_id,
                version=next_version,
                event_type="OUT_FOR_DELIVERY",
                occurred_at=delivery_time,
                producer_id="delivery-firenze-01",
                producer_type="DELIVERY",
                correlation_id=correlation_id,
                payload={
                    "hubId": "HUB-FIRENZE",
                    "city": "Firenze",
                    "courierId": (
                        f"COURIER-{self.random.randint(1, 25):02d}"
                    ),
                },
            )
        )

        next_version += 1

        if self.random.random() < delivery_failure_probability:
            final_event_type = "DELIVERY_FAILED"
            final_payload = {
                "reason": "RECIPIENT_UNAVAILABLE",
                "attemptNumber": 1,
            }
        else:
            final_event_type = "ORDER_DELIVERED"
            final_payload = {
                "hubId": "HUB-FIRENZE",
                "city": "Firenze",
            }

        events.append(
            self._event(
                order_id=order_id,
                version=next_version,
                event_type=final_event_type,
                occurred_at=delivery_time + timedelta(minutes=45),
                producer_id="delivery-firenze-01",
                producer_type="DELIVERY",
                correlation_id=correlation_id,
                payload=final_payload,
            )
        )

        return events

    def _event(
        self,
        order_id: str,
        version: int,
        event_type: str,
        occurred_at: datetime,
        producer_id: str,
        producer_type: str,
        correlation_id: str,
        payload: dict | None = None,
    ) -> OrderEvent:
        event_id = uuid5(
            EVENT_NAMESPACE,
            (
                f"seed={self.seed};"
                f"order={order_id};"
                f"version={version};"
                f"type={event_type}"
            ),
        )

        return OrderEvent(
            event_id=event_id,
            event_type=event_type,
            aggregate_id=order_id,
            aggregate_version=version,
            occurred_at=occurred_at,
            producer_id=producer_id,
            producer_type=producer_type,
            correlation_id=correlation_id,
            payload=payload or {},
        )

    @staticmethod
    def _validate_parameters(
        order_count: int,
        start_time: datetime,
        delay_probability: float,
        cancellation_probability: float,
        delivery_failure_probability: float,
    ) -> None:
        if order_count < 1:
            raise ValueError("Order count must be greater than zero.")

        if start_time.tzinfo is None:
            raise ValueError("Start time must include a timezone.")

        probabilities = {
            "delay_probability": delay_probability,
            "cancellation_probability": cancellation_probability,
            "delivery_failure_probability": (
                delivery_failure_probability
            ),
        }

        for name, value in probabilities.items():
            if value < 0 or value > 1:
                raise ValueError(
                    f"{name} must be between 0 and 1."
                )