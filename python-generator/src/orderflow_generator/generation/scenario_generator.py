from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from orderflow_generator.models.event import OrderEvent


class ScenarioGenerator:
    def generate_successful_delivery(
        self,
        order_id: str,
        start_time: datetime,
    ) -> list[OrderEvent]:
        start_time = self._require_utc(start_time)
        correlation_id = order_id.replace("ORD-", "CORR-")

        items = [
            {
                "productId": "PRD-100",
                "productName": "Wireless Sensor",
                "quantity": 2,
                "unitPrice": 20.00,
            },
            {
                "productId": "PRD-205",
                "productName": "Edge Gateway Adapter",
                "quantity": 1,
                "unitPrice": 15.00,
            },
        ]

        total_amount = self._calculate_total(items)

        return [
            self._event(
                event_id="7e429750-7f5d-4a55-9ce4-5609b32b9a67",
                event_type="ORDER_CREATED",
                order_id=order_id,
                version=1,
                occurred_at=start_time,
                producer_id="ecommerce-node-01",
                producer_type="ECOMMERCE",
                correlation_id=correlation_id,
                payload={
                    "customerId": "CUS-501",
                    "currency": "EUR",
                    "items": items,
                    "totalAmount": float(total_amount),
                    "destination": {
                        "city": "Firenze",
                        "country": "IT",
                    },
                },
            ),
            self._event(
                event_id="c9e05cd1-039f-4ccb-99af-b89909d48cf0",
                event_type="ORDER_CONFIRMED",
                order_id=order_id,
                version=2,
                occurred_at=start_time + timedelta(minutes=2),
                producer_id="ecommerce-node-01",
                producer_type="ECOMMERCE",
                correlation_id=correlation_id,
            ),
            self._event(
                event_id="70f3383e-a586-40cc-b8ac-43b6757ca015",
                event_type="PAYMENT_COMPLETED",
                order_id=order_id,
                version=3,
                occurred_at=start_time + timedelta(minutes=4),
                producer_id="payment-node-01",
                producer_type="PAYMENT",
                correlation_id=correlation_id,
                payload={
                    "paymentId": "PAY-2001",
                    "amount": float(total_amount),
                    "currency": "EUR",
                    "method": "CARD",
                },
            ),
            self._event(
                event_id="02c39cc1-1116-4327-b1b4-bd612c76866f",
                event_type="INVENTORY_RESERVED",
                order_id=order_id,
                version=4,
                occurred_at=start_time + timedelta(minutes=8),
                producer_id="warehouse-modena-01",
                producer_type="WAREHOUSE",
                correlation_id=correlation_id,
                payload={
                    "warehouseId": "WAREHOUSE-MODENA",
                    "reservationId": "RES-2001",
                },
            ),
            self._event(
                event_id="1daa071e-c730-43ef-b3d8-d79db83e53ce",
                event_type="ORDER_PACKED",
                order_id=order_id,
                version=5,
                occurred_at=start_time + timedelta(minutes=20),
                producer_id="warehouse-modena-01",
                producer_type="WAREHOUSE",
                correlation_id=correlation_id,
                payload={
                    "packageId": "PKG-2001",
                    "weightKg": 2.4,
                },
            ),
            self._event(
                event_id="8706a280-5390-46ce-a72c-71f60996b8df",
                event_type="SHIPMENT_STARTED",
                order_id=order_id,
                version=6,
                occurred_at=start_time + timedelta(hours=1),
                producer_id="warehouse-modena-01",
                producer_type="WAREHOUSE",
                correlation_id=correlation_id,
                payload={
                    "shipmentId": "SHP-2001",
                    "hubId": "HUB-MODENA",
                },
            ),
            self._event(
                event_id="e03656dc-3798-46a2-94d4-986a61974bde",
                event_type="HUB_REACHED",
                order_id=order_id,
                version=7,
                occurred_at=start_time + timedelta(hours=2, minutes=30),
                producer_id="hub-bologna-01",
                producer_type="LOGISTICS_HUB",
                correlation_id=correlation_id,
                payload={
                    "hubId": "HUB-BOLOGNA",
                    "city": "Bologna",
                },
            ),
            self._event(
                event_id="12e3cef2-62c6-4c32-987e-794f044402b4",
                event_type="DELIVERY_DELAYED",
                order_id=order_id,
                version=8,
                occurred_at=start_time + timedelta(hours=3),
                producer_id="hub-bologna-01",
                producer_type="LOGISTICS_HUB",
                correlation_id=correlation_id,
                payload={
                    "hubId": "HUB-BOLOGNA",
                    "delayMinutes": 35,
                    "reason": "TRAFFIC",
                },
            ),
            self._event(
                event_id="3c7958a8-1417-4e54-b00f-378681a4deec",
                event_type="OUT_FOR_DELIVERY",
                order_id=order_id,
                version=9,
                occurred_at=start_time + timedelta(hours=5, minutes=15),
                producer_id="delivery-firenze-01",
                producer_type="DELIVERY",
                correlation_id=correlation_id,
                payload={
                    "hubId": "HUB-FIRENZE",
                    "city": "Firenze",
                    "courierId": "COURIER-17",
                },
            ),
            self._event(
                event_id="60231b5c-c90e-43ae-a47d-d182245be6ec",
                event_type="ORDER_DELIVERED",
                order_id=order_id,
                version=10,
                occurred_at=start_time + timedelta(hours=6),
                producer_id="delivery-firenze-01",
                producer_type="DELIVERY",
                correlation_id=correlation_id,
                payload={
                    "hubId": "HUB-FIRENZE",
                    "city": "Firenze",
                },
            ),
        ]

    @staticmethod
    def _event(
        event_id: str,
        event_type: str,
        order_id: str,
        version: int,
        occurred_at: datetime,
        producer_id: str,
        producer_type: str,
        correlation_id: str,
        payload: dict | None = None,
    ) -> OrderEvent:
        return OrderEvent(
            event_id=UUID(event_id),
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
    def _calculate_total(items: list[dict]) -> Decimal:
        total = Decimal("0.00")

        for item in items:
            quantity = Decimal(str(item["quantity"]))
            unit_price = Decimal(str(item["unitPrice"]))
            total += quantity * unit_price

        return total.quantize(Decimal("0.01"))

    @staticmethod
    def _require_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("The scenario start time must include a timezone.")

        return value.astimezone(timezone.utc)