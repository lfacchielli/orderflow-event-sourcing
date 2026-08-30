import unittest
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from decimal import Decimal

from orderflow_generator.generation.scenario_generator import (
    ScenarioGenerator,
)


class ScenarioGeneratorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.generator = ScenarioGenerator()
        self.start_time = datetime(
            2026,
            8,
            29,
            10,
            0,
            0,
            tzinfo=timezone.utc,
        )
        self.events = self.generator.generate_successful_delivery(
            order_id="ORD-2001",
            start_time=self.start_time,
        )

    def test_generates_ten_events(self) -> None:
        self.assertEqual(10, len(self.events))

    def test_all_events_belong_to_the_same_order(self) -> None:
        aggregate_ids = {
            event.aggregate_id
            for event in self.events
        }

        self.assertEqual({"ORD-2001"}, aggregate_ids)

    def test_versions_are_progressive(self) -> None:
        actual_versions = [
            event.aggregate_version
            for event in self.events
        ]

        self.assertEqual(
            list(range(1, 11)),
            actual_versions,
        )

    def test_event_lifecycle_boundaries(self) -> None:
        self.assertEqual(
            "ORDER_CREATED",
            self.events[0].event_type,
        )
        self.assertEqual(
            "ORDER_DELIVERED",
            self.events[-1].event_type,
        )

    def test_timestamps_are_chronologically_ordered(self) -> None:
        timestamps = [
            event.occurred_at
            for event in self.events
        ]

        self.assertEqual(
            sorted(timestamps),
            timestamps,
        )

    def test_event_identifiers_are_unique(self) -> None:
        event_ids = {
            event.event_id
            for event in self.events
        }

        self.assertEqual(
            len(self.events),
            len(event_ids),
        )

    def test_order_total_is_calculated_correctly(self) -> None:
        created_event = self.events[0]
        items = created_event.payload["items"]

        calculated_total = sum(
            Decimal(str(item["quantity"]))
            * Decimal(str(item["unitPrice"]))
            for item in items
        )

        self.assertEqual(
            Decimal("55.00"),
            calculated_total,
        )
        self.assertEqual(
            55.00,
            created_event.payload["totalAmount"],
        )

    def test_total_delay_is_accumulated_correctly(self) -> None:
        total_delay = sum(
            event.payload["delayMinutes"]
            for event in self.events
            if event.event_type == "DELIVERY_DELAYED"
        )

        self.assertEqual(35, total_delay)

    def test_uses_expected_distributed_producers(self) -> None:
        producer_ids = {
            event.producer_id
            for event in self.events
        }

        self.assertEqual(
            {
                "ecommerce-node-01",
                "payment-node-01",
                "warehouse-modena-01",
                "hub-bologna-01",
                "delivery-firenze-01",
            },
            producer_ids,
        )

    def test_all_events_share_the_same_correlation_id(self) -> None:
        correlation_ids = {
            event.correlation_id
            for event in self.events
        }

        self.assertEqual(
            {"CORR-2001"},
            correlation_ids,
        )

    def test_correlation_id_is_derived_from_order_id(self) -> None:
        events = self.generator.generate_successful_delivery(
            order_id="ORD-9876",
            start_time=self.start_time,
        )

        correlation_ids = {
            event.correlation_id
            for event in events
        }

        self.assertEqual(
            {"CORR-9876"},
            correlation_ids,
        )

    def test_rejects_start_time_without_timezone(self) -> None:
        local_time_without_timezone = datetime(
            2026,
            8,
            29,
            10,
            0,
            0,
        )

        with self.assertRaises(ValueError):
            self.generator.generate_successful_delivery(
                order_id="ORD-2001",
                start_time=local_time_without_timezone,
            )

    def test_events_are_immutable(self) -> None:
        with self.assertRaises(FrozenInstanceError):
            self.events[0].event_type = "MODIFIED"

    def test_serialization_uses_contract_field_names(self) -> None:
        serialized_event = self.events[0].to_dict()

        self.assertEqual(
            "ORD-2001",
            serialized_event["aggregateId"],
        )
        self.assertEqual(
            1,
            serialized_event["aggregateVersion"],
        )
        self.assertEqual(
            "ORDER_CREATED",
            serialized_event["eventType"],
        )
        self.assertEqual(
            "ecommerce-node-01",
            serialized_event["producerId"],
        )

        self.assertNotIn(
            "aggregate_id",
            serialized_event,
        )
        self.assertNotIn(
            "event_type",
            serialized_event,
        )

    def test_serialization_outputs_utc_timestamp(self) -> None:
        serialized_event = self.events[0].to_dict()

        self.assertEqual(
            "2026-08-29T10:00:00Z",
            serialized_event["occurredAt"],
        )


if __name__ == "__main__":
    unittest.main()