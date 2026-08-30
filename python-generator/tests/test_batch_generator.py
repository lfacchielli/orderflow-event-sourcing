import unittest
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from orderflow_generator.generation.batch_generator import (
    BatchGenerator,
)


class BatchGeneratorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.start_time = datetime(
            2026,
            8,
            30,
            10,
            0,
            0,
            tzinfo=timezone.utc,
        )

        self.generator = BatchGenerator(seed=42)
        self.events = self.generator.generate_orders(
            order_count=20,
            start_time=self.start_time,
            delay_probability=0.30,
            cancellation_probability=0.10,
            delivery_failure_probability=0.10,
        )

    @staticmethod
    def group_by_order(events):
        grouped_events = defaultdict(list)

        for event in events:
            grouped_events[event.aggregate_id].append(event)

        return grouped_events

    @staticmethod
    def serialize(events):
        return [
            event.to_dict()
            for event in events
        ]

    def test_generates_requested_number_of_orders(self) -> None:
        order_ids = {
            event.aggregate_id
            for event in self.events
        }

        self.assertEqual(20, len(order_ids))

    def test_generates_expected_order_identifiers(self) -> None:
        order_ids = sorted({
            event.aggregate_id
            for event in self.events
        })

        self.assertEqual("ORD-3001", order_ids[0])
        self.assertEqual("ORD-3020", order_ids[-1])

    def test_versions_are_progressive_for_every_order(self) -> None:
        grouped_events = self.group_by_order(self.events)

        for order_id, events in grouped_events.items():
            ordered_events = sorted(
                events,
                key=lambda event: event.aggregate_version,
            )

            actual_versions = [
                event.aggregate_version
                for event in ordered_events
            ]

            expected_versions = list(
                range(1, len(ordered_events) + 1)
            )

            self.assertEqual(
                expected_versions,
                actual_versions,
                msg=f"Invalid versions for {order_id}",
            )

    def test_every_order_starts_with_order_created(self) -> None:
        grouped_events = self.group_by_order(self.events)

        for order_id, events in grouped_events.items():
            ordered_events = sorted(
                events,
                key=lambda event: event.aggregate_version,
            )

            self.assertEqual(
                "ORDER_CREATED",
                ordered_events[0].event_type,
                msg=f"Invalid first event for {order_id}",
            )

    def test_every_order_has_one_terminal_event(self) -> None:
        terminal_event_types = {
            "ORDER_DELIVERED",
            "ORDER_CANCELLED",
            "DELIVERY_FAILED",
        }

        grouped_events = self.group_by_order(self.events)

        for order_id, events in grouped_events.items():
            terminal_events = [
                event
                for event in events
                if event.event_type in terminal_event_types
            ]

            self.assertEqual(
                1,
                len(terminal_events),
                msg=f"Invalid terminal events for {order_id}",
            )

    def test_event_identifiers_are_unique(self) -> None:
        event_ids = [
            event.event_id
            for event in self.events
        ]

        self.assertEqual(
            len(event_ids),
            len(set(event_ids)),
        )

    def test_same_seed_produces_same_dataset(self) -> None:
        second_generator = BatchGenerator(seed=42)

        second_events = second_generator.generate_orders(
            order_count=20,
            start_time=self.start_time,
            delay_probability=0.30,
            cancellation_probability=0.10,
            delivery_failure_probability=0.10,
        )

        self.assertEqual(
            self.serialize(self.events),
            self.serialize(second_events),
        )

    def test_different_seed_produces_different_dataset(self) -> None:
        second_generator = BatchGenerator(seed=43)

        second_events = second_generator.generate_orders(
            order_count=20,
            start_time=self.start_time,
            delay_probability=0.30,
            cancellation_probability=0.10,
            delivery_failure_probability=0.10,
        )

        self.assertNotEqual(
            self.serialize(self.events),
            self.serialize(second_events),
        )

    def test_all_events_have_utc_timestamps(self) -> None:
        for event in self.events:
            self.assertIsNotNone(event.occurred_at.tzinfo)
            self.assertEqual(event.occurred_at.utcoffset(), timedelta(seconds=0))

    def test_all_events_have_matching_correlation_id(self) -> None:
        for event in self.events:
            expected_correlation_id = event.aggregate_id.replace(
                "ORD-",
                "CORR-",
            )

            self.assertEqual(
                expected_correlation_id,
                event.correlation_id,
            )

    def test_cancelled_orders_stop_after_cancellation(self) -> None:
        grouped_events = self.group_by_order(self.events)

        for order_id, events in grouped_events.items():
            ordered_events = sorted(
                events,
                key=lambda event: event.aggregate_version,
            )

            event_types = [
                event.event_type
                for event in ordered_events
            ]

            if "ORDER_CANCELLED" in event_types:
                self.assertEqual(
                    [
                        "ORDER_CREATED",
                        "ORDER_CONFIRMED",
                        "ORDER_CANCELLED",
                    ],
                    event_types,
                    msg=f"Invalid cancellation path for {order_id}",
                )

    def test_delayed_orders_have_positive_delay(self) -> None:
        delay_events = [
            event
            for event in self.events
            if event.event_type == "DELIVERY_DELAYED"
        ]

        for event in delay_events:
            self.assertGreater(
                event.payload["delayMinutes"],
                0,
            )

    def test_zero_delay_probability_generates_no_delays(self) -> None:
        generator = BatchGenerator(seed=42)

        events = generator.generate_orders(
            order_count=20,
            start_time=self.start_time,
            delay_probability=0.0,
            cancellation_probability=0.0,
            delivery_failure_probability=0.0,
        )

        delay_events = [
            event
            for event in events
            if event.event_type == "DELIVERY_DELAYED"
        ]

        self.assertEqual([], delay_events)

    def test_full_delay_probability_delays_all_active_orders(self) -> None:
        generator = BatchGenerator(seed=42)

        events = generator.generate_orders(
            order_count=10,
            start_time=self.start_time,
            delay_probability=1.0,
            cancellation_probability=0.0,
            delivery_failure_probability=0.0,
        )

        delayed_order_ids = {
            event.aggregate_id
            for event in events
            if event.event_type == "DELIVERY_DELAYED"
        }

        self.assertEqual(10, len(delayed_order_ids))

    def test_full_cancellation_probability_cancels_all_orders(self) -> None:
        generator = BatchGenerator(seed=42)

        events = generator.generate_orders(
            order_count=10,
            start_time=self.start_time,
            delay_probability=0.0,
            cancellation_probability=1.0,
            delivery_failure_probability=0.0,
        )

        grouped_events = self.group_by_order(events)

        self.assertEqual(10, len(grouped_events))

        for order_events in grouped_events.values():
            self.assertEqual(
                [
                    "ORDER_CREATED",
                    "ORDER_CONFIRMED",
                    "ORDER_CANCELLED",
                ],
                [
                    event.event_type
                    for event in order_events
                ],
            )

    def test_full_failure_probability_fails_all_deliveries(self) -> None:
        generator = BatchGenerator(seed=42)

        events = generator.generate_orders(
            order_count=10,
            start_time=self.start_time,
            delay_probability=0.0,
            cancellation_probability=0.0,
            delivery_failure_probability=1.0,
        )

        final_events = [
            event
            for event in events
            if event.event_type == "DELIVERY_FAILED"
        ]

        self.assertEqual(10, len(final_events))

    def test_uses_distributed_producer_types(self) -> None:
        producer_types = {
            event.producer_type
            for event in self.events
        }

        self.assertTrue(
            {
                "ECOMMERCE",
                "PAYMENT",
                "WAREHOUSE",
                "LOGISTICS_HUB",
                "DELIVERY",
            }.issubset(producer_types)
        )

    def test_rejects_zero_orders(self) -> None:
        with self.assertRaises(ValueError):
            self.generator.generate_orders(
                order_count=0,
                start_time=self.start_time,
            )

    def test_rejects_start_time_without_timezone(self) -> None:
        start_time_without_timezone = datetime(
            2026,
            8,
            30,
            10,
            0,
            0,
        )

        with self.assertRaises(ValueError):
            self.generator.generate_orders(
                order_count=10,
                start_time=start_time_without_timezone,
            )

    def test_rejects_probability_below_zero(self) -> None:
        with self.assertRaises(ValueError):
            self.generator.generate_orders(
                order_count=10,
                start_time=self.start_time,
                delay_probability=-0.1,
            )

    def test_rejects_probability_above_one(self) -> None:
        with self.assertRaises(ValueError):
            self.generator.generate_orders(
                order_count=10,
                start_time=self.start_time,
                cancellation_probability=1.1,
            )


if __name__ == "__main__":
    unittest.main()