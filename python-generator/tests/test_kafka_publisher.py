import json
import unittest
from dataclasses import dataclass

from orderflow_generator.kafka.publisher import KafkaPublisher


@dataclass
class FakeMessage:
    assigned_partition: int

    def partition(self) -> int:
        return self.assigned_partition

    def key(self):
        return None


class FakeProducer:
    def __init__(
        self,
        partitions_by_key: dict[str, int] | None = None,
        undelivered: int = 0,
        delivery_error=None,
    ) -> None:
        self.partitions_by_key = partitions_by_key or {}
        self.undelivered = undelivered
        self.delivery_error = delivery_error
        self.records: list[dict] = []
        self.poll_calls: list[float] = []
        self.flush_calls: list[float] = []

    def produce(
        self,
        topic,
        key,
        value,
        on_delivery,
    ) -> None:
        partition = self.partitions_by_key.get(key, 0)

        self.records.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "partition": partition,
            }
        )

        on_delivery(
            self.delivery_error,
            FakeMessage(partition),
        )

    def poll(self, timeout) -> None:
        self.poll_calls.append(timeout)

    def flush(self, timeout) -> int:
        self.flush_calls.append(timeout)
        return self.undelivered


class KafkaPublisherTest(unittest.TestCase):
    def valid_event(
        self,
        aggregate_id: str = "ORD-3001",
        version: int = 1,
    ) -> dict:
        return {
            "eventId": f"event-{aggregate_id}-{version}",
            "eventType": "ORDER_CREATED",
            "aggregateId": aggregate_id,
            "aggregateVersion": version,
            "occurredAt": "2026-08-30T10:00:00Z",
            "producerId": "ecommerce-node-01",
            "producerType": "ECOMMERCE",
            "correlationId": aggregate_id.replace(
                "ORD-",
                "CORR-",
            ),
            "payload": {},
        }

    def publisher_with(self, producer) -> KafkaPublisher:
        return KafkaPublisher(
            bootstrap_servers="unused:9092",
            client_id="test-producer",
            topic="order-events",
            producer=producer,
        )

    def test_publishes_event_with_aggregate_id_as_key(self) -> None:
        producer = FakeProducer()
        publisher = self.publisher_with(producer)

        publisher.publish([self.valid_event()])

        self.assertEqual(1, len(producer.records))
        self.assertEqual(
            "ORD-3001",
            producer.records[0]["key"],
        )

    def test_publishes_event_to_configured_topic(self) -> None:
        producer = FakeProducer()
        publisher = self.publisher_with(producer)

        publisher.publish([self.valid_event()])

        self.assertEqual(
            "order-events",
            producer.records[0]["topic"],
        )

    def test_serializes_event_as_json(self) -> None:
        producer = FakeProducer()
        publisher = self.publisher_with(producer)

        publisher.publish([self.valid_event()])

        serialized_value = producer.records[0]["value"]
        decoded_event = json.loads(serialized_value)

        self.assertEqual("ORD-3001", decoded_event["aggregateId"])
        self.assertEqual(1, decoded_event["aggregateVersion"])
        self.assertEqual(
            "ORDER_CREATED",
            decoded_event["eventType"],
        )

    def test_preserves_complete_event_content(self) -> None:
        producer = FakeProducer()
        publisher = self.publisher_with(producer)
        event = self.valid_event()

        publisher.publish([event])

        decoded_event = json.loads(
            producer.records[0]["value"]
        )

        self.assertEqual(event, decoded_event)

    def test_publishes_multiple_events(self) -> None:
        producer = FakeProducer()
        publisher = self.publisher_with(producer)

        events = [
            self.valid_event(version=1),
            self.valid_event(version=2),
            self.valid_event(version=3),
        ]

        publisher.publish(events)

        self.assertEqual(3, len(producer.records))
        self.assertEqual(
            [1, 2, 3],
            [
                json.loads(record["value"])["aggregateVersion"]
                for record in producer.records
            ],
        )

    def test_uses_same_key_for_events_of_same_order(self) -> None:
        producer = FakeProducer(
            partitions_by_key={
                "ORD-3001": 2,
            }
        )
        publisher = self.publisher_with(producer)

        events = [
            self.valid_event(version=1),
            self.valid_event(version=2),
            self.valid_event(version=3),
        ]

        publisher.publish(events)

        produced_keys = [
            record["key"]
            for record in producer.records
        ]

        assigned_partitions = [
            record["partition"]
            for record in producer.records
        ]

        self.assertEqual(
            ["ORD-3001", "ORD-3001", "ORD-3001"],
            produced_keys,
        )
        self.assertEqual(
            [2, 2, 2],
            assigned_partitions,
        )

    def test_supports_different_orders(self) -> None:
        producer = FakeProducer(
            partitions_by_key={
                "ORD-3001": 0,
                "ORD-3002": 2,
            }
        )
        publisher = self.publisher_with(producer)

        events = [
            self.valid_event(
                aggregate_id="ORD-3001",
                version=1,
            ),
            self.valid_event(
                aggregate_id="ORD-3002",
                version=1,
            ),
        ]

        publisher.publish(events)

        self.assertEqual(
            ["ORD-3001", "ORD-3002"],
            [
                record["key"]
                for record in producer.records
            ],
        )

        self.assertEqual(
            [0, 2],
            [
                record["partition"]
                for record in producer.records
            ],
        )

    def test_calls_poll_after_producing_each_event(self) -> None:
        producer = FakeProducer()
        publisher = self.publisher_with(producer)

        publisher.publish(
            [
                self.valid_event(version=1),
                self.valid_event(version=2),
                self.valid_event(version=3),
            ]
        )

        self.assertEqual(3, len(producer.poll_calls))

    def test_flushes_producer_after_publication(self) -> None:
        producer = FakeProducer()
        publisher = self.publisher_with(producer)

        publisher.publish([self.valid_event()])

        self.assertEqual(1, len(producer.flush_calls))

    def test_raises_error_when_messages_remain_undelivered(self) -> None:
        producer = FakeProducer(undelivered=2)
        publisher = self.publisher_with(producer)

        with self.assertRaises(RuntimeError):
            publisher.publish([self.valid_event()])


if __name__ == "__main__":
    unittest.main()