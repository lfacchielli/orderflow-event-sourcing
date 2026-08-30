import json
from collections.abc import Iterable
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from orderflow_generator.kafka.publish_report import PublishReport


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str,
        topic: str,
        producer: Any | None = None,
    ) -> None:
        self.topic = topic
        self.producer = producer or Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "snappy",
            }
        )

    def publish(
        self,
        events: Iterable[dict[str, Any]],
        timeout_seconds: float = 30.0,
    ) -> PublishReport:
        report = PublishReport()

        def delivery_callback(
            error: KafkaError | None,
            message: Message,
        ) -> None:
            if error is not None:
                report.record_failure()
                return

            report.record_delivery(message.partition())

        for event in events:
            self._validate_event(event)
            report.attempted += 1

            key = event["aggregateId"]
            value = json.dumps(
                event,
                separators=(",", ":"),
                ensure_ascii=False,
            )

            while True:
                try:
                    self.producer.produce(
                        topic=self.topic,
                        key=key,
                        value=value,
                        on_delivery=delivery_callback,
                    )
                    break
                except BufferError:
                    self.producer.poll(0.1)

            self.producer.poll(0)

        undelivered = self.producer.flush(timeout_seconds)

        if undelivered > 0:
            raise RuntimeError(
                f"{undelivered} event(s) remained undelivered."
            )

        if report.failed > 0:
            raise RuntimeError(
                f"{report.failed} event delivery failure(s)."
            )

        if report.delivered != report.attempted:
            raise RuntimeError(
                "Delivery report mismatch: "
                f"attempted={report.attempted}, "
                f"delivered={report.delivered}."
            )

        return report

    @staticmethod
    def _validate_event(event: dict[str, Any]) -> None:
        required_fields = {
            "eventId",
            "eventType",
            "aggregateId",
            "aggregateVersion",
            "occurredAt",
            "producerId",
            "producerType",
            "correlationId",
            "payload",
        }

        missing_fields = required_fields - event.keys()

        if missing_fields:
            missing = ", ".join(sorted(missing_fields))
            raise ValueError(
                f"Event is missing required field(s): {missing}"
            )

        if not event["aggregateId"]:
            raise ValueError("aggregateId cannot be empty.")