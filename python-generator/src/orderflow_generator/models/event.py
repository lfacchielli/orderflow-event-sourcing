from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class OrderEvent:
    event_id: UUID
    event_type: str
    aggregate_id: str
    aggregate_version: int
    occurred_at: datetime
    producer_id: str
    producer_type: str
    correlation_id: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        event = asdict(self)

        event["eventId"] = str(event.pop("event_id"))
        event["eventType"] = event.pop("event_type")
        event["aggregateId"] = event.pop("aggregate_id")
        event["aggregateVersion"] = event.pop("aggregate_version")
        event["occurredAt"] = (
            event.pop("occurred_at")
            .isoformat()
            .replace("+00:00", "Z")
        )
        event["producerId"] = event.pop("producer_id")
        event["producerType"] = event.pop("producer_type")
        event["correlationId"] = event.pop("correlation_id")

        return event