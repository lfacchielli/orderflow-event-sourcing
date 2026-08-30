from dataclasses import dataclass, field


@dataclass
class PublishReport:
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    partitions: dict[int, int] = field(default_factory=dict)

    def record_delivery(self, partition: int) -> None:
        self.delivered += 1
        self.partitions[partition] = (
            self.partitions.get(partition, 0) + 1
        )

    def record_failure(self) -> None:
        self.failed += 1