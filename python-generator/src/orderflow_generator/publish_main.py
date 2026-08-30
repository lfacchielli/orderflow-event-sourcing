import argparse
import json
from pathlib import Path
from typing import Any

from orderflow_generator.kafka.publisher import KafkaPublisher


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish an OrderFlow JSON event batch to Kafka."
        )
    )

    parser.add_argument(
        "--input",
        default="output/generated-batch.json",
        help="JSON file containing an array of events.",
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap server list.",
    )

    parser.add_argument(
        "--topic",
        default="order-events",
        help="Kafka destination topic.",
    )

    parser.add_argument(
        "--client-id",
        default="orderflow-python-generator",
        help="Kafka producer client identifier.",
    )

    return parser.parse_args()


def load_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(
            f"Input file not found: {path}"
        )

    content = json.loads(
        path.read_text(encoding="utf-8")
    )

    if not isinstance(content, list):
        raise ValueError(
            "Input JSON must contain an array of events."
        )

    if not content:
        raise ValueError(
            "Input JSON must contain at least one event."
        )

    return content


def main() -> None:
    arguments = parse_arguments()
    input_path = Path(arguments.input)
    events = load_events(input_path)

    publisher = KafkaPublisher(
        bootstrap_servers=arguments.bootstrap_servers,
        client_id=arguments.client_id,
        topic=arguments.topic,
    )

    print("Publishing OrderFlow events to Kafka.")
    print(f"Input:      {input_path.resolve()}")
    print(f"Broker:     {arguments.bootstrap_servers}")
    print(f"Topic:      {arguments.topic}")
    print(f"Events:     {len(events)}")
    print("")

    report = publisher.publish(events)

    print("Kafka publication completed successfully.")
    print(f"Attempted:  {report.attempted}")
    print(f"Delivered:  {report.delivered}")
    print(f"Failed:     {report.failed}")
    print("")
    print("Delivered events by partition")

    for partition, count in sorted(
        report.partitions.items()
    ):
        print(f"  Partition {partition}: {count}")


if __name__ == "__main__":
    main()