import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from orderflow_generator.generation.batch_generator import (
    BatchGenerator,
)


def probability(value: str) -> float:
    parsed_value = float(value)

    if parsed_value < 0 or parsed_value > 1:
        raise argparse.ArgumentTypeError(
            "Probability must be between 0 and 1."
        )

    return parsed_value


def positive_integer(value: str) -> int:
    parsed_value = int(value)

    if parsed_value < 1:
        raise argparse.ArgumentTypeError(
            "The value must be greater than zero."
        )

    return parsed_value


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate multiple synthetic OrderFlow event streams."
        )
    )

    parser.add_argument(
        "--orders",
        type=positive_integer,
        default=10,
        help="Number of orders to generate.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed used to make generation reproducible.",
    )

    parser.add_argument(
        "--delay-probability",
        type=probability,
        default=0.20,
        help="Probability that an order accumulates a delay.",
    )

    parser.add_argument(
        "--cancellation-probability",
        type=probability,
        default=0.05,
        help="Probability that an order is cancelled.",
    )

    parser.add_argument(
        "--failure-probability",
        type=probability,
        default=0.05,
        help="Probability that the final delivery fails.",
    )

    parser.add_argument(
        "--output",
        default="output/generated-batch.json",
        help="Destination JSON file.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    generator = BatchGenerator(seed=arguments.seed)
    events = generator.generate_orders(
        order_count=arguments.orders,
        start_time=datetime(
            2026,
            8,
            30,
            10,
            0,
            0,
            tzinfo=timezone.utc,
        ),
        delay_probability=arguments.delay_probability,
        cancellation_probability=(
            arguments.cancellation_probability
        ),
        delivery_failure_probability=(
            arguments.failure_probability
        ),
    )

    serialized_events = [
        event.to_dict()
        for event in events
    ]

    output_path = Path(arguments.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(
        json.dumps(
            serialized_events,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    final_events = {
        "ORDER_DELIVERED",
        "ORDER_CANCELLED",
        "DELIVERY_FAILED",
    }

    final_status_counts = Counter(
        event.event_type
        for event in events
        if event.event_type in final_events
    )

    delayed_orders = {
        event.aggregate_id
        for event in events
        if event.event_type == "DELIVERY_DELAYED"
    }

    producer_counts = Counter(
        event.producer_id
        for event in events
    )

    print("OrderFlow batch generated successfully.")
    print(f"Seed:              {arguments.seed}")
    print(f"Orders:            {arguments.orders}")
    print(f"Events:            {len(events)}")
    print(f"Delayed orders:    {len(delayed_orders)}")
    print(
        "Delivered orders:  "
        f"{final_status_counts['ORDER_DELIVERED']}"
    )
    print(
        "Cancelled orders:  "
        f"{final_status_counts['ORDER_CANCELLED']}"
    )
    print(
        "Failed deliveries: "
        f"{final_status_counts['DELIVERY_FAILED']}"
    )
    print(f"Output:            {output_path.resolve()}")
    print("")
    print("Events by producer")

    for producer_id, count in sorted(producer_counts.items()):
        print(f"  {producer_id}: {count}")


if __name__ == "__main__":
    main()