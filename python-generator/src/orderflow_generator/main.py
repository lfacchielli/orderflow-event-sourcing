import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from orderflow_generator.generation.scenario_generator import (
    ScenarioGenerator,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a deterministic OrderFlow event scenario."
    )

    parser.add_argument(
        "--order-id",
        default="ORD-2001",
        help="Aggregate identifier used by all generated events.",
    )

    parser.add_argument(
        "--output",
        default="output/generated-events.json",
        help="Destination JSON file.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    generator = ScenarioGenerator()
    events = generator.generate_successful_delivery(
        order_id=arguments.order_id,
        start_time=datetime(
            2026,
            8,
            29,
            10,
            0,
            0,
            tzinfo=timezone.utc,
        ),
    )

    output_path = Path(arguments.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    serialized_events = [event.to_dict() for event in events]

    output_path.write_text(
        json.dumps(
            serialized_events,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print("OrderFlow scenario generated successfully.")
    print(f"Order:       {arguments.order_id}")
    print(f"Events:      {len(serialized_events)}")
    print(f"First event: {serialized_events[0]['eventType']}")
    print(f"Last event:  {serialized_events[-1]['eventType']}")
    print(f"Output:      {output_path.resolve()}")


if __name__ == "__main__":
    main()