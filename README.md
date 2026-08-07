# OrderFlow Event Sourcing

Educational project developed for the Distributed Edge Programming course.

The project demonstrates how Apache Kafka can be used as an event store to reconstruct the current and historical state of logistics orders from immutable events.

## Main concepts

- Event Sourcing
- Apache Kafka topics and partitions
- Distributed event producers
- Java state reconstruction
- Consumer groups and parallel processing
- PostgreSQL projections and snapshots
- Python synthetic event generation
- Web-based demonstration interface
- Event-driven analytics

## Project status

The project is currently under development.


## Repository structure

- `analytics`: analytical queries and generated results.
- `docs`: theoretical and architectural project documentation.
- `infra`: Docker, Kafka and PostgreSQL infrastructure.
- `java-state-reconstructor`: Java service responsible for rebuilding order state.
- `python-generator`: distributed synthetic event generators.
- `samples`: example events and simulation scenarios.
- `web-ui`: web interface used to control and demonstrate the system.