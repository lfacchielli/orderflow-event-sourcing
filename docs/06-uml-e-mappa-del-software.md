# UML e Mappa Architetturale di OrderFlow


## 1. Vista generale del sistema

```mermaid
flowchart LR
    PG[Python Generator]
    KP[Kafka Publisher]
    K[(Apache Kafka)]
    KC[Java Kafka Consumer]
    DES[OrderEventDeserializer]
    TP[TransactionalOrderEventProcessor]
    PRJ[OrderStateProjector]
    DB[(PostgreSQL)]
    API[Java HTTP API]
    UI[HTML CSS JavaScript Dashboard]

    PG -->|eventi versionati| KP
    KP -->|key = orderId| K
    K -->|ConsumerRecord| KC
    KC --> DES
    DES -->|OrderEvent| TP
    TP --> PRJ
    TP -->|transazione JDBC| DB
    API -->|query JDBC| DB
    UI -->|fetch /api/...| API
```

### Responsabilità sintetiche

```text
Python Generator          crea scenari ed eventi logistici
Kafka Publisher           serializza e pubblica gli eventi
Apache Kafka              conserva topic, partizioni e offset
Java Consumer             legge i ConsumerRecord
Deserializer              converte JSON in OrderEvent
Transactional Processor   coordina validazione e transazione
OrderStateProjector       calcola il nuovo stato
PostgreSQL                conserva proiezioni, deduplica e snapshot
HTTP API                  espone lo stato al browser
Dashboard                 visualizza stato corrente e storico
```

---

## 2. Alberatura delle cartelle

```text
orderflow-event-sourcing/
├── docs/
│   ├── 01-kafka-event-streaming.md
│   ├── 02-event-sourcing-cqrs.md
│   ├── 03-producer-consumer-topic-partizioni.md
│   ├── 04-replay-snapshot-idempotenza.md
│   ├── 05-architettura-orderflow.md
│   └── 06-uml-e-mappa-del-software.md
│
├── infra/
│   ├── docker-compose.yml
│   └── postgres/
│       ├── init.sql
│       └── migrations/
│           ├── 001-create-order-projections.sql
│           └── 002-create-order-snapshots.sql
│
├── java-state-reconstructor/
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/it/orderflow/reconstructor/
│       │   │   ├── api/
│       │   │   ├── consumer/
│       │   │   ├── domain/
│       │   │   ├── persistence/
│       │   │   ├── processing/
│       │   │   ├── projection/
│       │   │   ├── replay/
│       │   │   ├── serialization/
│       │   │   └── snapshot/
│       │   └── resources/static/
│       │       ├── index.html
│       │       ├── css/styles.css
│       │       └── js/app.js
│       └── test/
│
├── python-generator/
│   ├── requirements.txt
│   ├── src/orderflow_generator/
│   │   ├── main.py
│   │   ├── batch_main.py
│   │   ├── publish_main.py
│   │   ├── generation/
│   │   └── kafka/
│   └── tests/
│
├── samples/
│   └── scenarios/
│       └── successful-delivery-with-delay.json
│
├── .env.example
├── .gitignore
└── README.md
```

### Dipendenze tra directory

```mermaid
flowchart TD
    Samples[samples]
    Python[python-generator]
    Infra[infra]
    Java[java-state-reconstructor]
    Docs[docs]

    Samples -->|scenario JSON| Python
    Samples -->|scenario controllato| Java
    Python -->|pubblica eventi| Infra
    Java -->|consuma Kafka e usa PostgreSQL| Infra
    Docs -.->|documenta| Python
    Docs -.->|documenta| Java
    Docs -.->|documenta| Infra
```

---

## 3. Diagramma dei componenti

```mermaid
flowchart TB
    subgraph PROD[Producer side]
        SG[Scenario Generator]
        BG[Batch Generator]
        PUB[Kafka Publisher]
        SG --> PUB
        BG --> PUB
    end

    subgraph KAFKA[Event transport]
        T1[(order-events)]
        T2[(order-events-dashboard)]
    end

    subgraph JAVA[Java State Reconstructor]
        KCF[KafkaConsumerFactory]
        KOC[TransactionalKafkaOrderConsumer]
        OED[OrderEventDeserializer]
        TOEP[TransactionalOrderEventProcessor]
        OSP[OrderStateProjector]
        SVC[SnapshotService]
        RPL[OrderReplayService]
        API[ApiServer]
    end

    subgraph POSTGRES[PostgreSQL]
        OS[(order_states)]
        PE[(processed_events)]
        SS[(order_snapshots)]
    end

    subgraph WEB[Web layer]
        SFH[StaticFileHandler]
        OH[OrdersHandler]
        HH[OrderHistoryHandler]
        JS[app.js]
        HTML[index.html]
    end

    PUB --> T1
    PUB -.-> T2
    T1 --> KCF
    T2 -.-> KCF
    KCF --> KOC
    KOC --> OED
    KOC --> TOEP
    TOEP --> OSP
    TOEP --> SVC
    TOEP --> OS
    TOEP --> PE
    SVC --> SS
    RPL --> OSP
    API --> SFH
    API --> OH
    OH --> OS
    OH --> HH
    HH --> SS
    HTML --> JS
    JS -->|fetch| OH
```

---

## 4. Topic Kafka e flussi

### Topic principale

```text
order-events
```

Uso previsto:

```text
Producer Python
    |
    | key = orderId
    | value = evento JSON
    v
Kafka topic order-events
    |
    v
Consumer Java
```

### Regola di partizionamento

```text
Kafka key = aggregateId = orderId
```

```mermaid
flowchart LR
    E1[ORDER_CREATED ORD-2001]
    E2[ORDER_PACKED ORD-2001]
    E3[ORDER_DELIVERED ORD-2001]
    HASH[Kafka partitioner]
    P1[(Partition N)]

    E1 --> HASH
    E2 --> HASH
    E3 --> HASH
    HASH -->|same key| P1
```

Questa scelta mantiene nella stessa partizione tutti gli eventi dello stesso ordine.

### Configurazione consumer

```text
enable.auto.commit = false
auto.offset.reset = earliest
allow.auto.create.topics = false
key.deserializer = StringDeserializer
value.deserializer = StringDeserializer
```

### Consumer group

```text
order-state-reconstructor
```

Per le prove dashboard può essere usato un gruppo separato:

```text
order-state-reconstructor-dashboard
```

---

## 5. Generatore Python

### Vista dei componenti

```mermaid
classDiagram
    class ScenarioGenerator {
        +generate(...)
    }

    class BatchGenerator {
        +generate(...)
    }

    class KafkaPublisher {
        +publish(...)
        +flush()
    }

    class PublishReport {
        +publishedEvents
        +failedEvents
    }

    class main_py {
        +main()
    }

    class batch_main_py {
        +main()
    }

    class publish_main_py {
        +main()
    }

    main_py --> ScenarioGenerator
    batch_main_py --> BatchGenerator
    publish_main_py --> BatchGenerator
    publish_main_py --> KafkaPublisher
    KafkaPublisher --> PublishReport
```


### `main.py`

Responsabilità prevista:

- punto di ingresso per generare uno scenario singolo;
- costruzione degli eventi dell'ordine;
- eventuale scrittura del risultato su file o stdout.

### `batch_main.py`

Responsabilità prevista:

- punto di ingresso per generare più ordini;
- utilizzo del batch generator;
- gestione di seed, quantità e identificatori progressivi.

### `publish_main.py`

Responsabilità prevista:

- generare o caricare eventi;
- configurare Kafka;
- pubblicare i record;
- stampare un report finale.

### `generation/batch_generator.py`

Responsabilità:

- costruire più scenari;
- mantenere coerenza tra ordine, evento e versione;
- produrre un output deterministico quando viene usato lo stesso seed.

### `kafka/publisher.py`

Responsabilità:

```text
OrderEvent
    |
    v
JSON serialization
    |
    v
Kafka producer.send / produce
    |
    | key = orderId
    v
order-events
```

### `kafka/publish_report.py`

Responsabilità:

- contare eventi pubblicati;
- registrare eventuali errori;
- restituire un risultato leggibile al punto di ingresso.

---

## 6. Package Java

```mermaid
flowchart TD
    API[api]
    CON[consumer]
    DOM[domain]
    PER[persistence]
    PROC[processing]
    PROJ[projection]
    REP[replay]
    SER[serialization]
    SNAP[snapshot]

    API --> PER
    API --> SNAP
    CON --> SER
    CON --> PROC
    SER --> DOM
    PROC --> DOM
    PROC --> PROJ
    PROC --> PER
    PROC --> SNAP
    PROJ --> DOM
    PER --> DOM
    REP --> DOM
    REP --> PROJ
    REP --> SER
    SNAP --> DOM
    SNAP --> PROJ
```

### Significato dei package

```text
domain          modelli ed eventi immutabili
serialization   conversione JSON -> OrderEvent
projection      transizioni di stato
processing      transazione applicativa
a consumer       integrazione Kafka
persistence     accesso JDBC
replay          ricostruzione storica
snapshot        salvataggio e replay ottimizzato
api             server HTTP, endpoint e file statici
```

---

## 7. Dominio e proiezione

### Diagramma delle classi principali

```mermaid
classDiagram
    class OrderEvent {
        <<interface or abstract domain type>>
        +eventId()
        +eventType()
        +aggregateId()
        +aggregateVersion()
        +occurredAt()
    }

    class OrderState {
        +orderId()
        +status()
        +version()
        +customerId()
        +items()
        +totalAmount()
        +destination()
        +currentHub()
        +visitedHubs()
        +totalDelayMinutes()
        +hasDelay()
        +createdAt()
        +deliveredAt()
        +lastUpdatedAt()
    }

    class OrderItem {
        +productId()
        +productName()
        +quantity()
        +unitPrice()
    }

    class Destination {
        +city()
        +country()
    }

    class OrderStatus {
        <<enumeration>>
        CREATED
        CONFIRMED
        PAID
        INVENTORY_RESERVED
        PACKED
        IN_TRANSIT
        OUT_FOR_DELIVERY
        DELIVERED
        CANCELLED
        DELIVERY_FAILED
    }

    class OrderStateProjector {
        +apply(OrderState, OrderEvent) OrderState
    }

    OrderState --> OrderStatus
    OrderState *-- OrderItem
    OrderState *-- Destination
    OrderStateProjector --> OrderState
    OrderStateProjector --> OrderEvent
```

### `OrderStateProjector.apply`

```text
Input:
    stato precedente, anche null
    evento corrente

Controlli:
    aggregateId coerente
    versione attesa
    transizione valida
    stato terminale

Output:
    nuovo OrderState immutabile
```

### Esempio

```text
OrderState(PACKED, v5)
+
SHIPMENT_STARTED(v6)
=
OrderState(IN_TRANSIT, v6)
```

---

## 8. Consumer e processamento transazionale

### Class diagram

```mermaid
classDiagram
    class KafkaConsumerSettings {
        +fromEnvironment() KafkaConsumerSettings
        +defaultSettings() KafkaConsumerSettings
        +bootstrapServers()
        +groupId()
        +clientId()
        +topic()
        +pollTimeout()
    }

    class KafkaConsumerFactory {
        +create(KafkaConsumerSettings) Consumer
    }

    class TransactionalKafkaOrderConsumer {
        -Consumer consumer
        -String topic
        -Duration pollTimeout
        -OrderEventDeserializer deserializer
        -TransactionalOrderEventProcessor processor
        +run()
        +stop()
        +close()
    }

    class ConsumerApplication {
        +main(String[])
    }

    class TransactionalOrderEventProcessor {
        -ConnectionFactory connectionFactory
        -OrderStateRepository stateRepository
        -ProcessedEventRepository processedEventRepository
        -OrderStateProjector projector
        -SnapshotService snapshotService
        +process(OrderEvent, KafkaRecordMetadata) TransactionalProcessingResult
    }

    class KafkaRecordMetadata {
        +topic()
        +partition()
        +offset()
    }

    class TransactionalProcessingResult {
        +state()
        +alreadyProcessed()
    }

    ConsumerApplication --> KafkaConsumerSettings
    ConsumerApplication --> KafkaConsumerFactory
    ConsumerApplication --> TransactionalKafkaOrderConsumer
    KafkaConsumerFactory --> KafkaConsumerSettings
    TransactionalKafkaOrderConsumer --> TransactionalOrderEventProcessor
    TransactionalKafkaOrderConsumer --> KafkaRecordMetadata
    TransactionalOrderEventProcessor --> TransactionalProcessingResult
```

### Flusso del consumer

```text
poll()
  |
  v
for each ConsumerRecord
  |
  +-> deserialize JSON
  +-> validate record.key == aggregateId
  +-> processor.process(event, metadata)
  |
  v
commitSync()
```

### Flusso del processor

```text
open JDBC connection
setAutoCommit(false)
        |
        v
processed event exists?
        |
        +-> yes: return duplicate
        |
        v
load current OrderState
        |
        v
projector.apply
        |
        v
save OrderState
        |
        +-> SnapshotService.createIfRequired
        |
        v
save ProcessedEvent
        |
        v
commit
```

---

## 9. Persistenza PostgreSQL

### Class diagram

```mermaid
classDiagram
    class DatabaseSettings {
        +fromEnvironment() DatabaseSettings
        +jdbcUrl()
        +user()
        +password()
    }

    class ConnectionFactory {
        +openConnection() Connection
    }

    class OrderStateRepository {
        <<interface>>
        +findAll(Connection) List~OrderState~
        +findById(Connection, String) Optional~OrderState~
        +save(Connection, OrderState)
    }

    class JdbcOrderStateRepository {
        +findAll(Connection) List~OrderState~
        +findById(Connection, String) Optional~OrderState~
        +save(Connection, OrderState)
    }

    class ProcessedEventRepository {
        <<interface>>
        +exists(...)
        +save(Connection, ProcessedEvent)
    }

    class JdbcProcessedEventRepository {
        +exists(...)
        +save(Connection, ProcessedEvent)
    }

    class ProcessedEvent {
        +eventId()
        +orderId()
        +aggregateVersion()
        +topicName()
        +partitionNumber()
        +recordOffset()
    }

    ConnectionFactory --> DatabaseSettings
    JdbcOrderStateRepository ..|> OrderStateRepository
    JdbcProcessedEventRepository ..|> ProcessedEventRepository
    JdbcProcessedEventRepository --> ProcessedEvent
```

### Tabelle usate

```text
JdbcOrderStateRepository
    -> orderflow.order_states

JdbcProcessedEventRepository
    -> orderflow.processed_events

JdbcOrderSnapshotRepository
    -> orderflow.order_snapshots
```

---

## 10. Replay

### Class diagram

```mermaid
classDiagram
    class OrderReplayService {
        -OrderStateProjector projector
        +replayAll(List~OrderEvent~) ReplayResult
        +replayToVersion(List~OrderEvent~, long) ReplayResult
        +replayAtTime(List~OrderEvent~, Instant) ReplayResult
    }

    class ReplayResult {
        +state()
        +appliedEvents()
        +availableEvents()
        +isCompleteReplay()
    }

    class ReplayException

    class KafkaOrderHistoryReader {
        +readHistory(String orderId) List~OrderEvent~
    }

    class KafkaReplayCheck {
        +main(String[])
    }

    class PostgresProjectionReplayCheck {
        +main(String[])
    }

    OrderReplayService --> ReplayResult
    OrderReplayService --> ReplayException
    OrderReplayService --> OrderStateProjector
    KafkaOrderHistoryReader --> OrderEventDeserializer
    KafkaReplayCheck --> KafkaOrderHistoryReader
    KafkaReplayCheck --> OrderReplayService
    PostgresProjectionReplayCheck --> OrderReplayService
```

### Modalità

```text
replayAll
    applica tutta la cronologia

replayToVersion
    applica fino alla targetVersion

replayAtTime
    applica fino al targetTime

KafkaOrderHistoryReader
    legge senza committare offset

PostgresProjectionReplayCheck
    elimina e ricostruisce una proiezione
```

---

## 11. Snapshot

### Class diagram

```mermaid
classDiagram
    class OrderSnapshot {
        +orderId()
        +aggregateVersion()
        +state()
        +createdAt()
    }

    class SnapshotPolicy {
        -long interval
        +interval()
        +shouldCreateSnapshot(OrderState) boolean
    }

    class SnapshotService {
        -OrderSnapshotRepository repository
        -SnapshotPolicy policy
        +createIfRequired(Connection, OrderState) boolean
    }

    class OrderSnapshotRepository {
        <<interface>>
        +findAll(Connection, String) List~OrderSnapshot~
        +findByVersion(Connection, String, long) Optional~OrderSnapshot~
        +findLatest(Connection, String) Optional~OrderSnapshot~
        +findLatestBefore(Connection, String, long) Optional~OrderSnapshot~
        +save(Connection, OrderSnapshot)
    }

    class JdbcOrderSnapshotRepository

    class SnapshotReplayService {
        -OrderStateProjector projector
        +replay(OrderSnapshot, List~OrderEvent~, long) SnapshotReplayResult
    }

    class SnapshotReplayResult {
        +state()
        +startingVersion()
        +appliedEvents()
    }

    SnapshotService --> SnapshotPolicy
    SnapshotService --> OrderSnapshotRepository
    JdbcOrderSnapshotRepository ..|> OrderSnapshotRepository
    SnapshotReplayService --> OrderSnapshot
    SnapshotReplayService --> SnapshotReplayResult
    SnapshotReplayService --> OrderStateProjector
```

### Policy

```java
state.version() % interval == 0
```

Con intervallo 5:

```text
v5  -> snapshot PACKED
v10 -> snapshot DELIVERED
```

---

## 12. API HTTP e dashboard

### Class diagram API

```mermaid
classDiagram
    class ApiApplication {
        +main(String[])
    }

    class ApiSettings {
        +fromEnvironment() ApiSettings
        +host()
        +port()
        +backlog()
        +workerThreads()
    }

    class ApiServer {
        -HttpServer server
        -ExecutorService executor
        +start()
        +port()
        +close()
    }

    class JsonHttpResponse {
        +send(HttpExchange, int, Object)
        +sendMethodNotAllowed(HttpExchange)
        +sendOptions(HttpExchange)
    }

    class HealthHandler {
        +handle(HttpExchange)
    }

    class OrdersHandler {
        +handle(HttpExchange)
    }

    class OrderHistoryHandler {
        +handle(HttpExchange)
    }

    class StaticFileHandler {
        +handle(HttpExchange)
    }

    ApiApplication --> ApiSettings
    ApiApplication --> ApiServer
    ApiServer --> HealthHandler
    ApiServer --> OrdersHandler
    ApiServer --> OrderHistoryHandler
    ApiServer --> StaticFileHandler
    HealthHandler --> JsonHttpResponse
    OrdersHandler --> JsonHttpResponse
    OrderHistoryHandler --> JsonHttpResponse
```

### Routing

```text
/                                      StaticFileHandler
/css/styles.css                        StaticFileHandler
/js/app.js                             StaticFileHandler
/api/health                            HealthHandler
/api/orders                            OrdersHandler
/api/orders/{orderId}                  OrdersHandler
/api/orders/{orderId}/snapshots        OrderHistoryHandler
/api/orders/{orderId}/state?version=N  OrderHistoryHandler
```

### DTO API

```text
HealthResponse
OrderSummaryResponse
SnapshotSummaryResponse
HistoricalStateResponse
```

### JavaScript dashboard

Funzioni principali previste in `app.js`:

```text
requestJson(url)
loadDashboard()
renderHealth()
renderMetrics()
populateStatusFilter()
applyFilters()
renderOrders()
openOrder(orderId)
closeDrawer()
renderOrderDrawer()
loadHistoricalState(orderId, version)
statusBadge(status)
formatStatus(status)
formatAmount(value)
formatDate(value)
escapeHtml(value)
showError(message)
clearError()
```

### Comunicazione frontend-backend

```mermaid
sequenceDiagram
    participant B as Browser
    participant S as StaticFileHandler
    participant JS as app.js
    participant O as OrdersHandler
    participant H as OrderHistoryHandler
    participant DB as PostgreSQL

    B->>S: GET /
    S-->>B: index.html
    B->>S: GET /css/styles.css
    B->>S: GET /js/app.js
    S-->>B: static resources
    JS->>O: GET /api/orders
    O->>DB: SELECT order_states
    DB-->>O: projections
    O-->>JS: JSON orders
    JS->>O: GET /api/orders/ORD-2001
    O->>DB: SELECT order state
    O-->>JS: JSON detail
    JS->>H: GET snapshots
    H->>DB: SELECT order_snapshots
    H-->>JS: JSON snapshots
```

---

## 13. Diagramma ER

```mermaid
erDiagram
    ORDER_STATES {
        varchar order_id PK
        varchar status
        bigint aggregate_version
        varchar customer_id
        varchar currency
        jsonb items
        numeric total_amount
        jsonb destination
        varchar current_hub
        jsonb visited_hubs
        integer total_delay_minutes
        boolean has_delay
        timestamptz created_at
        timestamptz delivered_at
        timestamptz last_updated_at
    }

    PROCESSED_EVENTS {
        uuid event_id PK
        varchar order_id
        bigint aggregate_version
        varchar topic_name
        integer partition_number
        bigint record_offset
        timestamptz processed_at
    }

    ORDER_SNAPSHOTS {
        bigint snapshot_id PK
        varchar order_id
        bigint aggregate_version
        jsonb state_data
        timestamptz created_at
    }

    ORDER_STATES ||--o{ PROCESSED_EVENTS : "order_id"
    ORDER_STATES ||--o{ ORDER_SNAPSHOTS : "order_id"
```

### Relazioni logiche

Le tabelle non devono necessariamente avere foreign key fisiche per rappresentare la relazione. Il collegamento logico avviene tramite:

```text
order_id
```

Vincoli importanti:

```text
processed_events.event_id UNIQUE
processed_events(order_id, aggregate_version) UNIQUE
processed_events(topic, partition, offset) UNIQUE
order_snapshots(order_id, aggregate_version) UNIQUE
```

---

## 14. Sequenze principali

### 14.1 Processamento live Kafka

```mermaid
sequenceDiagram
    participant P as Python Producer
    participant K as Kafka
    participant C as TransactionalKafkaOrderConsumer
    participant D as OrderEventDeserializer
    participant T as TransactionalOrderEventProcessor
    participant R as Repositories
    participant S as SnapshotService
    participant DB as PostgreSQL

    P->>K: publish JSON, key=orderId
    K->>C: ConsumerRecord
    C->>D: deserialize(value)
    D-->>C: OrderEvent
    C->>C: validate key == aggregateId
    C->>T: process(event, metadata)
    T->>DB: BEGIN
    T->>R: processed event exists?
    R->>DB: SELECT processed_events
    T->>R: find current state
    R->>DB: SELECT order_states
    T->>T: projector.apply
    T->>R: save state
    R->>DB: UPSERT order_states
    T->>S: createIfRequired
    S->>DB: INSERT order_snapshots
    T->>R: save processed event
    R->>DB: INSERT processed_events
    T->>DB: COMMIT
    T-->>C: TransactionalProcessingResult
    C->>K: commitSync
```

### 14.2 Replay completo

```mermaid
sequenceDiagram
    participant F as Scenario JSON
    participant DES as OrderEventDeserializer
    participant R as OrderReplayService
    participant P as OrderStateProjector

    F->>DES: JSON events
    DES-->>R: List<OrderEvent>
    loop ordered events
        R->>P: apply(currentState, event)
        P-->>R: nextState
    end
    R-->>R: ReplayResult
```

### 14.3 Replay da snapshot

```mermaid
sequenceDiagram
    participant C as SnapshotReplayIntegrationCheck
    participant SR as SnapshotRepository
    participant DB as PostgreSQL
    participant RS as SnapshotReplayService
    participant P as OrderStateProjector

    C->>SR: findLatestBefore(orderId, targetVersion)
    SR->>DB: SELECT latest snapshot
    DB-->>SR: snapshot v5
    SR-->>C: OrderSnapshot
    C->>RS: replay(snapshot, events, v10)
    loop events v6-v10
        RS->>P: apply(state, event)
        P-->>RS: nextState
    end
    RS-->>C: DELIVERED v10, appliedEvents=5
```