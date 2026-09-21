# OrderFlow Event Sourcing

Progetto universitario sviluppato per il corso di **Distributed Edge Programming**.

OrderFlow è un sistema didattico basato su eventi che dimostra come sia possibile ricostruire lo stato corrente e storico di un ordine logistico partendo da una sequenza immutabile e versionata di eventi.

Il progetto combina Apache Kafka, Java, Python e PostgreSQL per implementare la generazione degli eventi, la ricostruzione dello stato, il replay, l’elaborazione idempotente, gli snapshot periodici, le API HTTP e una dashboard accessibile dal browser.

## Obiettivi del progetto

L’obiettivo principale è dimostrare che lo stato corrente di un ordine non deve necessariamente essere considerato l’unica fonte di informazione.

Lo stato può invece essere derivato dalla cronologia completa degli eventi:

```text
ORDER_CREATED
    |
    v
ORDER_CONFIRMED
    |
    v
PAYMENT_COMPLETED
    |
    v
INVENTORY_RESERVED
    |
    v
ORDER_PACKED
    |
    v
SHIPMENT_STARTED
    |
    v
HUB_REACHED
    |
    v
DELIVERY_DELAYED
    |
    v
OUT_FOR_DELIVERY
    |
    v
ORDER_DELIVERED
```

La stessa cronologia può essere utilizzata per ottenere:

- lo stato corrente dell’ordine;
- una versione storica intermedia;
- lo stato a un determinato timestamp;
- una proiezione PostgreSQL ricostruita;
- un replay ottimizzato che parte da uno snapshot.

## Concetti principali

Il progetto affronta i seguenti argomenti:

- Event Sourcing;
- eventi immutabili e versionati;
- topic, partizioni e offset di Apache Kafka;
- architettura producer-consumer;
- chiavi Kafka e ordinamento per aggregato;
- commit manuale degli offset;
- ricostruzione dello stato in Java;
- validazione della versione dell’aggregato;
- elaborazione idempotente degli eventi;
- proiezioni di lettura in PostgreSQL;
- replay completo e storico;
- snapshot periodici;
- replay ottimizzato a partire dagli snapshot;
- API HTTP;
- dashboard realizzata con HTML, CSS e JavaScript.

## Architettura

```text
+-------------------------+
| Python Event Generator  |
| deterministic scenarios |
+------------+------------+
             |
             | JSON events
             | Kafka key = orderId
             v
+-------------------------+
| Apache Kafka            |
| topics and partitions   |
| offsets and retention   |
+------------+------------+
             |
             | poll
             | manual offset commit
             v
+-------------------------+
| Java State Reconstructor|
| deserialize             |
| validate                |
| project                 |
| replay                  |
+------------+------------+
             |
             | JDBC transaction
             v
+-------------------------+
| PostgreSQL              |
| order_states            |
| processed_events        |
| order_snapshots         |
+------------+------------+
             |
             | SQL queries
             v
+-------------------------+
| Java HTTP API           |
| health and order APIs   |
+------------+------------+
             |
             | Fetch API
             v
+-------------------------+
| Operations Dashboard    |
| HTML, CSS, JavaScript   |
+-------------------------+
```

## Tecnologie utilizzate

- **Apache Kafka** per il trasporto durevole degli eventi;
- **Python 3** per la generazione deterministica degli eventi sintetici;
- **Java 17** per deserializzazione, proiezione, replay e API;
- **Maven** per compilazione, test e packaging;
- **PostgreSQL** per proiezioni, deduplicazione e snapshot;
- **Docker Compose** per l’infrastruttura locale;
- **HTML5, CSS3 e JavaScript** per la dashboard operativa.

Non sono richiesti Node.js, npm o strumenti di build frontend.

## Struttura del repository

```text
orderflow-event-sourcing/
├── docs/
│   ├── 01-kafka-event-streaming.md
│   ├── 02-event-sourcing-cqrs.md
│   ├── 03-producer-consumer-topic-partizioni.md
│   ├── 04-replay-snapshot-idempotenza.md
│   └── 05-architettura-orderflow.md
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
│       │   ├── java/
│       │   └── resources/
│       │       └── static/
│       │           ├── index.html
│       │           ├── css/styles.css
│       │           └── js/app.js
│       └── test/
│
├── python-generator/
│   ├── requirements.txt
│   ├── src/
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

## Scenario dimostrativo

Lo scenario deterministico principale utilizza l’ordine:

```text
ORD-2001
```

L’ordine attraversa dieci versioni dell’aggregato:

```text
v1  ORDER_CREATED          CREATED
v2  ORDER_CONFIRMED        CONFIRMED
v3  PAYMENT_COMPLETED      PAID
v4  INVENTORY_RESERVED     INVENTORY_RESERVED
v5  ORDER_PACKED           PACKED
v6  SHIPMENT_STARTED       IN_TRANSIT, HUB-MODENA
v7  HUB_REACHED            IN_TRANSIT, HUB-BOLOGNA
v8  DELIVERY_DELAYED       IN_TRANSIT, delay 35
v9  OUT_FOR_DELIVERY       OUT_FOR_DELIVERY, HUB-FIRENZE
v10 ORDER_DELIVERED        DELIVERED
```

Lo stato finale ricostruito è:

```text
orderId:            ORD-2001
status:             DELIVERED
version:            10
customerId:         CUS-501
currency:           EUR
totalAmount:        55.00
currentHub:         HUB-FIRENZE
visitedHubs:        HUB-MODENA, HUB-BOLOGNA, HUB-FIRENZE
totalDelayMinutes:  35
hasDelay:           true
```

La policy corrente genera due snapshot:

```text
v5  PACKED
v10 DELIVERED
```

## Modello PostgreSQL

L’applicazione Java utilizza tre tabelle principali.

### `orderflow.order_states`

Contiene l’ultimo stato ricostruito per ogni ordine.

La tabella rappresenta una proiezione di lettura, ottimizzata per le query e per la dashboard.

### `orderflow.processed_events`

Conserva gli identificatori degli eventi già processati e i relativi metadati Kafka.

Questa tabella fornisce la deduplicazione applicativa e permette di gestire in modo sicuro una strategia di elaborazione at-least-once.

### `orderflow.order_snapshots`

Conserva lo stato completo degli ordini, serializzato in formato JSONB, in corrispondenza di specifiche versioni dell’aggregato.

La policy attuale crea uno snapshot ogni cinque versioni.

## Transazione di elaborazione

Ogni nuovo evento viene elaborato seguendo questa sequenza:

```text
BEGIN PostgreSQL

check processed_events
load previous order state
apply OrderStateProjector
save current order state
save snapshot when required
register processed event

COMMIT PostgreSQL

COMMIT Kafka offset
```

Se la transazione PostgreSQL fallisce, tutte le modifiche vengono annullate e l’offset Kafka non viene committato.

Se PostgreSQL completa il commit ma l’applicazione si arresta prima del commit dell’offset Kafka, il record può essere ricevuto nuovamente. La tabella `processed_events` riconosce l’`eventId` già presente e impedisce che l’evento venga applicato due volte.

## Modalità di replay

Il progetto supporta differenti modalità di replay.

### Replay completo

```java
replayService.replayAll(events);
```

Il replay completo parte da uno stato iniziale vuoto e applica l’intera cronologia.

### Replay fino a una versione

```java
replayService.replayToVersion(
    events,
    5
);
```

Questa operazione permette di ricostruire lo stato dell’ordine alla versione richiesta.

### Replay fino a un timestamp

```java
replayService.replayAtTime(
    events,
    targetTime
);
```

In questo caso vengono applicati soltanto gli eventi avvenuti entro il momento specificato.

### Ricostruzione della proiezione PostgreSQL

La proiezione corrente può essere eliminata e ricreata a partire dalla cronologia degli eventi.

Questo dimostra che la tabella `order_states` è un dato derivato e non la fonte di verità principale.

### Replay storico da Kafka

Un reader dedicato può assegnare manualmente le partizioni Kafka, eseguire il seek all’inizio e leggere la cronologia di un ordine senza committare offset del consumer.

### Replay da snapshot

Per `ORD-2001`, il replay che parte dallo snapshot alla versione 5 applica soltanto gli eventi dalla versione 6 alla versione 10.

```text
Replay completo:          10 eventi
Replay da snapshot v5:     5 eventi
Applicazioni evitate:      5 eventi
Riduzione del replay:     50%
Stato finale:             equivalente
```

## API HTTP

L’applicazione Java espone i seguenti endpoint:

```text
GET /api/health
GET /api/orders
GET /api/orders/{orderId}
GET /api/orders/{orderId}/snapshots
GET /api/orders/{orderId}/state?version={version}
```

Esempi:

```text
GET http://localhost:8081/api/health
GET http://localhost:8081/api/orders
GET http://localhost:8081/api/orders/ORD-2001
GET http://localhost:8081/api/orders/ORD-2001/snapshots
GET http://localhost:8081/api/orders/ORD-2001/state?version=5
```

## Dashboard operativa

La dashboard viene servita direttamente dal server HTTP Java:

```text
http://localhost:8081/
```

L’interfaccia permette di visualizzare:

- lo stato dell’applicazione e di PostgreSQL;
- le metriche aggregate degli ordini;
- la lista delle proiezioni correnti;
- la ricerca e il filtro per stato;
- il dettaglio completo di un ordine;
- gli articoli e le informazioni logistiche;
- gli hub visitati e il ritardo accumulato;
- gli snapshot disponibili;
- lo stato storico corrispondente a uno snapshot.

Il frontend utilizza esclusivamente HTML, CSS e JavaScript standard.

## Prerequisiti

Per eseguire il progetto sono richiesti:

- Git;
- Docker Desktop;
- Docker Compose;
- Java 17;
- Maven;
- Python 3.

Node.js, npm e strumenti di build frontend non sono necessari.

## Configurazione locale

Creare il file locale di configurazione partendo dall’esempio versionato:

```powershell
Copy-Item .env.example .env
```

La configurazione locale predefinita utilizza la porta host `5433` per PostgreSQL.

Questa scelta evita conflitti con eventuali installazioni PostgreSQL locali che utilizzano già la porta `5432`.

```text
Windows host 127.0.0.1:5433
        |
        v
PostgreSQL container:5432
```

Quando Java viene eseguito direttamente da Windows, l’URL JDBC è:

```text
jdbc:postgresql://127.0.0.1:5433/orderflow
```

Se Java venisse eseguito in futuro all’interno dello stesso Docker Compose, dovrebbe invece utilizzare:

```text
jdbc:postgresql://postgres:5432/orderflow
```

## Avvio rapido

### 1. Clonare il repository

```powershell
git clone <URL_REPOSITORY>
cd orderflow-event-sourcing
```

### 2. Creare la configurazione locale

```powershell
Copy-Item .env.example .env
```

### 3. Avviare Kafka e PostgreSQL

```powershell
docker compose `
    --env-file .env `
    -f infra\docker-compose.yml `
    up -d
```

### 4. Verificare i container

```powershell
docker compose `
    --env-file .env `
    -f infra\docker-compose.yml `
    ps
```

Kafka e PostgreSQL devono risultare attivi. I servizi con health check devono raggiungere lo stato `healthy`.

### 5. Compilare il modulo Java

```powershell
cd java-state-reconstructor
mvn clean package
```

La build deve terminare con:

```text
BUILD SUCCESS
```

### 6. Costruire il runtime classpath

```powershell
mvn dependency:build-classpath `
    "-Dmdep.outputFile=target/runtime-classpath.txt"

$runtimeClasspath = (
    Get-Content `
        "target/runtime-classpath.txt" `
        -Raw
).Trim()
```

### 7. Configurare PostgreSQL nella sessione PowerShell corrente

```powershell
$env:POSTGRES_JDBC_URL = `
    "jdbc:postgresql://127.0.0.1:5433/orderflow"

$env:POSTGRES_USER = (
    docker exec orderflow-postgres `
        printenv POSTGRES_USER
).Trim()

$env:POSTGRES_PASSWORD = (
    docker exec orderflow-postgres `
        printenv POSTGRES_PASSWORD
).Trim()
```

### 8. Preparare lo scenario dimostrativo

Se il database è vuoto, elaborare lo scenario deterministico:

```powershell
java `
    -cp "target\classes;$runtimeClasspath" `
    it.orderflow.reconstructor.processing.TransactionalProcessingCheck `
    "../samples/scenarios/successful-delivery-with-delay.json"
```

Output previsto alla prima esecuzione:

```text
Transactional processing completed.
Processed:  10
Duplicates: 0
```

Se il comando viene eseguito una seconda volta:

```text
Transactional processing completed.
Processed:  0
Duplicates: 10
```

La seconda esecuzione dimostra l’idempotenza del processamento.

### 9. Configurare e avviare l’API

```powershell
$env:API_HOST = "127.0.0.1"
$env:API_PORT = "8081"
```

```powershell
java `
    -cp "target\classes;$runtimeClasspath" `
    it.orderflow.reconstructor.api.ApiApplication
```

Output previsto:

```text
OrderFlow API started.
Address: http://localhost:8081
Health:  http://localhost:8081/api/health
```

### 10. Aprire la dashboard

```text
http://localhost:8081/
```

La dashboard dovrebbe mostrare `ORD-2001`, il relativo stato corrente e gli snapshot disponibili.

## Test

### Test Java

```powershell
cd java-state-reconstructor
mvn clean test
```

### Packaging Java

```powershell
mvn clean package
```

### Test Python

```powershell
cd python-generator

$env:PYTHONPATH = "src"

python -m unittest discover `
    -s tests `
    -p "test_*.py" `
    -v
```

### Verificare la proiezione PostgreSQL corrente

```powershell
docker exec orderflow-postgres `
    psql -U orderflow -d orderflow `
    -c "SELECT order_id, status, aggregate_version, current_hub, total_delay_minutes FROM orderflow.order_states ORDER BY order_id;"
```

Risultato previsto:

```text
ORD-2001 | DELIVERED | 10 | HUB-FIRENZE | 35
```

### Verificare gli snapshot

```powershell
docker exec orderflow-postgres `
    psql -U orderflow -d orderflow `
    -c "SELECT order_id, aggregate_version, state_data->>'status' AS status FROM orderflow.order_snapshots ORDER BY order_id, aggregate_version;"
```

Risultato previsto:

```text
ORD-2001 | 5  | PACKED
ORD-2001 | 10 | DELIVERED
```

### Verificare le risorse statiche nel JAR

Dalla radice del repository:

```powershell
jar tf `
    "java-state-reconstructor\target\java-state-reconstructor-0.1.0-SNAPSHOT.jar" |
    Select-String "static/"
```

Il risultato deve includere:

```text
static/index.html
static/css/styles.css
static/js/app.js
```

## Documentazione

La documentazione teorica e pratica è disponibile nella cartella `docs`:

1. `docs/01-kafka-event-streaming.md`
2. `docs/02-event-sourcing-cqrs.md`
3. `docs/03-producer-consumer-topic-partizioni.md`
4. `docs/04-replay-snapshot-idempotenza.md`
5. `docs/05-architettura-orderflow.md`