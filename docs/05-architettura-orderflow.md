# Architettura e Guida Operativa di OrderFlow

## Indice

1. [Scopo del progetto](#1-scopo-del-progetto)
2. [Risultato raggiunto](#2-risultato-raggiunto)
3. [Architettura generale](#3-architettura-generale)
4. [Struttura logica dei componenti](#4-struttura-logica-dei-componenti)
5. [Contratto degli eventi](#5-contratto-degli-eventi)
6. [Scenario ORD-2001](#6-scenario-ord-2001)
7. [Flusso di elaborazione live](#7-flusso-di-elaborazione-live)
8. [Persistenza PostgreSQL](#8-persistenza-postgresql)
9. [Replay storico](#9-replay-storico)
10. [Snapshot](#10-snapshot)
11. [API HTTP e dashboard](#11-api-http-e-dashboard)
12. [Test e verifiche](#12-test-e-verifiche)
13. [Riproducibilità da GitHub](#13-riproducibilità-da-github)
14. [Procedura di avvio consigliata](#14-procedura-di-avvio-consigliata)
15. [Demo per l'esame](#15-demo-per-lesame)
16. [Limitazioni note](#16-limitazioni-note)
17. [Sviluppi futuri](#17-sviluppi-futuri)
18. [Checklist finale del repository](#18-checklist-finale-del-repository)

## 1. Scopo del progetto

OrderFlow è un progetto didattico di Data Engineering ed Event Sourcing applicato a un dominio logistico. L'obiettivo è dimostrare che lo stato corrente di un ordine può essere ricostruito da una sequenza immutabile e versionata di eventi.

Il progetto non si limita a calcolare lo stato finale. Dimostra anche:

- trasporto degli eventi con Kafka;
- partizionamento per ordine;
- consumer Java con commit manuale;
- validazione della sequenza;
- persistenza di proiezioni PostgreSQL;
- idempotenza;
- replay completo e storico;
- snapshot periodici;
- API HTTP;
- dashboard HTML, CSS e JavaScript.

## 2. Risultato raggiunto

Lo scenario principale `ORD-2001` attraversa dieci eventi e produce:

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
hasDelay:            true
```

Sono inoltre disponibili due snapshot:

```text
v5  PACKED
v10 DELIVERED
```

La dashboard consente di visualizzare lo stato corrente e aprire lo stato storico corrispondente agli snapshot.

## 3. Architettura generale

```text
+-------------------------+
| Python Event Generator  |
| scenari deterministici  |
+------------+------------+
             |
             | JSON, key = orderId
             v
+-------------------------+
| Apache Kafka            |
| topic order-events      |
| partizioni e offset     |
+------------+------------+
             |
             | poll, manual commit
             v
+-------------------------+
| Java State Reconstructor|
| deserialize, validate,  |
| project, replay         |
+------------+------------+
             |
             | transazione JDBC
             v
+-------------------------+
| PostgreSQL              |
| order_states            |
| processed_events        |
| order_snapshots         |
+------------+------------+
             |
             | query
             v
+-------------------------+
| Java HTTP API           |
| /api/health             |
| /api/orders             |
+------------+------------+
             |
             | fetch
             v
+-------------------------+
| Dashboard               |
| HTML + CSS + JavaScript |
+-------------------------+
```

## 4. Struttura logica dei componenti

### Generatore Python

Responsabilità:

- creazione di eventi sintetici;
- timestamp UTC;
- versionamento progressivo;
- serializzazione JSON;
- pubblicazione Kafka prevista dai componenti `kafka/publisher.py` e `publish_main.py`;
- test sullo scenario e sul batch.

### Kafka

Responsabilità:

- memorizzazione durevole del flusso;
- disaccoppiamento tra Python e Java;
- ordinamento per partizione;
- tracking degli offset;
- replay storico.

### Ricostruttore Java

Package principali:

```text
domain          modelli immutabili
serialization   JSON -> OrderEvent
projection      regole di transizione
processing      processamento e transazioni
consumer        integrazione Kafka
persistence     JDBC e repository
replay          replay completo e storico
snapshot        policy e replay ottimizzato
api             API HTTP e file statici
```

### PostgreSQL

Responsabilità:

- stato corrente interrogabile;
- deduplicazione applicativa;
- snapshot JSONB;
- transazioni atomiche.

### Dashboard

Responsabilità:

- metriche sintetiche;
- lista e ricerca ordini;
- dettaglio dell'ordine;
- hub, ritardi e articoli;
- navigazione tra stato corrente e snapshot.

## 5. Contratto degli eventi

Struttura generale:

```json
{
  "eventId": "89d...",
  "eventType": "ORDER_CREATED",
  "aggregateId": "ORD-2001",
  "aggregateVersion": 1,
  "occurredAt": "2026-08-29T10:00:00Z",
  "producerId": "ecommerce-node-01",
  "producerType": "ECOMMERCE",
  "correlationId": "CORR-2001",
  "payload": {
    "customerId": "CUS-501",
    "currency": "EUR",
    "totalAmount": 55.00
  }
}
```

Regole:

```text
eventId             univoco
aggregateId          identificatore ordine
aggregateVersion     progressivo da 1
occurredAt           timestamp UTC
Kafka key            uguale ad aggregateId
payload              dipendente dal tipo evento
```

## 6. Scenario ORD-2001

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

Lo scenario è sufficientemente ricco per dimostrare:

- transizioni;
- versionamento;
- dati monetari;
- hub multipli;
- ritardo cumulativo;
- stato terminale;
- snapshot intermedio e finale.

## 7. Flusso di elaborazione live

```text
ConsumerRecord<String, String>
        |
        v
OrderEventDeserializer
        |
        v
record.key == aggregateId ?
        |
        v
processed_events contains eventId ?
        |
        v
load previous OrderState
        |
        v
OrderStateProjector.apply
        |
        v
save order_states
        |
        +-> snapshot se version % 5 == 0
        |
        v
save processed_events
        |
        v
commit PostgreSQL
        |
        v
commit Kafka offset
```

Il projector rimane indipendente dall'infrastruttura:

```java
OrderState currentState = projector.apply(
    previousState,
    event
);
```

## 8. Persistenza PostgreSQL

### `orderflow.order_states`

Una riga per ordine, ottimizzata per la lettura.

Campi principali:

```text
order_id
status
aggregate_version
customer_id
currency
items JSONB
total_amount
destination JSONB
current_hub
visited_hubs JSONB
total_delay_minutes
created_at
delivered_at
last_updated_at
```

### `orderflow.processed_events`

Garantisce deduplicazione e tracciamento:

```text
event_id UNIQUE
order_id + aggregate_version UNIQUE
topic + partition + offset UNIQUE
```

### `orderflow.order_snapshots`

```text
order_id
aggregate_version
state_data JSONB
created_at
UNIQUE(order_id, aggregate_version)
```

## 9. Replay storico

Sono state implementate più modalità:

### Replay completo

```java
replayService.replayAll(events);
```

### Replay per versione

```java
replayService.replayToVersion(events, 5);
```

### Replay temporale

```java
replayService.replayAtTime(events, targetTime);
```

### Replay della proiezione PostgreSQL

```text
legge stato originale
elimina proiezione ORD-2001
riapplica gli eventi
salva la nuova proiezione
confronta gli stati
```

### Replay diretto da Kafka

```text
assign partitions
seekToBeginning
read until captured end offsets
filter by orderId
no offset commit
```

## 10. Snapshot

Policy:

```text
snapshot ogni 5 versioni
```

Risultati:

```text
v5  PACKED
v10 DELIVERED
```

Confronto:

```text
Replay completo:        10 eventi
Replay da snapshot v5:   5 eventi
Riduzione:              50%
Stato finale:            equivalente
```

Nel confronto degli importi viene usata equivalenza numerica:

```java
first.totalAmount().compareTo(
    second.totalAmount()
) == 0
```

## 11. API HTTP e dashboard

### Endpoint

```text
GET /api/health
GET /api/orders
GET /api/orders/{orderId}
GET /api/orders/{orderId}/snapshots
GET /api/orders/{orderId}/state?version=5
```

### File statici

```text
src/main/resources/static/
├── index.html
├── css/styles.css
└── js/app.js
```

La stessa applicazione Java serve API e frontend:

```text
http://localhost:8081/
```

Vantaggi:

- nessun Node.js;
- nessun framework frontend;
- nessun server statico separato;
- stessa origine per frontend e API;
- distribuzione nel JAR.

## 12. Test e verifiche

### Java

```powershell
cd java-state-reconstructor
mvn clean test
mvn clean package
```

### Risorse statiche nel JAR

```powershell
jar tf `
    "target\java-state-reconstructor-0.1.0-SNAPSHOT.jar" |
    Select-String "static/"
```

### Python

```powershell
cd python-generator
$env:PYTHONPATH = "src"
python -m unittest discover `
    -s tests `
    -p "test_*.py" `
    -v
```

### PostgreSQL

```powershell
docker exec orderflow-postgres `
    psql -U orderflow -d orderflow `
    -c "SELECT order_id, status, aggregate_version FROM orderflow.order_states;"
```

### Snapshot

```powershell
docker exec orderflow-postgres `
    psql -U orderflow -d orderflow `
    -c "SELECT order_id, aggregate_version, state_data->>'status' FROM orderflow.order_snapshots ORDER BY aggregate_version;"
```

## 13. Riproducibilità da GitHub

### Risposta breve

Il professore può clonare e utilizzare il progetto **se nel repository sono presenti configurazione di esempio e istruzioni complete**. Il codice e le migrazioni realizzate sono sufficienti, ma la riproducibilità non dipende soltanto dal codice.

Prerequisiti:

- Git;
- Docker Desktop con Docker Compose;
- Java 17;
- Maven;
- Python 3;
- una porta libera per API e servizi;
- file `.env` ricavabile da un `.env.example` versionato.

### Punto critico: `.env`

Il file `.env` normalmente è escluso da Git. Deve quindi esistere un file:

```text
.env.example
```

senza segreti reali, contenente almeno valori di sviluppo.

Esempio concettuale:

```dotenv
POSTGRES_DB=orderflow
POSTGRES_USER=orderflow
POSTGRES_PASSWORD=change-me-local
POSTGRES_PORT=5433
```

### Punto critico: porta PostgreSQL

Nel computer di sviluppo la porta `5432` era già occupata da PostgreSQL locale. Il container è stato quindi esposto sulla porta host `5433`:

```text
Windows host 127.0.0.1:5433
        -> container postgres:5432
```

Dentro Docker Compose, un'applicazione Java containerizzata userebbe invece:

```text
jdbc:postgresql://postgres:5432/orderflow
```

### Valutazione realistica

Se mancano `.env.example`, README di avvio o uno script che prepara lo scenario `ORD-2001`, il professore può compilare il codice ma potrebbe non riprodurre la demo al primo tentativo. La checklist finale di questo documento serve proprio a eliminare questo rischio.

## 14. Procedura di avvio consigliata

### 14.1 Clonare

```powershell
git clone <URL_REPOSITORY>
cd orderflow-event-sourcing
```

### 14.2 Preparare l'ambiente

```powershell
Copy-Item .env.example .env
```

Verificare che la porta PostgreSQL scelta non sia già occupata.

### 14.3 Avviare l'infrastruttura

```powershell
docker compose `
    --env-file .env `
    -f infra\docker-compose.yml `
    up -d
```

### 14.4 Verificare i container

```powershell
docker compose `
    --env-file .env `
    -f infra\docker-compose.yml `
    ps
```

### 14.5 Compilare Java

```powershell
cd java-state-reconstructor
mvn clean package
```

### 14.6 Costruire il runtime classpath

```powershell
mvn dependency:build-classpath `
    "-Dmdep.outputFile=target/runtime-classpath.txt"

$runtimeClasspath = (
    Get-Content "target/runtime-classpath.txt" -Raw
).Trim()
```

### 14.7 Configurare la sessione locale

```powershell
$env:POSTGRES_JDBC_URL = `
    "jdbc:postgresql://127.0.0.1:5433/orderflow"

$env:POSTGRES_USER = "orderflow"
$env:POSTGRES_PASSWORD = "<valore-da-env>"
$env:API_HOST = "127.0.0.1"
$env:API_PORT = "8081"
```

### 14.8 Avviare API e dashboard

```powershell
java `
    -cp "target\classes;$runtimeClasspath" `
    it.orderflow.reconstructor.api.ApiApplication
```

Aprire:

```text
http://localhost:8081/
```

### 14.9 Se il database è vuoto

Eseguire uno dei check di integrazione forniti dal progetto per ricostruire `ORD-2001` dal file scenario, usando la classe e il package effettivamente presenti. La documentazione definitiva del repository dovrebbe indicare un solo comando canonico.

Esempio già utilizzato durante lo sviluppo:

```powershell
java `
    -cp "target\classes;$runtimeClasspath" `
    it.orderflow.reconstructor.processing.TransactionalProcessingCheck `
    "../samples/scenarios/successful-delivery-with-delay.json"
```

## 15. Demo per l'esame

Sequenza consigliata, durata 8-10 minuti.

### 1. Problema

Spiegare che uno stato CRUD mostra dove si trova un ordine, ma non come ci è arrivato.

### 2. Evento

Aprire `order-created.json` o lo scenario completo e illustrare:

```text
eventId
aggregateId
aggregateVersion
eventType
occurredAt
payload
```

### 3. Kafka

Spiegare:

```text
key = orderId
ordinamento per partizione
offset
consumer group
```

### 4. Projector

Mostrare `OrderStateProjector.apply` e sottolineare che è puro e deterministico.

### 5. Test

```powershell
mvn clean test
```

### 6. PostgreSQL

Mostrare stato e snapshot:

```text
ORD-2001 DELIVERED v10
snapshot v5 PACKED
snapshot v10 DELIVERED
```

### 7. Dashboard

Aprire l'ordine, mostrare hub, ritardo, articoli e stato corrente.

### 8. Time travel

Cliccare snapshot v5:

```text
PACKED v5
```

Tornare allo stato corrente:

```text
DELIVERED v10
```

### 9. Conclusione

```text
Gli eventi rappresentano la storia.
Lo stato è una proiezione ricostruibile.
Gli snapshot ottimizzano il replay.
```

## 16. Limitazioni note

- scenario dimostrativo principale concentrato su `ORD-2001`;
- broker singolo e replication factor 1;
- nessuna autenticazione API;
- CORS permissivo nel prototipo;
- DLQ completa non implementata;
- test distribuiti e di carico non inclusi;
- consumer Java non incluso come servizio definitivo nel Compose;
- nessuno Schema Registry;
- frontend privo di framework, intenzionalmente semplice;
- gestione dei segreti adatta allo sviluppo locale, non alla produzione.

Queste limitazioni non invalidano l'obiettivo didattico, ma devono essere dichiarate chiaramente.

## 17. Sviluppi futuri

- popolamento con molti ordini e stati differenti;
- consumer e API containerizzati;
- DLQ e retry topic;
- Schema Registry;
- Avro o Protobuf;
- autenticazione e autorizzazione;
- metriche Prometheus e dashboard Grafana;
- tracing distribuito;
- deployment Kubernetes o cloud;
- più broker e replica;
- retention e compaction consapevoli;
- timeline completa degli eventi nella UI;
- test di rebalance e fault injection.

## 18. Checklist finale del repository

Prima della consegna verificare:

- [ ] `README.md` principale con quick start;
- [ ] `.env.example` presente e senza segreti;
- [ ] `.env` escluso da Git;
- [ ] `docker-compose.yml` valido;
- [ ] migrazioni PostgreSQL incluse;
- [ ] scenario `successful-delivery-with-delay.json` incluso;
- [ ] `mvn clean package` verde;
- [ ] test Python verdi;
- [ ] `target/`, `__pycache__/` e file generati esclusi;
- [ ] dashboard inclusa nel JAR;
- [ ] comando canonico per popolare `ORD-2001` documentato;
- [ ] comando canonico per avviare API e dashboard documentato;
- [ ] porte locali documentate;
- [ ] limitazioni dichiarate;
- [ ] `git status` pulito;
- [ ] clone di prova in una cartella nuova eseguito almeno una volta.
