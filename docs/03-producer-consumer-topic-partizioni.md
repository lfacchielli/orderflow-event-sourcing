# Architettura Producer-Consumer, Topic, Partizioni e Affidabilità

## Indice

1. [Obiettivo del documento](#1-obiettivo-del-documento)
2. [Producer e consumer](#2-producer-e-consumer)
3. [Disaccoppiamento temporale e spaziale](#3-disaccoppiamento-temporale-e-spaziale)
4. [Topic e contratti](#4-topic-e-contratti)
5. [Partizionamento e scalabilità](#5-partizionamento-e-scalabilità)
6. [Consumer group e rebalance](#6-consumer-group-e-rebalance)
7. [Offset e semantiche di consegna](#7-offset-e-semantiche-di-consegna)
8. [Commit manuale e transazioni applicative](#8-commit-manuale-e-transazioni-applicative)
9. [Errori, retry e DLQ](#9-errori-retry-e-dlq)
10. [Backpressure e colli di bottiglia](#10-backpressure-e-colli-di-bottiglia)
11. [Applicazione in OrderFlow](#11-applicazione-in-orderflow)
12. [Checklist progettuale](#12-checklist-progettuale)
13. [Domande utili per l'esame](#13-domande-utili-per-lesame)
14. [Riferimenti](#14-riferimenti)

## 1. Obiettivo del documento

Questo documento analizza l'architettura producer-consumer con attenzione a Kafka. Il tema centrale è come distribuire eventi tra componenti indipendenti mantenendo ordinamento locale, affidabilità, scalabilità e possibilità di recupero.

## 2. Producer e consumer

Un **producer** crea e pubblica record. Un **consumer** legge e processa record.

```text
Producer                 Broker                 Consumer
   |                        |                       |
   |---- produce(event) --->|                       |
   |                        |<----- poll() ----------|
   |                        |------ records -------->|
```

Il producer non deve chiamare direttamente il consumer. Kafka funge da intermediario durevole.

Nel progetto:

```text
Producer  = generatore Python
Broker    = Apache Kafka
Consumer  = ricostruttore Java
```

## 3. Disaccoppiamento temporale e spaziale

### Disaccoppiamento spaziale

Il producer non conosce l'indirizzo del consumer. Conosce soltanto il cluster e il topic.

### Disaccoppiamento temporale

Producer e consumer non devono essere attivi nello stesso momento. Il producer può pubblicare, il consumer può processare più tardi.

```text
10:00 producer pubblica
10:05 consumer si avvia
10:05 consumer recupera gli eventi conservati
```

### Disaccoppiamento del ritmo

Il producer e il consumer possono avere velocità diverse, entro i limiti di capacità e retention.

```text
producer rate  > consumer rate
                 |
                 v
                lag
```

Il lag rappresenta la distanza tra l'ultimo record disponibile e la posizione del consumer.

## 4. Topic e contratti

Un topic definisce il canale logico, non il significato completo dei dati. Il contratto dell'evento deve specificare:

- nome e tipo dell'evento;
- identificatore univoco;
- chiave di aggregazione;
- versione dell'aggregato;
- timestamp;
- producer;
- correlation id;
- payload.

Esempio:

```json
{
  "eventId": "...",
  "eventType": "DELIVERY_DELAYED",
  "aggregateId": "ORD-2001",
  "aggregateVersion": 8,
  "occurredAt": "2026-08-29T13:00:00Z",
  "producerId": "hub-bologna-01",
  "producerType": "LOGISTICS_HUB",
  "correlationId": "CORR-2001",
  "payload": {
    "delayMinutes": 35
  }
}
```

La chiave Kafka e il corpo JSON hanno ruoli diversi:

```text
record key -> routing e partizionamento
record value -> contenuto dell'evento
```

## 5. Partizionamento e scalabilità

Le partizioni permettono la lettura parallela. Se un topic ha tre partizioni, un consumer group può avere al massimo tre consumer attivi che ricevono partizioni contemporaneamente.

```text
Topic con 3 partizioni

P0 -> Consumer A
P1 -> Consumer B
P2 -> Consumer C
```

Con cinque consumer:

```text
A -> P0
B -> P1
C -> P2
D -> idle
E -> idle
```

Le partizioni sono quindi sia una scelta di throughput sia una scelta semantica.

### Chiave troppo concentrata

Se molte chiavi finiscono nella stessa partizione, si crea una hot partition.

### Chiave casuale

Se gli eventi dello stesso aggregato ricevono chiavi diverse, l'ordine applicativo può essere compromesso.

### Chiave OrderFlow

```text
key = orderId
```

È una scelta naturale perché l'unità di ordinamento è l'ordine.

## 6. Consumer group e rebalance

Un consumer group è un insieme di consumer che collabora sullo stesso carico. I membri condividono lo stesso `group.id`. Ogni partizione è assegnata a un solo consumer del gruppo in un dato momento.[^consumer-design]

```text
Group: order-state-reconstructor

Consumer 1 -> P0, P2
Consumer 2 -> P1
```

Se un consumer entra o esce, Kafka ridistribuisce le partizioni. Questo processo è il **rebalance**.

Durante un rebalance, l'applicazione deve gestire correttamente:

- record già letti ma non completati;
- offset da committare;
- risorse associate alla partizione;
- elaborazioni lunghe;
- eventuali stati locali.

Il progetto didattico usa un singolo consumer, ma la configurazione tramite `group.id` prepara la scalabilità orizzontale.

## 7. Offset e semantiche di consegna

### At-most-once

L'offset viene committato prima del processamento.

```text
commit offset
process record
crash
```

Il record può andare perso dal punto di vista applicativo.

### At-least-once

Il record viene processato prima del commit.

```text
process record
commit offset
```

Se avviene un crash tra i due passaggi, il record può essere riletto. Servono quindi operazioni idempotenti.

### Exactly-once

Exactly-once end-to-end richiede coordinamento tra il log, gli effetti applicativi e gli altri sistemi. Non si ottiene semplicemente attivando una proprietà del consumer.

OrderFlow adotta una strategia pratica at-least-once con deduplicazione applicativa:

```text
process record
save eventId
save projection
commit database
commit Kafka offset
```

## 8. Commit manuale e transazioni applicative

Kafka permette commit automatico o manuale. Con il commit manuale il programma decide quando un record è considerato completato.[^kafka-offsets]

Nel progetto:

```text
BEGIN PostgreSQL
    controlla processed_events
    legge order_states
    applica projector
    salva order_states
    salva eventuale snapshot
    registra processed_events
COMMIT PostgreSQL
COMMIT Kafka offset
```

```text
Kafka record
    |
    v
PostgreSQL transaction
    | success
    v
Kafka commit
```

Questa sequenza evita di committare un offset prima della persistenza dello stato.

Rimane una finestra di errore:

```text
COMMIT database riuscito
crash prima del COMMIT Kafka
```

Al riavvio il record viene riletto. `processed_events` riconosce l'`eventId` e rende la ripetizione innocua.

PostgreSQL definisce una transazione come un'operazione all-or-nothing: o tutti i passaggi diventano visibili, oppure nessuno.[^postgres-transactions]

## 9. Errori, retry e DLQ

Gli errori possono essere classificati.

### Transitori

- database temporaneamente non disponibile;
- timeout di rete;
- broker non raggiungibile.

Strategia: retry con backoff.

### Permanenti

- JSON non valido;
- tipo evento sconosciuto;
- chiave Kafka diversa dall'aggregate id;
- transizione di dominio non valida.

Strategia: Dead Letter Queue, alert e analisi.

```text
record
  |
  +-> valido -> process -> commit
  |
  +-> transitorio -> retry
  |
  +-> permanente -> DLQ -> commit sorgente
```

OrderFlow implementa la classificazione applicativa di diversi errori, ma una DLQ completa è indicata come sviluppo futuro.

## 10. Backpressure e colli di bottiglia

Kafka usa un modello pull: il consumer richiede record con `poll()`. Questo consente al consumer di controllare il ritmo e recuperare il lag tramite batch.[^consumer-design]

Possibili colli di bottiglia:

- una partizione molto più carica delle altre;
- query PostgreSQL lente;
- serializzazione JSON costosa;
- proiezione con logica complessa;
- transazioni troppo lunghe;
- `max.poll.interval.ms` incompatibile con il tempo di elaborazione;
- snapshot troppo frequenti.

Strategie:

- aumentare partizioni e consumer;
- usare batch ragionevoli;
- indicizzare le query;
- mantenere il projector puro e veloce;
- usare snapshot per replay lunghi;
- monitorare consumer lag e tempi di commit.

## 11. Applicazione in OrderFlow

### Producer

Il generatore Python produce eventi logistici deterministici.

### Topic

```text
order-events
```

Topic aggiuntivo predisposto:

```text
order-events-dashboard
```

### Consumer

Il consumer Java usa:

```text
enable.auto.commit = false
auto.offset.reset = earliest
allow.auto.create.topics = false
key/value deserializer = StringDeserializer
```

### Validazione

```java
if (!record.key().equals(event.aggregateId())) {
    throw new KafkaRecordValidationException(...);
}
```

### Elaborazione

```java
TransactionalProcessingResult result = processor.process(
    event,
    new KafkaRecordMetadata(
        record.topic(),
        record.partition(),
        record.offset()
    )
);
```

## 12. Checklist progettuale

Prima di mettere in produzione una pipeline producer-consumer, verificare:

- [ ] contratto dell'evento versionato;
- [ ] chiave di partizionamento motivata;
- [ ] numero partizioni coerente con il parallelismo;
- [ ] replication factor adeguato;
- [ ] producer con retry e delivery report;
- [ ] commit del consumer dopo gli effetti applicativi;
- [ ] deduplicazione o idempotenza;
- [ ] gestione dei record non validi;
- [ ] DLQ;
- [ ] metriche di lag;
- [ ] timeout e backoff;
- [ ] test di riavvio e rebalance;
- [ ] policy di retention;
- [ ] gestione dell'evoluzione dello schema.

## 13. Domande utili per l'esame

### Perché un consumer group scala al massimo fino al numero di partizioni?

Perché una singola partizione è assegnata a un solo consumer del gruppo per volta.

### Perché il commit manuale non garantisce exactly-once?

Perché il commit Kafka e la transazione su un database esterno non formano automaticamente una singola transazione distribuita.

### Come si gestisce la rilettura dopo un crash?

Con semantica at-least-once e deduplicazione tramite `eventId`.

### Cosa succede se il producer pubblica senza chiave?

Il routing può distribuire record correlati su partizioni diverse, perdendo l'ordinamento per aggregato.

## 14. Riferimenti

[^consumer-design]: Confluent Documentation, *Kafka Consumer Design*, https://docs.confluent.io/kafka/design/consumer-design.html
[^kafka-offsets]: Apache Kafka, *Distribution and Consumer Offset Tracking*, https://kafka.apache.org/43/implementation/distribution/
[^postgres-transactions]: PostgreSQL Documentation, *Transactions*, https://www.postgresql.org/docs/current/tutorial-transactions.html
