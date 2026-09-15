# Indice della raccolta

Questa raccolta comprende cinque documenti complementari, ciascuno di essi creato con l'intento di spiegare al meglio i contenuti teorici impiegati nello sviluppo del progetto. Tali documenti conterranno in particolare:

1. **01-kafka-event-streaming.md**: Kafka, log distribuito, topic, partizioni, offset e replay.
2. **02-event-sourcing-cqrs.md**: Event Sourcing, aggregati, proiezioni, CQRS e consistenza eventuale.
3. **03-producer-consumer-topic-partizioni.md**: architettura producer-consumer, consumer group, commit e affidabilità.
4. **04-replay-snapshot-idempotenza.md**: replay, snapshot, colli di bottiglia, transazioni e idempotenza.
5. **05-architettura-orderflow.md**: applicazione pratica dei concetti, architettura OrderFlow, avvio, demo e riproducibilità.

Nel corso dei documenti, il nostro progetto pratico verrà spesso chiamato con il nome OrderFlow.

L'ordine di lettura consigliato per una migliore comprensione è il seguente:

```text
Kafka e streaming
        |
        v
Event Sourcing e CQRS
        |
        v
Producer e consumer
        |
        v
Replay e snapshot
        |
        v
Architettura OrderFlow
```

---

# Apache Kafka ed Event Streaming

## Indice

1. [Obiettivo del documento](#1-obiettivo-del-documento)
2. [Che cos'è l'event streaming](#2-che-cosè-levent-streaming)
3. [Kafka come log distribuito](#3-kafka-come-log-distribuito)
4. [Broker, cluster e replica](#4-broker-cluster-e-replica)
5. [Topic, partizioni e offset](#5-topic-partizioni-e-offset)
6. [Ordinamento e chiavi](#6-ordinamento-e-chiavi)
7. [Persistenza, retention e replay](#7-persistenza-retention-e-replay)
8. [Kafka e sistemi tradizionali](#8-kafka-e-sistemi-tradizionali)
9. [Collegamento con OrderFlow](#9-collegamento-con-orderflow)
10. [Limiti e considerazioni progettuali](#10-limiti-e-considerazioni-progettuali)
11. [Domande utili per l'esame](#11-domande-utili-per-lesame)
12. [Riferimenti](#12-riferimenti)

## 1. Obiettivo del documento

Questo documento introduce i concetti fondamentali di **Apache Kafka** e dell'**event streaming**. L'obiettivo non è descrivere soltanto OrderFlow, ma fornire una base teorica in grado di far comprendere sistemi distribuiti che acquisiscono, conservano e processano flussi di eventi.

OrderFlow verrà poi utilizzato come esempio concreto, dove gli eventi logistici di un ordine vengono pubblicati da un producer Python, conservati in Kafka e consumati da un'applicazione Java in grado di ricostruire lo stato dell'ordine.

## 2. Che cos'è l'event streaming

Un evento è la rappresentazione di un fatto già accaduto. Alcuni esempi possono essere:

- un ordine è stato creato;
- un pagamento è stato completato;
- una spedizione ha raggiunto un hub;
- una consegna ha subito un ritardo;
- un ordine è stato consegnato.

L'event streaming consiste nel catturare questi fatti mentre avvengono, conservarli in modo durevole e renderli disponibili ad uno o più consumatori. Kafka definisce l'event streaming come la pratica di acquisire dati in tempo reale da sorgenti diverse, conservarli per un utilizzo successivo, elaborarli ed instradarli verso sistemi differenti.

```text
Sorgenti di eventi
        |
        v
  Stream durevole
        |
        +------------------+
        |                  |
        v                  v
Elaborazione live      Elaborazione storica
```

Il valore principale di questa ideologia non è soltanto la velocità, bensì la possibilità di disaccoppiare chi produce un fatto da chi lo utilizza.

## 3. Kafka come log distribuito

Kafka può essere di fatto considerato come un **log append-only distribuito**. Un producer aggiunge record in coda al log. Un consumer legge il log mantenendo una propria posizione.

```text
inizio del log                                  fine del log
    |                                                |
    v                                                v
 [evento 0] [evento 1] [evento 2] [evento 3] [evento 4]
                                      ^
                                      posizione consumer
```

La lettura non elimina il record. Consumer diversi possono leggere la stessa cronologia in momenti e con velocità differenti. Questa caratteristica rende Kafka adatto a:

- pipeline dati;
- architetture event-driven;
- sistemi di audit;
- sincronizzazione tra servizi;
- ricostruzione di proiezioni;
- elaborazioni in tempo reale e batch.

Kafka combina pubblicazione, sottoscrizione, memorizzazione durevole e processamento degli eventi in una piattaforma distribuita.[^kafka-intro]

## 4. Broker, cluster e replica

Un **broker** è un processo Kafka che conserva partizioni e risponde alle richieste dei client. Più broker formano un cluster.

```text
                   Kafka cluster
        +----------------+----------------+
        |                |                |
        v                v                v
    Broker 1         Broker 2         Broker 3
    P0 leader        P1 leader        P2 leader
    P1 replica       P2 replica       P0 replica
```

In un ambiente di produzione, le partizioni possono essere replicate. Una replica leader gestisce letture e scritture, mentre le repliche follower mantengono copie dei dati. La replica migliora la disponibilità in caso di guasto di un broker.

Nel progetto didattico OrderFlow viene usato un singolo broker e un fattore di replica pari a 1. Questa configurazione è sufficiente per dimostrare i concetti, ma non offre tolleranza al guasto del broker.

## 5. Topic, partizioni e offset

### Topic

Un **topic** è un flusso denominato di record. Producers e consumers non devono conoscersi direttamente: condividono il nome del topic.

Esempio OrderFlow:

```text
order-events
```

### Partizione

Un topic è diviso in partizioni. Ogni partizione è un log ordinato indipendente.

```text
Topic: order-events

Partition 0: [e0] [e1] [e2]
Partition 1: [e0] [e1]
Partition 2: [e0] [e1] [e2] [e3]
```

Le partizioni permettono di distribuire archiviazione e lavoro. Sono quindi l'unità fondamentale di parallelismo.

### Offset

L'offset è la posizione di un record **all'interno di una partizione**. Non esiste un offset globale valido per tutto il topic.

Un'identità Kafka completa è quindi:

```text
topic + partition + offset
```

Esempio:

```text
order-events, partition 2, offset 37
```

Kafka conserva per ogni consumer group l'offset committato. Il consumer può effettuare commit automatici oppure manuali. In caso di riavvio, il consumer riprende dalla posizione committata.[^kafka-distribution]

## 6. Ordinamento e chiavi

Kafka garantisce l'ordinamento dei record **dentro una singola partizione**, non tra partizioni diverse.

```text
Partition 0: A1 -> A2 -> A3      ordine garantito
Partition 1: B1 -> B2 -> B3      ordine garantito

A2 rispetto a B2                 nessun ordine globale garantito
```

Per mantenere insieme gli eventi dello stesso aggregato, il producer usa una chiave stabile. Il partizionatore calcola la partizione a partire dalla chiave.

In OrderFlow:

```text
Kafka key = orderId
```

```text
ORD-2001 -> hash -> Partition 1

ORDER_CREATED      -> Partition 1
ORDER_CONFIRMED    -> Partition 1
PAYMENT_COMPLETED  -> Partition 1
ORDER_DELIVERED    -> Partition 1
```

Questa scelta consente al consumer di osservare gli eventi di uno stesso ordine nella sequenza prevista. Il campo `aggregateVersion` aggiunge un ulteriore controllo applicativo: anche se un evento arrivasse fuori sequenza, il projector potrebbe rilevare il version gap.

## 7. Persistenza, retention e replay

Kafka conserva i record in base a una politica di retention. Il record non viene eliminato semplicemente perché un consumer lo ha letto.

Questa separazione permette il replay:

```text
Consumer live
    legge dagli offset committati

Consumer di replay
    assegna le partizioni
    esegue seek all'inizio
    rilegge la cronologia
```

Nel progetto OrderFlow è stato implementato un reader storico che:

1. individua le partizioni del topic;
2. acquisisce gli end offset iniziali;
3. esegue `assign` e `seekToBeginning`;
4. filtra i record per `orderId`;
5. ordina gli eventi per `aggregateVersion`;
6. ricostruisce lo stato senza committare offset.

```text
Kafka order-events
        |
        v
KafkaOrderHistoryReader
        |
        v
OrderReplayService
        |
        v
OrderState storico o finale
```

## 8. Kafka e sistemi tradizionali

Kafka non sostituisce automaticamente un database relazionale. I due strumenti rispondono a esigenze differenti.

### Database relazionale

- query ad hoc efficienti;
- vincoli e transazioni;
- stato corrente facilmente leggibile;
- aggiornamenti e join.

### Kafka

- cronologia append-only;
- disaccoppiamento producer-consumer;
- elaborazione asincrona;
- throughput elevato;
- replay degli eventi.

In OrderFlow, Kafka rappresenta la cronologia degli eventi, mentre PostgreSQL conserva proiezioni ottimizzate per la lettura della dashboard.

```text
Kafka                           PostgreSQL
cronologia degli eventi         stato corrente
append-only                     query SQL
replay                          dashboard
ordinamento per partizione      viste materializzate applicative
```

## 9. Collegamento con OrderFlow

Un record OrderFlow ha concettualmente questa forma:

```json
{
  "eventId": "uuid",
  "eventType": "ORDER_CREATED",
  "aggregateId": "ORD-2001",
  "aggregateVersion": 1,
  "occurredAt": "2026-08-29T10:00:00Z",
  "producerId": "ecommerce-node-01",
  "producerType": "ECOMMERCE",
  "correlationId": "CORR-2001",
  "payload": {}
}
```

Il producer usa `ORD-2001` anche come chiave Kafka. Il consumer Java verifica che:

```text
record.key == event.aggregateId
```

Dopo la validazione, l'evento viene applicato allo stato precedente:

```java
OrderState nextState = projector.apply(
    currentState,
    event
);
```

La posizione Kafka viene committata soltanto dopo l'elaborazione applicativa.

## 10. Limiti e considerazioni progettuali

Kafka introduce vantaggi ma anche complessità:

- occorre stabilire una chiave di partizionamento corretta;
- il numero di partizioni limita il parallelismo massimo di un consumer group;
- possono avvenire rebalance quando cambiano i membri del gruppo;
- la gestione degli offset influenza duplicati e perdita di elaborazioni;
- i contratti degli eventi devono evolvere in modo compatibile;
- un record errato può bloccare una partizione se non esiste una strategia DLQ;
- un singolo broker con replication factor 1 non è adatto a produzione.

La documentazione ufficiale specifica che `group.id` identifica il consumer group e che le proprietà di deserializzazione, bootstrap e offset reset sono centrali nella configurazione del consumer.[^consumer-config]

## 11. Domande utili per l'esame

### Perché la chiave Kafka è `orderId`?

Perché tutti gli eventi dello stesso ordine devono raggiungere la stessa partizione e mantenere un ordine relativo coerente.

### Kafka garantisce l'ordine globale?

No. Garantisce l'ordine dentro la singola partizione.

### Che differenza c'è tra offset corrente e offset committato?

La posizione corrente avanza durante la lettura. L'offset committato è la posizione durevole da cui il consumer riprende dopo un riavvio.

### Perché usare il commit manuale?

Per committare soltanto dopo che lo stato è stato elaborato e persistito con successo.

### Perché Kafka e PostgreSQL sono entrambi presenti?

Kafka conserva e distribuisce la cronologia; PostgreSQL espone una proiezione interrogabile velocemente.

## 12. Riferimenti

[^kafka-intro]: Apache Kafka, *Introduction*, https://kafka.apache.org/intro/
[^kafka-distribution]: Apache Kafka, *Distribution and Consumer Offset Tracking*, https://kafka.apache.org/43/implementation/distribution/
[^consumer-config]: Apache Kafka, *Consumer Configs*, https://kafka.apache.org/41/configuration/consumer-configs/
