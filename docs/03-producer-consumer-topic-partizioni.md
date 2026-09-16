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
12. [Riferimenti](#12-riferimenti)

## 1. Obiettivo del documento

Questo documento analizza l'architettura producer-consumer con attenzione a Kafka. Il tema centrale è capire come sia possibile distribuire eventi tra componenti indipendenti, mantenendo inoltre ordinamento locale, affidabilità, scalabilità e possibilità di recupero.

## 2. Producer e consumer

Identifichiamo con **producer** colui che crea e pubblica record, a differenza di un **consumer** che invece legge e processa record.

```text
Producer                 Broker                 Consumer
   |                        |                       |
   |---- produce(event) --->|                       |
   |                        |<----- poll() ----------|
   |                        |------ records -------->|
```

Questo permette al producer di non dover chiamare direttamente il consumer (o viceversa), proprio perché Kafka funge da intermediario durevole.

Nel progetto:

```text
Producer  = generatore Python
Broker    = Apache Kafka
Consumer  = ricostruttore Java
```

## 3. Disaccoppiamento temporale e spaziale

### Disaccoppiamento spaziale

Uno dei veri vantaggi di questa tipologia di architettura è quello di poter disaccoppiare le due parti, permettendo quindi al producer di non conoscere l'indirizzo del consumer, bensì avere conoscenza solo del cluster e del topic su cui deve scrivere. Si capisce immediatamente come questa ideologia renda altamente scalabile l'architettura, proprio perché più producer possono pubblicare loro informazioni su un topic e non doversi più preoccupare di quello che accadrà da quel momento in poi.

### Disaccoppiamento temporale

Per quanto riguarda invece il disaccoppiamento temporale, producer e consumer possono attivarsi in momenti diversi, permettendo di conseguenza lavori asincroni tra le parti che ne riducono sicuramente i vincoli di coinvolgimento. Ad esempio:

```text
10:00 producer pubblica
10:05 consumer si avvia
10:05 consumer recupera gli eventi conservati
```

### Disaccoppiamento del ritmo
Non per ultimo, possiamo citare anche il disaccoppiamento del ritmo, ovvero producer e consumer possono avere anche velocità differenti di operatività, ma questo non impatta sulle prestazioni generali proprio perché sono disallineati dalla partenza.

```text
producer rate  > consumer rate
                 |
                 v
                lag
```


## 4. Topic e contratti

Un topic definisce il canale logico, non il significato completo dei dati. Il contratto dell'evento deve poi essere in grado di specificare i seguenti elementi:

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

**N.B.** Se molte chiavi finiscono nella stessa partizione, si crea una hot partition.

**N.B.** Se gli eventi dello stesso aggregato ricevono chiavi diverse, l'ordine applicativo può essere compromesso.

## 6. Consumer group e rebalance

Un consumer group è un insieme di consumer che collabora sullo stesso carico. I membri condividono lo stesso `group.id` e pertanto ogni partizione può essere assegnata ad un solo consumer del gruppo in un dato momento.

```text
Group: order-state-reconstructor

Consumer 1 -> P0, P2
Consumer 2 -> P1
```

Se un consumer entra o esce, Kafka ridistribuisce le partizioni in modo automatico, defininendo il concetto chiave di **rebalance**.

Durante tale attività, l'applicazione deve essere in grado di gestire correttamente:

- record già letti ma non completati;
- offset da committare;
- risorse associate alla partizione;
- elaborazioni lunghe;
- eventuali stati locali.

Il progetto didattico usa un singolo consumer, ma la configurazione tramite `group.id` prepara la scalabilità orizzontale.

## 7. Offset e semantiche di consegna

Il momento in cui il consumer salva l’offset determina il comportamento del sistema in caso di errore. In particolare, cambia il rapporto tra il processamento del record e la possibilità che lo stesso record venga perso oppure elaborato più di una volta.

### At-most-once

Nella modalità **at-most-once**, il consumer esegue il commit dell’offset prima di processare il record.

```text
ricezione record
commit offset
processamento record
```

Questa strategia evita normalmente che uno stesso record venga elaborato più volte. Tuttavia, se l’applicazione si arresta dopo il commit ma prima di completare il processamento, Kafka considera comunque il record già consumato.

```text
commit offset completato
processamento iniziato
crash
```

Al successivo avvio, il consumer ripartirà dall’offset seguente. Dal punto di vista applicativo, quindi, il record potrebbe andare perso.

### At-least-once

Nella modalità **at-least-once**, il consumer processa prima il record e salva l’offset soltanto dopo aver completato con successo le operazioni applicative.

```text
ricezione record
processamento record
commit offset
```

Questa strategia riduce il rischio di perdere eventi. Rimane però una finestra temporale in cui il processamento può essere completato, mentre il commit dell’offset non è ancora avvenuto.

```text
processamento completato
crash prima del commit offset
```

Quando il consumer viene riavviato, Kafka consegna nuovamente il record. Per questo motivo, un sistema at-least-once deve progettare le operazioni applicative in modo idempotente, affinché l’elaborazione ripetuta dello stesso evento non produca effetti duplicati.

### Exactly-once

La semantica **exactly-once** richiede che ogni record produca i propri effetti una sola volta, anche in presenza di errori, retry e riavvii. Ottenere questo comportamento lungo l’intera pipeline richiede un coordinamento tra Kafka, il processamento applicativo e gli eventuali sistemi esterni, come un database.

Non è quindi sufficiente abilitare una singola proprietà del consumer. Se Kafka e PostgreSQL partecipano alla stessa elaborazione, bisogna considerare anche cosa accade quando uno dei due sistemi completa l’operazione e l’altro no.

OrderFlow adotta una soluzione pratica basata sulla semantica at-least-once e sulla deduplicazione applicativa:

```text
process record
save eventId
save projection
commit database
commit Kafka offset
```

In questo modo un record può essere ricevuto nuovamente, ma la presenza dell’`eventId` nella tabella degli eventi processati impedisce che venga applicato due volte.

## 8. Commit manuale e transazioni applicative

Kafka può gestire il commit degli offset automaticamente oppure lasciare questa responsabilità all’applicazione. Con il commit manuale, il programma decide quando un record può essere considerato completato.[^kafka-offsets]

Nel progetto OrderFlow, il commit automatico è disabilitato perché non vogliamo avanzare l’offset prima che lo stato dell’ordine sia stato salvato correttamente. La sequenza di elaborazione è la seguente:

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

La relazione tra Kafka e PostgreSQL può essere rappresentata così:

```text
Kafka record
    |
    v
PostgreSQL transaction
    |
    | success
    v
Kafka offset commit
```

Questa sequenza garantisce che l’offset non venga committato prima della persistenza della proiezione. Se la transazione PostgreSQL fallisce, il commit Kafka non viene eseguito e il record potrà essere ricevuto nuovamente.

Rimane comunque una possibile finestra di errore:

```text
COMMIT database riuscito
crash prima del COMMIT Kafka
```

In questo caso la proiezione è già stata aggiornata, ma Kafka non ne è ancora a conoscenza. Al riavvio, il consumer rilegge lo stesso record. La tabella `processed_events` riconosce però l’`eventId` già elaborato e rende innocua la ripetizione.

PostgreSQL gestisce inoltre come un’unica transazione il salvataggio dello stato, dell’eventuale snapshot e dell’evento processato. Questo significa che tutte le operazioni diventano visibili insieme oppure vengono annullate insieme.[^postgres-transactions]

## 9. Errori, retry e DLQ

Non tutti gli errori devono essere gestiti nello stesso modo. Una distinzione utile è quella tra errori transitori, che potrebbero risolversi con un nuovo tentativo, ed errori permanenti, che richiedono invece un intervento diverso.

### Errori transitori

Un errore è considerato transitorio quando la causa potrebbe scomparire dopo un breve intervallo. Alcuni esempi sono:

- database temporaneamente non disponibile;
- timeout durante una comunicazione di rete;
- broker Kafka momentaneamente non raggiungibile;
- limite temporaneo delle risorse del sistema.

In questi casi è ragionevole ripetere l’operazione. Generalmente si utilizza un retry con backoff, aumentando progressivamente il tempo di attesa tra un tentativo e il successivo. In questo modo si evita di sovraccaricare ulteriormente un servizio già in difficoltà.

### Errori permanenti

Un errore permanente non viene normalmente risolto ripetendo lo stesso processamento. Alcuni esempi sono:

- documento JSON non valido;
- tipo di evento non riconosciuto;
- chiave Kafka diversa dall’`aggregateId`;
- versione dell’aggregato non coerente;
- transizione di dominio non consentita.

Continuare a effettuare retry su questi record potrebbe bloccare la partizione e impedire l’elaborazione degli eventi successivi. Una strategia comune consiste quindi nell’inviare il record problematico verso una **Dead Letter Queue**, accompagnandolo con le informazioni necessarie per analizzare la causa dell’errore.

```text
record ricevuto
  |
  +-> valido
  |      |
  |      v
  |   process -> commit
  |
  +-> errore transitorio
  |      |
  |      v
  |    retry con backoff
  |
  +-> errore permanente
         |
         v
       DLQ -> analisi -> eventuale recupero
```

OrderFlow implementa già diversi controlli applicativi, tra cui la validazione della chiave Kafka, dell’identificatore dell’aggregato e della sequenza delle versioni. Una gestione completa tramite DLQ non è stata aggiunta al prototipo, ma rappresenta uno dei principali sviluppi futuri.

## 10. Backpressure e colli di bottiglia

Kafka utilizza un modello pull: è il consumer che richiede nuovi record attraverso il metodo `poll()`. Questo approccio permette all’applicazione di controllare il proprio ritmo di lettura e di elaborare i record in batch.[^consumer-design]

Se il producer pubblica più velocemente di quanto il consumer riesca a processare, aumenta il consumer lag, cioè la distanza tra gli ultimi record disponibili e quelli già elaborati.

```text
producer veloce
      |
      v
Kafka accumula record
      |
      v
consumer recupera progressivamente il lag
```

Un rallentamento può dipendere da diversi fattori:

- una partizione molto più carica rispetto alle altre;
- query PostgreSQL lente o prive di indici adeguati;
- serializzazione e deserializzazione JSON costose;
- logica di proiezione troppo complessa;
- transazioni applicative troppo lunghe;
- valore di `max.poll.interval.ms` non compatibile con il tempo di elaborazione;
- creazione degli snapshot troppo frequente.

Non esiste una soluzione unica per ogni collo di bottiglia. In base alla causa, si può aumentare il numero di partizioni e consumer, utilizzare batch di dimensione ragionevole, ottimizzare le query, mantenere il projector semplice oppure ridurre il costo del replay tramite snapshot.

È inoltre importante monitorare il consumer lag, la durata delle transazioni e il tempo necessario per committare gli offset. Senza queste metriche, un rallentamento può rimanere invisibile fino a quando il ritardo accumulato diventa significativo.

## 11. Applicazione in OrderFlow

Nel progetto OrderFlow, il generatore Python svolge il ruolo di producer. Il generatore crea scenari logistici deterministici e converte le operazioni di un ordine in una sequenza di eventi JSON versionati.

Gli eventi principali vengono pubblicati nel topic:

```text
order-events
```

È stato inoltre predisposto un topic separato per eventuali prove di popolamento della dashboard:

```text
order-events-dashboard
```

Il consumer Java è configurato per controllare esplicitamente il processamento dei record. Le proprietà principali sono:

```text
enable.auto.commit = false
auto.offset.reset = earliest
allow.auto.create.topics = false
key/value deserializer = StringDeserializer
```

La disattivazione del commit automatico permette di salvare l’offset soltanto dopo il completamento della transazione PostgreSQL. L’opzione `earliest` consente invece di partire dall’inizio quando il consumer group non possiede ancora offset committati.

Prima di elaborare un record, il consumer verifica anche che la chiave Kafka corrisponda all’identificatore dell’aggregato contenuto nell’evento:

```java
if (!record.key().equals(event.aggregateId())) {
    throw new KafkaRecordValidationException(...);
}
```

Questo controllo serve a garantire che la chiave utilizzata per il partizionamento sia coerente con l’ordine descritto nel messaggio.

Dopo la deserializzazione e la validazione, l’evento viene affidato al processor transazionale insieme ai metadati Kafka:

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

Il processor verifica i duplicati, recupera lo stato precedente, applica il projector, aggiorna la proiezione, crea un eventuale snapshot e registra l’evento processato. Soltanto dopo il successo di queste operazioni il consumer può committare l’offset Kafka.


## 12. Riferimenti

Confluent Documentation, *Kafka Consumer Design*, https://docs.confluent.io/kafka/design/consumer-design.html

Apache Kafka, *Distribution and Consumer Offset Tracking*, https://kafka.apache.org/43/implementation/distribution/

PostgreSQL Documentation, *Transactions*, https://www.postgresql.org/docs/current/tutorial-transactions.html
