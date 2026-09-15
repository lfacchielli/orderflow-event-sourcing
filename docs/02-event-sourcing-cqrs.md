# Event Sourcing, Aggregati, Proiezioni e CQRS

## Indice

1. [Obiettivo del documento](#1-obiettivo-del-documento)
2. [Dal CRUD all'Event Sourcing](#2-dal-crud-allevent-sourcing)
3. [Evento, comando e stato](#3-evento-comando-e-stato)
4. [Aggregati e versionamento](#4-aggregati-e-versionamento)
5. [Funzione di proiezione](#5-funzione-di-proiezione)
6. [Replay e time travel](#6-replay-e-time-travel)
7. [Proiezioni e consistenza eventuale](#7-proiezioni-e-consistenza-eventuale)
8. [CQRS](#8-cqrs)
9. [Idempotenza, duplicati e version gap](#9-idempotenza-duplicati-e-version-gap)
10. [Evoluzione degli eventi](#10-evoluzione-degli-eventi)
11. [Applicazione in OrderFlow](#11-applicazione-in-orderflow)
12. [Vantaggi, costi e criteri di adozione](#12-vantaggi-costi-e-criteri-di-adozione)
13. [Domande utili per l'esame](#13-domande-utili-per-lesame)
14. [Riferimenti](#14-riferimenti)

## 1. Obiettivo del documento

L'Event Sourcing è un modello di persistenza in cui lo stato non viene considerato il dato primario. Il dato primario è la sequenza degli eventi che ha prodotto quello stato.

Martin Fowler sintetizza il pattern come la memorizzazione di tutte le modifiche allo stato applicativo sotto forma di sequenza di eventi. La cronologia può poi essere interrogata e usata per ricostruire stati passati.[^fowler-es]

Microsoft sottolinea che il pattern offre auditabilità e ricostruzione storica, ma introduce compromessi significativi nella gestione di concorrenza, query ed evoluzione degli schemi.[^microsoft-es]

## 2. Dal CRUD all'Event Sourcing

### Modello CRUD

Nel CRUD tradizionale viene conservato principalmente lo stato corrente:

```text
order_id: ORD-2001
status: DELIVERED
version: 10
```

Una modifica sovrascrive il valore precedente:

```text
PACKED -> IN_TRANSIT -> DELIVERED
                    stato precedente perso
```

Senza un audit log separato, non è possibile sapere come si sia arrivati allo stato finale.

### Modello Event Sourcing

Con Event Sourcing si conserva ogni fatto:

```text
v1  ORDER_CREATED
v2  ORDER_CONFIRMED
v3  PAYMENT_COMPLETED
v4  INVENTORY_RESERVED
v5  ORDER_PACKED
v6  SHIPMENT_STARTED
v7  HUB_REACHED
v8  DELIVERY_DELAYED
v9  OUT_FOR_DELIVERY
v10 ORDER_DELIVERED
```

Lo stato corrente è una funzione della cronologia:

```text
state_v10 = fold(apply, empty_state, events_v1_v10)
```

## 3. Evento, comando e stato

### Comando

Un comando esprime un'intenzione:

```text
DeliverOrder
CancelOrder
ReserveInventory
```

Il comando può essere rifiutato perché chiede che qualcosa avvenga.

### Evento

Un evento esprime un fatto già avvenuto:

```text
OrderDelivered
OrderCancelled
InventoryReserved
```

Per questo motivo, i nomi degli eventi sono generalmente al passato.

### Stato

Lo stato è la rappresentazione corrente derivata dagli eventi:

```text
OrderState
  status = DELIVERED
  version = 10
  currentHub = HUB-FIRENZE
  totalDelayMinutes = 35
```

```text
Command
   |
   v
Regole di dominio
   |
   v
Event
   |
   v
Projection
   |
   v
State
```

OrderFlow parte da eventi già generati e si concentra soprattutto sulla parte `Event -> Projection -> State`.

## 4. Aggregati e versionamento

Un aggregato è un confine di consistenza del dominio. Nell'esempio logistico l'aggregato è l'ordine.

```text
aggregateId = ORD-2001
```

Ogni aggregato possiede una propria sequenza versionata:

```text
ORD-2001: v1, v2, v3, ... v10
ORD-2002: v1, v2, v3
```

La versione permette di verificare la sequenza:

```text
versione corrente = 4
versione attesa    = 5
```

Casi anomali:

```text
evento v4 -> OLD_OR_DUPLICATE_EVENT
evento v6 -> VERSION_GAP
evento ORD-9000 -> AGGREGATE_MISMATCH
```

Il versionamento applicativo è importante anche quando il sistema di messaggistica mantiene l'ordine. Kafka garantisce l'ordine per partizione, ma la regola di dominio deve essere verificabile indipendentemente dall'infrastruttura.

## 5. Funzione di proiezione

Il cuore di un sistema event-sourced è una funzione deterministica:

```text
next_state = apply(current_state, event)
```

In Java:

```java
OrderState nextState = projector.apply(
    currentState,
    event
);
```

Proprietà desiderate:

- nessun accesso diretto a Kafka;
- nessun accesso diretto a PostgreSQL;
- nessun effetto collaterale;
- stesso input, stesso output;
- stato precedente immutabile;
- transizioni non valide rifiutate.

Esempio:

```text
OrderState(status=PACKED, version=5)
+
SHIPMENT_STARTED(version=6)
=
OrderState(status=IN_TRANSIT, version=6)
```

Una funzione pura è semplice da testare e può essere riutilizzata per:

- elaborazione live;
- replay completo;
- replay storico;
- ricostruzione dopo cancellazione della proiezione;
- replay da snapshot.

## 6. Replay e time travel

Il replay consiste nell'applicare nuovamente gli eventi nell'ordine dell'aggregato.

```text
empty state
    + v1
    + v2
    + v3
    ...
    + v10
    = stato finale
```

OrderFlow supporta tre forme concettuali:

### Replay completo

```java
replayService.replayAll(events);
```

### Replay fino a una versione

```java
replayService.replayToVersion(events, 5);
```

Risultato:

```text
PACKED v5
```

### Replay fino a un timestamp

```java
replayService.replayAtTime(
    events,
    Instant.parse("2026-08-29T13:00:00Z")
);
```

Il time travel è uno dei vantaggi più rappresentativi dell'Event Sourcing: non si osserva soltanto dove si trova il sistema, ma anche come ci è arrivato.[^fowler-es]

## 7. Proiezioni e consistenza eventuale

Una proiezione è un modello di lettura derivato dagli eventi. Può essere ricostruita perché non rappresenta la fonte primaria.

```text
Event stream
    |
    +-> Projection A: stato ordine
    +-> Projection B: dashboard ritardi
    +-> Projection C: report per hub
```

In OrderFlow, `orderflow.order_states` contiene una riga per ordine ed è ottimizzata per query e dashboard.

La proiezione può essere temporaneamente indietro rispetto al log eventi:

```text
Kafka ha ricevuto v10
consumer ha processato fino a v9
```

Questo è un esempio di **consistenza eventuale**. La proiezione converge allo stato corretto quando il consumer elabora gli eventi mancanti.

## 8. CQRS

CQRS significa **Command Query Responsibility Segregation**. Il principio consiste nel separare il modello usato per modificare il sistema da quello usato per leggerlo. Fowler evidenzia che la separazione può essere utile in domini complessi, ma aggiunge complessità e non è adatta a ogni sistema.[^fowler-cqrs]

```text
Write side                         Read side
Comandi                            Query HTTP
   |                                  |
   v                                  v
Regole di dominio                 PostgreSQL
   |                                  |
   v                                  v
Event store / Kafka              Dashboard
```

Event Sourcing e CQRS sono distinti:

- Event Sourcing stabilisce come memorizzare i cambiamenti;
- CQRS separa responsabilità di scrittura e lettura;
- possono essere usati insieme, ma uno non implica automaticamente l'altro.

OrderFlow usa una forma didattica di separazione:

- Kafka e gli eventi rappresentano il lato storico;
- PostgreSQL e le API rappresentano il lato di lettura.

## 9. Idempotenza, duplicati e version gap

### Idempotenza

Un'operazione idempotente può essere ripetuta senza modificare ulteriormente il risultato.

Nel progetto, `processed_events` conserva:

```text
event_id
order_id
aggregate_version
topic_name
partition_number
record_offset
```

Prima di processare:

```text
if eventId exists:
    non applicare nuovamente l'evento
```

### Duplicato applicativo

Lo stesso `eventId` viene ricevuto più volte.

```text
prima ricezione  -> processata
seconda ricezione -> duplicate
```

### Evento vecchio

```text
state v5 + event v4
```

### Version gap

```text
state v5 + event v7
```

Manca la versione 6, quindi il sistema non può ricostruire lo stato correttamente.

### Stato terminale

Dopo `DELIVERED`, `CANCELLED` o `DELIVERY_FAILED`, nuovi eventi di avanzamento possono essere rifiutati.

## 10. Evoluzione degli eventi

Gli eventi immutabili non dovrebbero essere modificati retroattivamente. Quando il contratto evolve, le strategie comuni includono:

- aggiunta di campi opzionali;
- versionamento esplicito dello schema;
- upcasting durante la lettura;
- nuovi tipi di evento;
- compatibilità tra producer e consumer;
- Schema Registry con Avro, Protobuf o JSON Schema.

Un consumer robusto non dovrebbe assumere che tutti gli eventi siano stati prodotti dalla stessa versione dell'applicazione.

## 11. Applicazione in OrderFlow

La cronologia `ORD-2001` produce:

```text
v1  CREATED
v2  CONFIRMED
v3  PAID
v4  INVENTORY_RESERVED
v5  PACKED
v6  IN_TRANSIT
v7  IN_TRANSIT, HUB-BOLOGNA
v8  IN_TRANSIT, delay=35
v9  OUT_FOR_DELIVERY
v10 DELIVERED
```

Stato finale:

```text
orderId:            ORD-2001
status:             DELIVERED
version:            10
currentHub:         HUB-FIRENZE
visitedHubs:        MODENA, BOLOGNA, FIRENZE
totalDelayMinutes:  35
```

La stessa cronologia permette di ottenere:

```text
stato v5  -> PACKED
stato v10 -> DELIVERED
```

## 12. Vantaggi, costi e criteri di adozione

### Vantaggi

- audit completo;
- ricostruzione storica;
- debug basato sui fatti;
- nuove proiezioni costruibili a posteriori;
- disaccoppiamento tra scrittura e lettura;
- integrazione naturale con sistemi event-driven.

### Costi

- maggiore complessità concettuale;
- evoluzione degli schemi;
- gestione dei duplicati;
- consistenza eventuale;
- replay costoso su stream lunghi;
- difficoltà nelle cancellazioni normative;
- necessità di osservabilità e strumenti operativi.

Microsoft raccomanda l'Event Sourcing quando auditabilità e ricostruzione storica giustificano la complessità, non come scelta predefinita per ogni componente.[^microsoft-es]

## 13. Domande utili per l'esame

### Qual è la fonte di verità?

La cronologia degli eventi. La tabella `order_states` è una proiezione derivata.

### Perché non aggiornare direttamente lo stato?

Per non perdere le informazioni sui cambiamenti che hanno prodotto lo stato.

### Il projector può accedere al database?

È preferibile di no. Un projector puro è più facile da testare e riusare.

### CQRS ed Event Sourcing sono la stessa cosa?

No. CQRS separa comandi e query; Event Sourcing conserva i cambiamenti come eventi.

### Come viene gestito un duplicato?

Con un identificatore evento già registrato e con controlli sulla versione dell'aggregato.

## 14. Riferimenti

[^fowler-es]: Martin Fowler, *Event Sourcing*, https://martinfowler.com/eaaDev/EventSourcing.html
[^fowler-cqrs]: Martin Fowler, *CQRS*, https://martinfowler.com/bliki/CQRS.html
[^microsoft-es]: Microsoft Azure Architecture Center, *Event Sourcing pattern*, https://learn.microsoft.com/it-it/azure/architecture/patterns/event-sourcing
