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
12. [Limitazioni note](#12-limitazioni-note)
13. [Sviluppi futuri](#13-sviluppi-futuri)

## 1. Scopo del progetto

OrderFlow è un progetto didattico applicato al dominio della logistica. L’obiettivo principale è dimostrare che lo stato corrente di un ordine non deve necessariamente essere memorizzato come unico dato autorevole, ma può essere ricostruito a partire da una sequenza immutabile e versionata di eventi.

In un sistema tradizionale, lo stato di un ordine viene aggiornato direttamente nel database. Il valore precedente viene quindi sostituito da quello nuovo. Con l’Event Sourcing, invece, ogni cambiamento viene rappresentato come un fatto avvenuto nel dominio.

```text
ORDER_CREATED
ORDER_CONFIRMED
PAYMENT_COMPLETED
ORDER_PACKED
SHIPMENT_STARTED
ORDER_DELIVERED
```

La sequenza degli eventi descrive l’intera storia dell’ordine. Lo stato corrente viene ottenuto applicando questi eventi in ordine.

Il progetto utilizza questo principio per mostrare concretamente diversi aspetti tipici di un’architettura event-driven:

- produzione e trasporto degli eventi tramite Kafka;
- partizionamento degli eventi in base all’ordine;
- validazione della sequenza e delle versioni;
- ricostruzione deterministica dello stato;
- persistenza delle proiezioni in PostgreSQL;
- gestione idempotente degli eventi duplicati;
- replay completo, storico e temporale;
- creazione periodica di snapshot;
- esposizione dei dati tramite API HTTP;
- visualizzazione mediante una dashboard HTML, CSS e JavaScript.

OrderFlow non vuole rappresentare un sistema completo pronto per la produzione. Lo scopo è rendere visibili e verificabili i concetti principali dell’Event Sourcing, mostrando come i diversi componenti collaborano all’interno della stessa pipeline.

## 2. Risultato raggiunto

Lo scenario principale del progetto riguarda l’ordine `ORD-2001`. La cronologia contiene dieci eventi che descrivono l’intero ciclo di vita dell’ordine, dalla creazione fino alla consegna.

Al termine della ricostruzione, il sistema produce il seguente stato:

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

Lo stato mostra non soltanto che l’ordine è stato consegnato, ma anche alcune informazioni accumulate durante la sua evoluzione. Sono presenti, per esempio, gli hub attraversati e il ritardo totale registrato durante la spedizione.

Il sistema crea inoltre due snapshot:

```text
v5  PACKED
v10 DELIVERED
```

Lo snapshot alla versione 5 rappresenta l’ordine quando era stato preparato ma non ancora spedito. Lo snapshot alla versione 10 rappresenta invece lo stato finale.

La dashboard consente di visualizzare la proiezione corrente e di aprire gli snapshot disponibili. In questo modo è possibile passare dallo stato `DELIVERED v10` allo stato storico `PACKED v5`, per poi tornare alla situazione corrente.

## 3. Architettura generale

L’architettura di OrderFlow è composta da una pipeline che parte dal generatore Python e termina nella dashboard web.

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

Il generatore Python crea eventi logistici sotto forma di documenti JSON. Ogni evento viene pubblicato su Kafka utilizzando `orderId` come chiave, in modo che tutti gli eventi appartenenti allo stesso ordine raggiungano la stessa partizione.

Il consumer Java legge i record, deserializza il JSON, verifica la chiave, controlla la versione e applica il projector. Il nuovo stato viene poi salvato in PostgreSQL attraverso una transazione JDBC.

Il database non rappresenta la cronologia autorevole. Contiene invece dati derivati, ottimizzati per essere interrogati velocemente dalle API e dalla dashboard.

L’applicazione Java espone infine sia gli endpoint HTTP sia i file statici del frontend. Di conseguenza, API e interfaccia vengono servite dallo stesso processo e dalla stessa porta.

## 4. Struttura logica dei componenti

### Generatore Python

Il generatore Python simula diversi sistemi che producono eventi relativi a un ordine, come un servizio e-commerce, un sistema di pagamento oppure un nodo logistico.

Le sue responsabilità principali sono:

- creare eventi sintetici ma coerenti;
- utilizzare timestamp in formato UTC;
- incrementare progressivamente la versione dell’aggregato;
- serializzare gli eventi in JSON;
- preparare scenari deterministici;
- pubblicare i record su Kafka;
- verificare il comportamento tramite test automatici.

I componenti `kafka/publisher.py` e `publish_main.py` sono predisposti per la pubblicazione, mentre i generatori di scenario producono sequenze utilizzabili anche localmente nei test e nei replay.

### Apache Kafka

Kafka svolge il ruolo di log distribuito e intermediario tra il producer Python e il consumer Java.

Kafka permette ai due componenti di rimanere disaccoppiati. Il producer non deve conoscere direttamente il consumer e il consumer può iniziare a elaborare gli eventi anche dopo la loro pubblicazione.

Le responsabilità principali di Kafka nel progetto sono:

- conservare il flusso degli eventi;
- distribuire i record tra le partizioni;
- mantenere l’ordinamento all’interno della singola partizione;
- associare una posizione a ogni record tramite l’offset;
- conservare gli offset committati dai consumer group;
- permettere la rilettura storica degli eventi.

### Ricostruttore Java

Il ricostruttore Java contiene la parte principale della logica applicativa. Il codice è suddiviso in package con responsabilità distinte:

```text
domain          modelli immutabili
serialization   conversione JSON -> OrderEvent
projection      regole di transizione dello stato
processing      elaborazione e transazioni applicative
consumer        integrazione con Kafka
persistence     JDBC e repository PostgreSQL
replay          replay completo e storico
snapshot        policy e replay ottimizzato
api             API HTTP e risorse statiche
```

Questa separazione permette di mantenere la logica di dominio indipendente dall’infrastruttura. Il projector, per esempio, non conosce Kafka o PostgreSQL. Riceve soltanto lo stato precedente e l’evento da applicare.

### PostgreSQL

PostgreSQL conserva le proiezioni necessarie alla lettura. Il database viene utilizzato per:

- memorizzare lo stato corrente di ogni ordine;
- registrare gli eventi già processati;
- impedire elaborazioni duplicate;
- salvare gli snapshot in formato JSONB;
- garantire che più operazioni vengano completate nella stessa transazione.

PostgreSQL fornisce quindi una rappresentazione interrogabile dello stato, mentre Kafka conserva la cronologia degli eventi.

### Dashboard

La dashboard rappresenta il livello finale della pipeline. È stata sviluppata utilizzando esclusivamente HTML, CSS e JavaScript standard.

La dashboard permette di:

- visualizzare metriche sintetiche;
- cercare e filtrare gli ordini;
- aprire il dettaglio di una proiezione;
- vedere articoli, destinazione, ritardo e hub visitati;
- consultare gli snapshot disponibili;
- passare dallo stato corrente a uno stato storico.

## 5. Contratto degli eventi

Tutti gli eventi seguono una struttura comune. Alcuni campi identificano l’evento e l’aggregato, mentre il contenuto del `payload` cambia in base al tipo di evento.

Un esempio semplificato è il seguente:

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

I campi principali rispettano le seguenti regole:

```text
eventId             identificatore univoco dell'evento
aggregateId         identificatore dell'ordine
aggregateVersion    versione progressiva a partire da 1
occurredAt          timestamp UTC del fatto
Kafka key           uguale ad aggregateId
payload             contenuto dipendente dal tipo di evento
```

L’`eventId` viene utilizzato per la deduplicazione. L’`aggregateId` identifica l’ordine e viene impiegato anche come chiave Kafka. L’`aggregateVersion` consente invece di verificare la continuità della sequenza.

Il campo `correlationId` permette di collegare eventi appartenenti allo stesso flusso operativo, mentre `producerId` e `producerType` indicano il sistema che ha generato il fatto.

## 6. Scenario ORD-2001

Lo scenario principale attraversa dieci versioni:

```text
v1  ORDER_CREATED          CREATED
v2  ORDER_CONFIRMED        CONFIRMED
v3  PAYMENT_COMPLETED      PAID
v4  INVENTORY_RESERVED     INVENTORY_RESERVED
v5  ORDER_PACKED           PACKED
v6  SHIPMENT_STARTED       IN_TRANSIT, HUB-MODENA
v7  HUB_REACHED            IN_TRANSIT, HUB-BOLOGNA
v8  DELIVERY_DELAYED       IN_TRANSIT, ritardo 35
v9  OUT_FOR_DELIVERY       OUT_FOR_DELIVERY, HUB-FIRENZE
v10 ORDER_DELIVERED        DELIVERED
```

Le prime versioni descrivono le operazioni commerciali e di preparazione. L’ordine viene creato, confermato e pagato. Successivamente viene riservato l’inventario e il pacco viene preparato.

Dalla versione 6 inizia il percorso logistico. La spedizione parte dall’hub di Modena, raggiunge Bologna, registra un ritardo di 35 minuti e arriva infine a Firenze per la consegna.

Questo scenario permette di verificare contemporaneamente:

- transizioni tra stati differenti;
- incremento della versione;
- gestione degli importi;
- attraversamento di più hub;
- accumulo di un ritardo;
- raggiungimento di uno stato terminale;
- creazione di snapshot intermedi e finali.

## 7. Flusso di elaborazione live

Quando il consumer riceve un record da Kafka, non aggiorna immediatamente il database. Prima di salvare il nuovo stato viene eseguita una sequenza di controlli.

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
processed_events contiene eventId ?
        |
        v
caricamento OrderState precedente
        |
        v
OrderStateProjector.apply
        |
        v
salvataggio order_states
        |
        +-> snapshot se version % 5 == 0
        |
        v
salvataggio processed_events
        |
        v
commit PostgreSQL
        |
        v
commit offset Kafka
```

Il record viene prima deserializzato. Il consumer verifica quindi che la chiave Kafka corrisponda all’`aggregateId` contenuto nell’evento.

La tabella `processed_events` viene interrogata per capire se l’evento sia già stato elaborato. Se l’`eventId` è già presente, il processor restituisce un risultato duplicato e non applica nuovamente il cambiamento.

Se l’evento è nuovo, il repository carica lo stato precedente e il projector calcola lo stato successivo:

```java
OrderState currentState = projector.apply(
    previousState,
    event
);
```

Il projector rimane indipendente dall’infrastruttura. Non effettua query SQL, non legge Kafka e non utilizza l’orologio del sistema. Questa caratteristica permette di riutilizzarlo sia nell’elaborazione live sia durante il replay.

Le modifiche a `order_states`, `order_snapshots` e `processed_events` vengono completate nella stessa transazione PostgreSQL. Soltanto dopo il commit del database il consumer può committare l’offset Kafka.

## 8. Persistenza PostgreSQL

PostgreSQL utilizza tre tabelle principali, ognuna con una responsabilità specifica.

### `orderflow.order_states`

La tabella `order_states` contiene una riga per ogni ordine. Rappresenta la proiezione corrente e permette alla dashboard di recuperare rapidamente lo stato senza dover rileggere l’intera cronologia.

I campi principali sono:

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

Le informazioni strutturate, come gli articoli e gli hub visitati, vengono conservate in formato JSONB. I campi più utilizzati nelle query restano invece colonne relazionali.

### `orderflow.processed_events`

La tabella `processed_events` registra gli eventi già elaborati e garantisce l’idempotenza applicativa.

I vincoli principali sono:

```text
event_id UNIQUE
order_id + aggregate_version UNIQUE
topic + partition + offset UNIQUE
```

Questi vincoli permettono di riconoscere:

- lo stesso evento ricevuto più volte;
- la stessa versione applicata due volte;
- lo stesso record Kafka elaborato nuovamente.

### `orderflow.order_snapshots`

La tabella `order_snapshots` salva una copia completa dello stato a determinate versioni.

```text
order_id
aggregate_version
state_data JSONB
created_at
UNIQUE(order_id, aggregate_version)
```

Il campo `state_data` contiene l’intero `OrderState` serializzato. Il vincolo univoco impedisce la creazione di due snapshot per lo stesso ordine e per la stessa versione.

## 9. Replay storico

OrderFlow permette di ricostruire lo stato in modi differenti, utilizzando sempre lo stesso `OrderStateProjector`.

### Replay completo

Il replay completo applica tutti gli eventi disponibili:

```java
replayService.replayAll(events);
```

Nel caso di `ORD-2001`, vengono applicati tutti i dieci eventi e il risultato è `DELIVERED v10`.

### Replay per versione

Il replay può fermarsi a una versione specifica:

```java
replayService.replayToVersion(
    events,
    5
);
```

Il risultato è lo stato `PACKED v5`.

Questa operazione permette di osservare l’aggregato in un momento preciso della sua evoluzione.

### Replay temporale

Il replay può anche selezionare gli eventi avvenuti entro un determinato timestamp:

```java
replayService.replayAtTime(
    events,
    targetTime
);
```

In questo caso la ricostruzione applica soltanto gli eventi con `occurredAt` precedente o uguale al momento richiesto.

### Replay della proiezione PostgreSQL

È stato inoltre verificato che la proiezione possa essere eliminata e ricostruita:

```text
lettura dello stato originale
eliminazione della proiezione ORD-2001
applicazione dei dieci eventi
salvataggio della nuova proiezione
confronto tra stato originale e ricostruito
```

Il risultato ha confermato che la proiezione PostgreSQL è un dato derivato.

### Replay diretto da Kafka

Il progetto contiene anche una modalità di lettura storica da Kafka:

```text
assegnazione manuale delle partizioni
seekToBeginning
lettura fino agli end offset iniziali
filtro per orderId
nessun commit degli offset
```

In questo modo il replay non modifica la posizione del consumer operativo.

## 10. Snapshot

La policy di OrderFlow crea uno snapshot ogni cinque versioni:

```text
snapshot ogni 5 versioni
```

Per `ORD-2001` vengono quindi generati:

```text
v5  PACKED
v10 DELIVERED
```

Il replay completo richiede dieci applicazioni del projector:

```text
Replay completo: 10 eventi
```

Se la ricostruzione parte dallo snapshot alla versione 5, gli eventi precedenti sono già rappresentati nello stato salvato:

```text
Replay da snapshot v5: 5 eventi
```

Il confronto produce:

```text
Replay completo:        10 eventi
Replay da snapshot v5:   5 eventi
Riduzione:              50%
Stato finale:            equivalente
```

Durante la verifica è emersa una differenza tecnica nella rappresentazione dell’importo:

```text
55.0
55.00
```

I valori sono numericamente equivalenti, ma `BigDecimal.equals()` considera anche la scala. Il confronto utilizza quindi:

```java
first.totalAmount().compareTo(
    second.totalAmount()
) == 0
```

Questa verifica ha confermato che replay completo e replay da snapshot producono lo stesso risultato dal punto di vista del dominio.

## 11. API HTTP e dashboard

L’applicazione Java espone le proiezioni e gli snapshot attraverso diversi endpoint:

```text
GET /api/health
GET /api/orders
GET /api/orders/{orderId}
GET /api/orders/{orderId}/snapshots
GET /api/orders/{orderId}/state?version=5
```

L’endpoint `/api/health` verifica lo stato dell’applicazione e la raggiungibilità di PostgreSQL.

L’endpoint `/api/orders` restituisce la lista sintetica delle proiezioni, mentre `/api/orders/{orderId}` restituisce lo stato completo dell’ordine.

Gli endpoint storici permettono di ottenere l’elenco degli snapshot e lo stato corrispondente a una versione disponibile.

La dashboard è composta da tre file statici:

```text
src/main/resources/static/
├── index.html
├── css/styles.css
└── js/app.js
```

Questi file vengono inclusi nel JAR Maven e serviti direttamente da `ApiApplication`.

L’applicazione è disponibile all’indirizzo:

```text
http://localhost:8081/
```

La scelta di utilizzare HTML, CSS e JavaScript standard consente di evitare Node.js, framework frontend e server statici separati. Inoltre, frontend e API condividono la stessa origine HTTP, semplificando la configurazione.







## 12. Demo

La demo può essere organizzata in un percorso di circa otto o dieci minuti.

### 1. Presentare il problema

Iniziare spiegando che un modello CRUD mostra lo stato corrente, ma non conserva necessariamente il percorso che ha prodotto quello stato.

Nel caso dell’ordine, vedere soltanto:

```text
DELIVERED
```

non permette di sapere quali hub siano stati attraversati, se si sia verificato un ritardo o in quale momento siano avvenuti i cambiamenti.

### 2. Mostrare il contratto dell’evento

Aprire lo scenario JSON e illustrare i campi principali:

```text
eventId
eventType
aggregateId
aggregateVersion
occurredAt
payload
```

Sottolineare che `aggregateVersion` descrive la posizione dell’evento nella storia dell’ordine.

### 3. Spiegare Kafka

Descrivere il ruolo di Kafka e la scelta della chiave:

```text
Kafka key = orderId
```

La chiave permette agli eventi dello stesso ordine di raggiungere la stessa partizione. È quindi possibile mantenere l’ordinamento relativo della sequenza.

Spiegare inoltre il ruolo di offset e consumer group.

### 4. Mostrare il projector

Aprire `OrderStateProjector.apply` e spiegare che la funzione riceve lo stato precedente e un evento.

```java
nextState = projector.apply(
    previousState,
    event
);
```

Sottolineare che la funzione è pura e deterministica.

### 5. Eseguire i test

```powershell
mvn clean test
```

Il risultato verde dimostra che deserializzazione, transizioni, versioni, replay e snapshot sono verificati automaticamente.

### 6. Mostrare PostgreSQL

Visualizzare la proiezione:

```text
ORD-2001 DELIVERED v10
```

Visualizzare quindi gli snapshot:

```text
snapshot v5  PACKED
snapshot v10 DELIVERED
```

### 7. Aprire la dashboard

Mostrare:

- stato corrente;
- importo;
- articoli;
- ritardo;
- hub visitati;
- ultimo aggiornamento.

### 8. Mostrare il time travel

Aprire lo snapshot alla versione 5:

```text
PACKED v5
```

Spiegare che in quel momento la spedizione non era ancora iniziata.

Tornare quindi allo stato corrente:

```text
DELIVERED v10
```

### 9. Concludere

La conclusione può essere sintetizzata così:

```text
Gli eventi rappresentano la storia.
Lo stato è una proiezione ricostruibile.
Gli snapshot riducono il costo del replay.
```

## 13. Limitazioni note

OrderFlow è un progetto didattico e include alcune semplificazioni consapevoli.

Lo scenario dimostrativo principale è concentrato su `ORD-2001`. Questa scelta permette di verificare in modo deterministico tutte le versioni, le transizioni e gli snapshot, ma non rappresenta un test di carico con molti ordini concorrenti.

Il cluster Kafka utilizza un singolo broker e un replication factor pari a 1. La configurazione è sufficiente per lo sviluppo locale, ma non offre l’alta disponibilità richiesta da un ambiente di produzione.

L’API HTTP non implementa autenticazione o autorizzazione. Anche la configurazione CORS è permissiva, perché la priorità del prototipo è mostrare i dati e non gestire la sicurezza applicativa.

Il frontend è stato realizzato volutamente senza framework. Questa scelta rende il codice più semplice da comprendere, visto che l'unico obiettivo finale era mostrare in modo più chiaro ciò che l'eventSourcing ricostruisce.

Queste limitazioni non compromettono l’obiettivo didattico, anzi, definiscono piuttosto il confine tra un prototipo dimostrativo e un sistema pronto per la produzione.



