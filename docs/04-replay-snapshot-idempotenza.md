# Replay, Snapshot, Idempotenza e Ottimizzazione della Ricostruzione

## Indice

1. [Obiettivo del documento](#1-obiettivo-del-documento)
2. [Il costo della ricostruzione completa](#2-il-costo-della-ricostruzione-completa)
3. [Replay deterministico](#3-replay-deterministico)
4. [Snapshot](#4-snapshot)
5. [Scelta dell'intervallo](#5-scelta-dellintervallo)
6. [Replay da snapshot](#6-replay-da-snapshot)
7. [Snapshot e database](#7-snapshot-e-database)
8. [Idempotenza](#8-idempotenza)
9. [Transazioni e consistenza](#9-transazioni-e-consistenza)
10. [Confronto semantico dello stato](#10-confronto-semantico-dello-stato)
11. [Applicazione in OrderFlow](#11-applicazione-in-orderflow)
12. [Metriche e benchmark](#12-metriche-e-benchmark)
13. [Errori progettuali comuni](#13-errori-progettuali-comuni)
14. [Domande utili per l'esame](#14-domande-utili-per-lesame)
15. [Riferimenti](#15-riferimenti)

## 1. Obiettivo del documento

La ricostruzione dello stato è il centro dell'Event Sourcing. Tuttavia, applicare migliaia o milioni di eventi ogni volta può diventare un collo di bottiglia. Gli snapshot riducono il numero di eventi da rielaborare senza cambiare la fonte di verità.

Questo documento analizza replay, snapshot, idempotenza e transazioni, usando OrderFlow come caso pratico.

## 2. Il costo della ricostruzione completa

Consideriamo un aggregato con `N` eventi. Un replay completo richiede una complessità lineare:

```text
T_replay(N) = N * costo_apply
```

Esempio:

```text
10 eventi     -> 10 applicazioni
10.000 eventi -> 10.000 applicazioni
1.000.000     -> 1.000.000 applicazioni
```

Il costo reale comprende:

- lettura dal log;
- deserializzazione;
- validazione;
- applicazione della funzione di dominio;
- allocazione del nuovo stato;
- eventuali operazioni accessorie.

Un singolo replay può essere accettabile. Molti replay simultanei possono saturare CPU, rete o storage.

## 3. Replay deterministico

Un replay è affidabile se la funzione di proiezione è deterministica.

```text
same initial state
+ same ordered events
= same final state
```

Condizioni:

- gli eventi sono immutabili;
- il loro ordine è noto;
- il projector non usa l'ora corrente;
- il projector non legge valori casuali;
- il projector non effettua chiamate esterne;
- le regole dipendono soltanto da stato ed evento.

Esempio corretto:

```java
OrderState next = projector.apply(current, event);
```

Esempio problematico:

```java
state.withUpdatedAt(Instant.now());
```

Se l'evento contiene già `occurredAt`, il replay deve usare quel timestamp, non l'orologio del momento di ricostruzione.

## 4. Snapshot

Uno snapshot è una copia dello stato dell'aggregato a una specifica versione.

```text
Snapshot
  orderId = ORD-2001
  aggregateVersion = 5
  state = PACKED
```

```text
Eventi v1-v5
     |
     v
Snapshot v5
```

Lo snapshot è derivato. Può essere eliminato e ricreato. Non sostituisce la cronologia degli eventi.

### Proprietà desiderate

- contiene la versione esatta;
- contiene l'identificatore dell'aggregato;
- rappresenta uno stato completo;
- è serializzabile e deserializzabile;
- è idempotente sulla coppia `(aggregateId, version)`;
- può essere validato rispetto allo stream.

## 5. Scelta dell'intervallo

Una policy semplice crea uno snapshot ogni `K` versioni:

```java
return state.version() % interval == 0;
```

Con `K = 5`:

```text
v1  no
v2  no
v3  no
v4  no
v5  snapshot
v6  no
v7  no
v8  no
v9  no
v10 snapshot
```

### Intervallo troppo piccolo

- molte scritture;
- maggiore occupazione;
- beneficio marginale tra snapshot vicini.

### Intervallo troppo grande

- pochi costi di scrittura;
- replay più lunghi;
- recupero più lento.

### Policy adattive

In sistemi avanzati, la policy può dipendere da:

- numero di eventi;
- tempo trascorso;
- dimensione dello stato;
- costo medio del replay;
- tipo di aggregato;
- stato terminale.

Per un progetto didattico, un intervallo fisso è più chiaro e verificabile.

## 6. Replay da snapshot

Per ricostruire la versione target `V`, si cerca lo snapshot più recente con versione inferiore o uguale al target, a seconda della semantica scelta.

```text
snapshot v5
+
eventi v6, v7, v8, v9, v10
=
stato v10
```

Algoritmo:

```text
1. trova latest snapshot before target
2. state = snapshot.state
3. startVersion = snapshot.version
4. seleziona eventi con version > startVersion
5. applica eventi fino alla targetVersion
6. verifica state.version == targetVersion
```

Pseudocodice:

```java
OrderState state = snapshot.state();

for (OrderEvent event : events) {
    if (event.aggregateVersion() > snapshot.version()) {
        state = projector.apply(state, event);
    }
}
```

## 7. Snapshot e database

OrderFlow salva gli snapshot in PostgreSQL:

```sql
CREATE TABLE orderflow.order_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    aggregate_version BIGINT NOT NULL,
    state_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (order_id, aggregate_version)
);
```

`JSONB` è adatto allo stato serializzato perché PostgreSQL valida il JSON, lo memorizza in una rappresentazione elaborabile e supporta indicizzazione e operatori dedicati.[^postgres-jsonb]

Il vincolo unico impedisce duplicati logici:

```text
ORD-2001 v5 -> una sola riga
ORD-2001 v10 -> una sola riga
```

Il repository usa concettualmente:

```sql
ON CONFLICT (order_id, aggregate_version)
DO NOTHING
```

## 8. Idempotenza

Lo snapshot deve essere idempotente, ma anche l'elaborazione dell'evento deve esserlo.

### Idempotenza dello snapshot

```text
save(snapshot v5)
save(snapshot v5)
count = 1
```

### Idempotenza dell'evento

```text
process(eventId X)
process(eventId X)
state aggiornato una sola volta
```

In OrderFlow la tabella `processed_events` registra l'identità dell'evento e la posizione Kafka. La seconda elaborazione restituisce un risultato `alreadyProcessed`.

```text
prima esecuzione
Processed: 10
Duplicates: 0

seconda esecuzione
Processed: 0
Duplicates: 10
```

## 9. Transazioni e consistenza

Lo stato, il record di deduplicazione e lo snapshot devono essere scritti nella stessa transazione PostgreSQL.

```text
BEGIN
    UPSERT order_states
    INSERT order_snapshots, se necessario
    INSERT processed_events
COMMIT
```

Se lo snapshot fallisce:

```text
ROLLBACK
```

PostgreSQL garantisce che una transazione sia atomica: i passaggi sono visibili tutti insieme oppure non producono effetto.[^postgres-transactions]

La sequenza applicativa completa è:

```text
Kafka event
    |
    v
BEGIN PostgreSQL
    state
    snapshot
    processed event
COMMIT PostgreSQL
    |
    v
COMMIT Kafka offset
```

## 10. Confronto semantico dello stato

Due stati possono essere semanticamente uguali ma non risultare uguali con un confronto tecnico diretto.

Esempio `BigDecimal`:

```text
55.0
55.00
```

Numericamente:

```java
first.compareTo(second) == 0
```

Ma:

```java
first.equals(second) == false
```

perché `equals` considera anche la scala.

Nel confronto tra replay completo e replay da snapshot, OrderFlow usa equivalenza semantica per gli importi:

```java
first.totalAmount().compareTo(
    second.totalAmount()
) == 0
```

Questa distinzione è importante nei test di sistemi distribuiti, perché serializzazione e database possono normalizzare la rappresentazione senza cambiare il valore di dominio.

## 11. Applicazione in OrderFlow

### Snapshot creati

```text
ORD-2001 v5  PACKED
ORD-2001 v10 DELIVERED
```

### Replay completo

```text
10 eventi applicati
stato finale = DELIVERED v10
```

### Replay da snapshot v5

```text
snapshot v5
+ 5 eventi
= DELIVERED v10
```

### Riduzione

```text
Eventi replay completo:      10
Eventi replay con snapshot:   5
Eventi evitati:               5
Riduzione:                   50%
```

### Snapshot finale

Lo snapshot v10 rappresenta già lo stato finale:

```text
snapshot v10 + 0 eventi = DELIVERED v10
```

## 12. Metriche e benchmark

Per valutare gli snapshot è utile misurare:

- numero di eventi applicati;
- durata del replay;
- memoria allocata;
- dimensione dello snapshot;
- frequenza di creazione;
- tempo di caricamento dello snapshot;
- percentuale di eventi evitati;
- hit rate degli snapshot.

Formula di riduzione:

```text
riduzione_percentuale =
(eventi_completi - eventi_post_snapshot)
/ eventi_completi * 100
```

Gli snapshot non garantiscono automaticamente un guadagno. Se lo stato è enorme e il replay è economico, caricare uno snapshot molto grande potrebbe non essere vantaggioso.

## 13. Errori progettuali comuni

- considerare lo snapshot la fonte di verità;
- eliminare eventi dopo lo snapshot senza una policy consapevole;
- creare snapshot con versione non corrispondente allo stato;
- usare `Instant.now()` dentro il projector;
- salvare snapshot fuori dalla transazione applicativa;
- non gestire duplicati;
- usare confronto `BigDecimal.equals()` per equivalenza monetaria;
- scegliere intervalli arbitrari senza misurazioni;
- non testare la ricostruzione senza snapshot;
- non verificare che replay completo e ottimizzato convergano.

## 14. Domande utili per l'esame

### Lo snapshot sostituisce gli eventi?

No. È un'ottimizzazione derivata. Gli eventi restano la cronologia autorevole.

### Perché usare uno snapshot precedente al target?

Per evitare di applicare eventi già rappresentati nello stato salvato.

### Perché `v5 + eventi v6-v10` produce lo stesso stato del replay completo?

Perché il projector è deterministico e lo snapshot v5 equivale al risultato degli eventi v1-v5.

### Come si evita di salvare due volte lo stesso snapshot?

Con un vincolo unico su ordine e versione e con `ON CONFLICT DO NOTHING`.

### Qual è il vantaggio misurato nel progetto?

Per lo scenario di 10 eventi, il replay da v5 applica 5 eventi, con una riduzione del 50%.

## 15. Riferimenti

[^postgres-jsonb]: PostgreSQL Documentation, *JSON Types*, https://www.postgresql.org/docs/current/datatype-json.html
[^postgres-transactions]: PostgreSQL Documentation, *Transactions*, https://www.postgresql.org/docs/current/tutorial-transactions.html
[^microsoft-es]: Microsoft Azure Architecture Center, *Event Sourcing pattern*, https://learn.microsoft.com/it-it/azure/architecture/patterns/event-sourcing
