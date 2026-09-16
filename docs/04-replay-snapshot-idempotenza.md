# Replay, Snapshot, Idempotenza e Ottimizzazione della Ricostruzione

## Indice

1. [Obiettivo del documento](#1-obiettivo-del-documento)
2. [Il costo della ricostruzione completa](#2-il-costo-della-ricostruzione-completa)
3. [Replay deterministico](#3-replay-deterministico)
4. [Snapshot](#4-snapshot)
5. [Scelta dell’intervallo](#5-scelta-dellintervallo)
6. [Replay da snapshot](#6-replay-da-snapshot)
7. [Snapshot e database](#7-snapshot-e-database)
8. [Idempotenza](#8-idempotenza)
9. [Transazioni e consistenza](#9-transazioni-e-consistenza)
10. [Confronto semantico dello stato](#10-confronto-semantico-dello-stato)
11. [Applicazione in OrderFlow](#11-applicazione-in-orderflow)
12. [Metriche e benchmark](#12-metriche-e-benchmark)
13. [Errori progettuali comuni](#13-errori-progettuali-comuni)
14. [Domande utili per l’esame](#14-domande-utili-per-lesame)
15. [Riferimenti](#15-riferimenti)

La ricostruzione dello stato rappresenta uno degli aspetti centrali dell’Event Sourcing. In questo modello, infatti, lo stato corrente non viene considerato come l’unica informazione importante, ma come il risultato dell’applicazione ordinata di tutti gli eventi appartenenti a un aggregato.

Questo approccio permette di ricostruire lo stato in qualsiasi momento, ma può diventare costoso quando la cronologia contiene migliaia o milioni di eventi. Applicare ogni volta l’intera sequenza può trasformarsi in un collo di bottiglia, soprattutto se molti aggregati vengono ricostruiti contemporaneamente.

Gli snapshot permettono di ridurre questo costo, perché salvano periodicamente una copia dello stato già calcolato. La cronologia degli eventi rimane comunque la fonte di verità, mentre lo snapshot viene utilizzato soltanto come punto di partenza più avanzato.

Il documento analizza quindi replay, snapshot, idempotenza e transazioni, utilizzando OrderFlow come esempio concreto.

## 2. Il costo della ricostruzione completa

Consideriamo un aggregato composto da `N` eventi. Se la ricostruzione parte sempre dallo stato iniziale, il projector deve applicare tutti gli eventi della cronologia.

Il costo può essere rappresentato in modo semplificato come:

```text
T_replay(N) = N * costo_apply
```

Il numero di operazioni cresce linearmente con la lunghezza dello stream:

```text
10 eventi      -> 10 applicazioni
10.000 eventi  -> 10.000 applicazioni
1.000.000      -> 1.000.000 applicazioni
```

Il costo del replay non dipende soltanto dalla chiamata alla funzione di proiezione. Per ogni evento possono essere necessarie diverse operazioni:

- lettura dal log o dall’event store;
- deserializzazione del contenuto;
- validazione del contratto;
- controllo dell’identificatore e della versione;
- applicazione della logica di dominio;
- creazione del nuovo stato;
- eventuali operazioni accessorie.

Un replay occasionale di pochi eventi è generalmente poco costoso. Il problema emerge quando gli stream diventano molto lunghi oppure quando il sistema deve ricostruire contemporaneamente molti aggregati. In questi casi possono aumentare il consumo di CPU, il traffico di rete, l’utilizzo dello storage e il tempo necessario per rispondere a una richiesta.

## 3. Replay deterministico

Per essere affidabile, un replay deve produrre sempre lo stesso risultato quando riceve lo stesso stato iniziale e la stessa sequenza ordinata di eventi.

```text
same initial state
+ same ordered events
= same final state
```

Questa proprietà prende il nome di determinismo. Per ottenerla è necessario che:

- gli eventi siano immutabili;
- l’ordine degli eventi sia conosciuto;
- il projector non dipenda dall’ora corrente;
- non vengano utilizzati valori casuali;
- il projector non effettui chiamate verso servizi esterni;
- le regole dipendano soltanto dallo stato precedente e dall’evento ricevuto.

Un’applicazione corretta della proiezione ha quindi una forma simile alla seguente:

```java
OrderState next = projector.apply(
    current,
    event
);
```

Un esempio problematico sarebbe invece:

```java
state.withUpdatedAt(Instant.now());
```

L’utilizzo di `Instant.now()` introdurrebbe un valore diverso a ogni esecuzione. Due replay della stessa cronologia produrrebbero quindi stati differenti.

Se l’evento contiene già il campo `occurredAt`, il projector deve utilizzare quel valore. Il timestamp dell’evento descrive quando il fatto è realmente avvenuto e rimane stabile anche durante una ricostruzione eseguita molto tempo dopo.

## 4. Snapshot

Uno snapshot è una copia completa dello stato di un aggregato a una determinata versione.

```text
Snapshot
  orderId = ORD-2001
  aggregateVersion = 5
  state = PACKED
```

Lo snapshot alla versione 5 rappresenta lo stesso risultato che si otterrebbe applicando gli eventi dalla versione 1 alla versione 5:

```text
Eventi v1-v5
     |
     v
Snapshot v5
```

Lo snapshot è un dato derivato. Può essere eliminato e ricostruito partendo dalla cronologia, quindi non sostituisce gli eventi e non diventa la nuova fonte di verità.

Uno snapshot affidabile dovrebbe possedere alcune proprietà fondamentali:

- deve contenere la versione esatta dell’aggregato;
- deve riportare l’identificatore dell’aggregato;
- deve rappresentare uno stato completo e utilizzabile;
- deve poter essere serializzato e deserializzato;
- deve essere idempotente sulla coppia `(aggregateId, version)`;
- deve poter essere confrontato con lo stato ottenuto dal replay completo.

È inoltre importante che la versione registrata nello snapshot coincida con la versione contenuta nello stato. Uno snapshot dichiarato come versione 5 non può contenere uno stato alla versione 4 o 6.

## 5. Scelta dell’intervallo

Una policy semplice consiste nel creare uno snapshot ogni `K` versioni.

In Java, la regola può essere espressa così:

```java
return state.version() % interval == 0;
```

Utilizzando un intervallo pari a 5, il comportamento sarà:

```text
v1  nessuno snapshot
v2  nessuno snapshot
v3  nessuno snapshot
v4  nessuno snapshot
v5  creazione snapshot
v6  nessuno snapshot
v7  nessuno snapshot
v8  nessuno snapshot
v9  nessuno snapshot
v10 creazione snapshot
```

### Intervallo troppo piccolo

Creare snapshot molto frequentemente riduce il numero di eventi necessari durante il replay, ma aumenta il numero delle scritture e lo spazio occupato.

Per esempio, uno snapshot a ogni versione produrrebbe molti dati quasi identici:

```text
snapshot v1
snapshot v2
snapshot v3
snapshot v4
snapshot v5
```

Il beneficio tra due versioni consecutive potrebbe non giustificare il costo aggiuntivo.

### Intervallo troppo grande

Un intervallo molto ampio riduce il costo di salvataggio degli snapshot, ma lascia al replay una quantità maggiore di lavoro.

Se viene creato uno snapshot ogni 10.000 eventi, una ricostruzione potrebbe comunque dover applicare migliaia di eventi successivi all’ultimo snapshot disponibile.

### Policy adattive

Nei sistemi più avanzati, la frequenza può dipendere da diversi fattori:

- numero di eventi elaborati;
- tempo trascorso dall’ultimo snapshot;
- costo medio del replay;
- frequenza di lettura dell’aggregato.

Un ordine molto attivo potrebbe richiedere snapshot frequenti, mentre un aggregato con pochi eventi potrebbe non averne bisogno. Nel progetto OrderFlow viene utilizzato un intervallo fisso perché è più semplice da comprendere, implementare e verificare.

## 6. Replay da snapshot

Per ricostruire una versione target `V`, il sistema cerca lo snapshot più recente che possa essere utilizzato come base. Nel caso di OrderFlow, per ricostruire la versione 10 viene selezionato lo snapshot precedente alla versione target, cioè quello alla versione 5.

```text
snapshot v5
+
eventi v6, v7, v8, v9, v10
=
stato v10
```

La ricostruzione non riparte quindi dallo stato vuoto. Gli eventi dalla versione 1 alla versione 5 sono già rappresentati nello snapshot e non devono essere applicati nuovamente.

L’algoritmo può essere descritto in questo modo:

```text
1. cerca lo snapshot più recente prima del target
2. usa snapshot.state come stato iniziale
3. memorizza snapshot.version come versione iniziale
4. seleziona gli eventi con versione successiva
5. applica gli eventi fino alla versione target
6. verifica che lo stato finale abbia la versione prevista
```

In pseudocodice:

```java
OrderState state = snapshot.state();

for (OrderEvent event : events) {
    if (
        event.aggregateVersion()
            > snapshot.version()
    ) {
        state = projector.apply(
            state,
            event
        );
    }
}
```

Il controllo finale sulla versione è importante. Se la ricostruzione termina alla versione 9 mentre la versione richiesta è 10, significa che manca almeno un evento oppure che la selezione della cronologia non è corretta.

## 7. Snapshot e database

OrderFlow conserva gli snapshot in PostgreSQL attraverso una tabella dedicata:

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

La tabella non suddivide lo stato in molte colonne relazionali, ma salva la rappresentazione completa di `OrderState` nel campo `state_data`.

Il tipo `JSONB` è adatto a questo scopo perché PostgreSQL verifica che il contenuto sia un documento JSON valido e lo conserva in una rappresentazione che può essere interrogata e indicizzata.[^postgres-jsonb]

Il vincolo:

```sql
UNIQUE (order_id, aggregate_version)
```

impedisce di salvare più snapshot per lo stesso ordine e la stessa versione:

```text
ORD-2001 v5  -> una sola riga
ORD-2001 v10 -> una sola riga
```

Il repository utilizza inoltre una strategia equivalente a:

```sql
ON CONFLICT (
    order_id,
    aggregate_version
)
DO NOTHING
```

Se lo stesso snapshot viene salvato una seconda volta, PostgreSQL non crea un duplicato e l’operazione rimane sicura.

## 8. Idempotenza

La proprietà di idempotenza non riguarda soltanto gli snapshot. Anche il processamento degli eventi deve poter essere ripetuto senza produrre modifiche duplicate.

### Idempotenza dello snapshot

Consideriamo due tentativi di salvataggio dello stesso snapshot:

```text
save(snapshot v5)
save(snapshot v5)
```

Il risultato deve essere:

```text
count = 1
```

La combinazione del vincolo univoco e di `ON CONFLICT DO NOTHING` garantisce questo comportamento.

### Idempotenza dell’evento

Lo stesso principio viene applicato agli eventi:

```text
process(eventId X)
process(eventId X)
```

Lo stato dell’ordine deve essere aggiornato una sola volta.

In OrderFlow, la tabella `processed_events` registra l’identità dell’evento insieme ai metadati Kafka. Prima di applicare un evento, il processor verifica se il relativo `eventId` è già presente.

La prima esecuzione dello scenario produce:

```text
Processed: 10
Duplicates: 0
```

Se la stessa sequenza viene elaborata nuovamente, il risultato diventa:

```text
Processed: 0
Duplicates: 10
```

Il sistema riconosce quindi gli eventi già elaborati e non modifica nuovamente la proiezione o gli snapshot.

Questa proprietà è essenziale nelle architetture at-least-once, dove un record può essere consegnato nuovamente in seguito a un crash o a un mancato commit dell’offset.

## 9. Transazioni e consistenza

Lo stato corrente, l’eventuale snapshot e il record di deduplicazione devono essere salvati nella stessa transazione PostgreSQL.

```text
BEGIN
    UPSERT order_states
    INSERT order_snapshots, se necessario
    INSERT processed_events
COMMIT
```

Questa struttura evita situazioni incoerenti. Per esempio, non vogliamo che lo stato venga aggiornato senza registrare l’evento tra quelli processati, oppure che venga creato uno snapshot senza salvare la relativa proiezione.

Se una delle operazioni fallisce, l’intera transazione viene annullata:

```text
errore durante il salvataggio
        |
        v
ROLLBACK
```

PostgreSQL garantisce l’atomicità della transazione. Tutte le modifiche diventano visibili insieme oppure nessuna modifica produce effetto.[^postgres-transactions]

La sequenza applicativa completa è:

```text
Kafka event
    |
    v
BEGIN PostgreSQL
    salva stato
    salva eventuale snapshot
    registra evento processato
COMMIT PostgreSQL
    |
    v
COMMIT Kafka offset
```

Il commit dell’offset Kafka avviene soltanto dopo il successo della transazione PostgreSQL. Se il database restituisce un errore, il consumer non avanza definitivamente la propria posizione e il record potrà essere riletto.

## 10. Confronto semantico dello stato

Durante il confronto tra replay completo e replay da snapshot, due stati possono essere equivalenti dal punto di vista del dominio ma differenti dal punto di vista della loro rappresentazione tecnica.

Un esempio è fornito da `BigDecimal`:

```text
55.0
55.00
```

I due valori rappresentano lo stesso importo. Il metodo `compareTo()` li considera numericamente uguali:

```java
first.compareTo(second) == 0
```

Il metodo `equals()`, invece, restituisce `false`:

```java
first.equals(second) == false
```

Questo accade perché `equals()` considera anche la scala decimale. Il primo valore ha una cifra decimale, mentre il secondo ne possiede due.

Nel progetto OrderFlow, la serializzazione JSONB e la lettura da PostgreSQL hanno prodotto esattamente questa differenza:

```text
replay completo:  55.0
replay snapshot:  55.00
```

Per questo motivo, il confronto tra i due stati utilizza un criterio semantico:

```java
first.totalAmount().compareTo(
    second.totalAmount()
) == 0
```

Questa distinzione è importante nei sistemi distribuiti. Database, serializzatori e linguaggi differenti possono normalizzare la rappresentazione di un valore senza modificarne il significato applicativo.

## 11. Applicazione in OrderFlow

Nel progetto OrderFlow, la policy crea uno snapshot ogni cinque versioni.

Per l’ordine `ORD-2001` sono stati salvati:

```text
ORD-2001 v5  PACKED
ORD-2001 v10 DELIVERED
```

### Replay completo

La ricostruzione completa parte dallo stato vuoto e applica tutti i dieci eventi:

```text
stato iniziale
+ eventi v1-v10
= DELIVERED v10
```

Il numero di eventi applicati è:

```text
10
```

### Replay da snapshot v5

La ricostruzione ottimizzata utilizza lo stato `PACKED` già salvato alla versione 5 e applica soltanto gli eventi successivi:

```text
snapshot v5
+ eventi v6-v10
= DELIVERED v10
```

Il numero di eventi applicati è quindi:

```text
5
```

### Riduzione ottenuta

Il confronto fornisce il seguente risultato:

```text
Eventi replay completo:      10
Eventi replay con snapshot:   5
Eventi evitati:               5
Riduzione:                   50%
```

I due percorsi producono uno stato finale semanticamente equivalente.

### Snapshot finale

Lo snapshot alla versione 10 rappresenta già lo stato finale dell’ordine:

```text
snapshot v10
+ 0 eventi successivi
= DELIVERED v10
```

Questo significa che, se il sistema deve recuperare esattamente la versione 10 e accetta lo snapshot come base valida, non è necessario applicare ulteriori eventi.

## 12. Metriche e benchmark

Per valutare correttamente l’efficacia degli snapshot non è sufficiente contarne il numero. È utile misurare diversi aspetti:

- numero di eventi applicati durante il replay;
- durata complessiva della ricostruzione;
- memoria allocata;
- dimensione dello snapshot;
- frequenza di creazione;
- tempo necessario per caricarlo;

La percentuale di riduzione può essere calcolata con:

```text
riduzione_percentuale =
(eventi_completi - eventi_post_snapshot)
/
eventi_completi
*
100
```

Nel caso di OrderFlow:

```text
(10 - 5) / 10 * 100 = 50%
```

Uno snapshot non garantisce però automaticamente un miglioramento. Se lo stato serializzato è molto grande e il replay degli eventi è particolarmente economico, il caricamento e la deserializzazione dello snapshot potrebbero avere un costo simile o superiore.

Per questo motivo, in un sistema reale l’intervallo dovrebbe essere scelto sulla base di misurazioni e non soltanto di un valore arbitrario.

## 13. Errori progettuali comuni

Durante l’introduzione degli snapshot è facile commettere alcuni errori.

Il primo consiste nel considerare lo snapshot come la nuova fonte di verità. Lo snapshot è invece una rappresentazione derivata e deve poter essere eliminato senza perdere la cronologia del sistema.

Un altro errore consiste nel creare uno snapshot con una versione diversa da quella dello stato contenuto. Questa incoerenza comprometterebbe la selezione degli eventi successivi.

È inoltre necessario evitare di:

- eliminare gli eventi dopo la creazione dello snapshot senza una policy precisa;
- scegliere intervalli senza effettuare misurazioni;
- non confrontare il risultato con il replay completo.

Il test più importante consiste proprio nel verificare che i due percorsi convergano allo stesso stato applicativo.


## 15. Riferimenti

PostgreSQL Documentation, *JSON Types*, https://www.postgresql.org/docs/current/datatype-json.html

PostgreSQL Documentation, *Transactions*, https://www.postgresql.org/docs/current/tutorial-transactions.html

Microsoft Azure Architecture Center, *Event Sourcing pattern*, https://learn.microsoft.com/it-it/azure/architecture/patterns/event-sourcing