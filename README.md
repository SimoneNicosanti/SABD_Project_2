# SABD_Project_2
Questo repository contiene il codice che permette di analizzare dei dati forniti dalla Infront Financial Tecnology riguardanti scambi di strumenti finanziari avvenuti tra l'8 e il 14 Novembre 2021.

L'analisi è fatta in modalità streaming: per simulare al meglio lo streaming dei dati un container producer si occupa di inserire i dati in un topic Kafka da cui poi vengono letti da Flink.

Le queries a chui si è risposto sono le seguenti:
* **Query_1.** Per le azioni (campo SecType con valore pari a E) scambiate sui mercati di Parigi (FR) che iniziano per “G”, calcolare il numero di eventi ed il valor medio del prezzo di vendita (campo Last) sulle finestre temporali di 1 ora, 1 giorno, dall'inizio del dataset.
* **Query_2.** Calcolare la classifica aggiornata in tempo reale delle 5 azioni (di qualsiasi mercato) che registrano la più alta variazione del prezzo di vendita calcolato nella finestra temporale indicata di seguito e delle 5 azioni (di qualsiasi mercato) con la variazione del prezzo di vendita più bassa sulle finestre temporali di 30 minuti, 1 ora, 1 giorno.
* **Query_3.** Dopo aver calcolato la variazione di prezzo di vendita delle azioni scambiate sui diversi mercati per la finestra temporale di seguito indicata, raggruppare le azioni per mercato (ETR, FR e NL) e calcolare il 25-esimo, 50-esimo, 75-esimo percentile della variazione del prezzo di vendita per ciascun mercato sulle finestre temporali di 30 minuti, 1 ora, 1 giorno.

Il framework di elaborazione usato è Apache Flink; i codici delle query si trovano in *flink_processor/src/queries*

## Requisiti 
Il progetto usa **Docker** e **Docker Compose** per istanziare Kafka, Jobmanager e Taskmanager di Flink e container di produce e consume.

## Deployment
Per fare il deployment del progetto eseguire:
```bash
docker compose up --detach
```

Il dataset sarà scaricato automaticamente dal web da internet alla partenza del container del producer.

## Esecuzione delle Query
Per eseguire le query si può eseguire dalla directory utils della cartella docker il comando:
```bash
query_result_getter.sh <queryNum>
```
Con **queryNum** :
* 1 &rarr; Query_1
* 2 &rarr; Query_2
* 3 &rarr; Query_3

I risultati della query vengono scritti nella directory *Results* nel file con nome *Query_numQuery_timeWindow*

Se si vuole eseguire una query per il monitoraggio eseguire il comando:
```bash
metrics_getter.sh <numeroDellaQuery>
```
Il monitoraggio è fatto usando Grafana; si può accedere alla WebUI di Grafana dal link *localhost:3000/* dopo aver fatto partire l'archittettura