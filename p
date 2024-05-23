Un file journal contiene i log delle transazioni di BookKeeper. Prima di effettuare qualsiasi aggiornamento, un bookie
(server di storage che gestisce dei ledgers, ovvero una sequenza di entries dove ogni entry è una sequenza di bytes
scritti in maniera sequenziale con politica append-only e at most once) si assicura che una transazione che descrive
l'aggiornamento sia scritta sulla memoria non-volatile. Un nuovo file di journal viene creato all'avvio del bookie o quando il
file di journal più vecchio raggiunge la soglia di dimensione del file di journal.
JournalScanningJournalTest -



Nel contesto della classe Journal il primo metodo individuato è il seguente:
public long scanJournal(long journalId, long journalPos, JournalScanner scanner)
Questo metodo offre alla classe Journal la possibilità di eseguire una scansione di un journal (identificato tramite
journalId) a partire da una specifica posizione (indicata tramite journalPos) utilizzando un oggetto scanner.
Iniziamo con l’analisi dei singoli parametri:
● long journalId: identificativo del log del journal
● long journalPos: posizione da cui iniziare la scansione
● JournalScanner scanner: oggetto scanner per la gestione delle entries del journal
Dalla dichiarazione del metodo sappiamo anche che il valore di ritorno è long scanOffset e corrisponde all’offset del byte
fino al quale il journal è stato letto.


Con un approccio prettamente black box le classi di equivalenza individuate per i primi due parametri sarebbero {<0},
{>=0}. Tuttavia, quando un journal viene creato, ad esso viene ragionevolmente associato un certo journalId: dalle prime
prove eseguite nei test ho verificato che a meno che tale parametro non corrisponda al vero journalId il flow di
esecuzione risulta costante e relegato ad un solo outcome.
Per quanto appena detto quindi le classi di equivalenza scelte sono le seguenti:
● long journalId: {correctJournalId}, {incorrectJournalId}
● long journalPos: {<0}, {>=0}
● JournalScanner scanner: {null}, {valid_instance}, {invalid_instance}
Dove il correctJournalId è preso attraverso la chiamata a:
Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
Per quanto riguarda le istanze di scanner si è preso spunto dalla test suite originale presente in bookkeeper (di cui si
sono mantenute anche alcune classi e/o metodi per gestire la configurazione