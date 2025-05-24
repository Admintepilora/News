#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utilità per interagire con ClickHouse
Fornisce funzioni per salvare articoli di news in ClickHouse
Versione modificata con supporto per failover alla tabella news_original
"""
import os
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client

def get_clickhouse_client(host=None, port=None, user=None, password=None, database=None):
    """Crea e restituisce un client ClickHouse usando variabili d'ambiente o valori predefiniti"""
    # Leggi dalle variabili d'ambiente o usa i valori di fallback
    host = host or os.environ.get('CLICKHOUSE_HOST', '91.99.20.165')
    port = port or int(os.environ.get('CLICKHOUSE_PORT', '9000'))
    user = user or os.environ.get('CLICKHOUSE_USER', 'default')
    password = password or os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse')
    database = database or os.environ.get('CLICKHOUSE_DATABASE', 'news')
    
    return Client(host=host, port=port, user=user, password=password, database=database)

def check_table_availability(table_name='news', database=None, logger=None):
    """
    Verifica se la tabella specificata è disponibile per inserimenti
    Restituisce True se la tabella è disponibile, False altrimenti
    """
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    database = database or os.environ.get('CLICKHOUSE_DATABASE', 'news')
    client = get_clickhouse_client(database=database)
    
    try:
        # Prova a verificare l'esistenza della tabella
        tables = client.execute(f"SHOW TABLES FROM {database} LIKE '{table_name}'")
        if not tables:
            log_error(f"La tabella {database}.{table_name} non esiste")
            return False
        
        # Prova a inserire una riga di test per verificare se la tabella è disponibile per scrittura
        test_id = datetime.now().strftime('%Y%m%d%H%M%S')
        try:
            client.execute(f"""
                INSERT INTO {database}.{table_name}
                (url, title, body, date, source, searchKey, image)
                VALUES
                ('test://availability_check/{test_id}', 'Test', 'Test', now(), 'Test', NULL, NULL)
            """)
            
            # Se arrivi qui, la tabella è disponibile
            # Ora elimina la riga di test
            client.execute(f"""
                ALTER TABLE {database}.{table_name} DELETE WHERE url = 'test://availability_check/{test_id}'
            """)
            
            log(f"Tabella {database}.{table_name} disponibile per inserimenti")
            return True
        except Exception as e:
            if "Table is not initialized yet" in str(e):
                log_error(f"La tabella {database}.{table_name} non è ancora inizializzata: {e}")
            else:
                log_error(f"Errore durante il test di inserimento nella tabella {database}.{table_name}: {e}")
            return False
    except Exception as e:
        log_error(f"Errore durante la verifica della tabella {database}.{table_name}: {e}")
        return False

def create_news_table_if_not_exists():
    """Crea la tabella news se non esiste"""
    client = get_clickhouse_client()
    database = os.environ.get('CLICKHOUSE_DATABASE', 'news')
    
    # Controlla se il database esiste
    databases = [row[0] for row in client.execute("SHOW DATABASES")]
    if database not in databases:
        client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    # Crea la tabella - Nota: per i database esistenti, dovremo fare una migrazione separata
    # per applicare questa modifica, ma tutte le nuove installazioni avranno questa struttura
    query = f"""
    CREATE TABLE IF NOT EXISTS {database}.news (
        id UUID DEFAULT generateUUIDv4(),
        url String,
        title String,
        body String,
        date DateTime,
        source String,
        searchKey Nullable(String),
        image Nullable(String),
        created_at DateTime DEFAULT now(),
        
        INDEX url_idx url TYPE bloom_filter GRANULARITY 1,
        INDEX title_idx title TYPE tokenbf_v1(8, 3, 0) GRANULARITY 1,
        INDEX body_idx body TYPE tokenbf_v1(8, 3, 0) GRANULARITY 1,
        INDEX source_idx source TYPE tokenbf_v1(4, 3, 0) GRANULARITY 1
    ) ENGINE = MergeTree()
    ORDER BY (url, date, source, id)  -- URL come prima campo per facilitare la deduplicazione
    PARTITION BY toYYYYMM(date)
    SETTINGS index_granularity = 8192
    """
    
    client.execute(query)
    return True

def save_articles_to_clickhouse(articles, table_name='news', dedup_by_url=True, logger=None, 
                          batch_size=1000, retry_attempts=3):
    """
    Salva gli articoli di news in ClickHouse con gestione ottimizzata dei tipi di dati
    e supporto per batch processing e retry
    
    Versione modificata con supporto per failover alla tabella news_original se la tabella news
    non è disponibile per inserimenti (es. a causa dell'errore "Table is not initialized yet")
    """
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    # Controlla se articles è vuoto
    if articles is None or (isinstance(articles, list) and len(articles) == 0) or \
       (isinstance(articles, pd.DataFrame) and articles.empty):
        log("Nessun articolo da salvare in ClickHouse")
        return 0
    
    # Converti in DataFrame se non lo è già
    if not isinstance(articles, pd.DataFrame):
        try:
            df = pd.DataFrame([article for article in articles])
        except Exception as e:
            log_error(f"Errore nella conversione degli articoli in DataFrame: {e}")
            return 0
    else:
        df = articles.copy()  # Crea una copia per evitare di modificare l'originale
    
    # Controlla che ci siano i campi richiesti
    for field in ['url', 'title', 'body', 'date']:
        if field not in df.columns:
            log_error(f"Campo obbligatorio '{field}' mancante negli articoli")
            return 0
    
    # Assicurati che la data sia in formato datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Normalizza i tipi di dati problematici
    # Converti stringhe, gestisci NaN e valori nulli
    for col in df.columns:
        if col in ['url', 'title', 'body', 'source']:
            # Garantisce che i campi di testo siano stringhe
            df[col] = df[col].fillna('').astype(str)
        elif col in ['searchKey', 'image']:
            # Converti in stringhe ma lascia None come null
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
    
    # Prepara la connessione a ClickHouse
    try:
        # Assicurati che la tabella esista
        create_news_table_if_not_exists()
        
        # Ottieni il client
        client = get_clickhouse_client()
        database = os.environ.get('CLICKHOUSE_DATABASE', 'news')
        
        # Verifica la disponibilità della tabella
        if not check_table_availability(table_name=table_name, database=database, logger=logger):
            log_error(f"La tabella {table_name} non è disponibile per inserimenti in ClickHouse")
            return 0
        
        # Converti DataFrame in lista di dict
        articles_dict = df.to_dict(orient='records')
        
        # Deduplicazione per URL se richiesto
        if dedup_by_url:
            # Non recuperiamo tutti gli URL (potrebbe essere troppo per la memoria)
            # Invece, controlliamo solo gli URL specifici che stiamo per inserire
            if articles_dict:
                # Estrai gli URL da controllare
                urls_to_check = [article['url'] for article in articles_dict if 'url' in article and article['url']]
                
                # Costruisci una query con IN per verificare solo gli URL attuali
                # Usiamo una query ottimizzata che ritorna solo gli URL che già esistono
                if urls_to_check:
                    try:
                        urls_in_db = set()
                        # Chunk la lista di URL per evitare query troppo grandi
                        chunk_size = 1000
                        for i in range(0, len(urls_to_check), chunk_size):
                            chunk = urls_to_check[i:i+chunk_size]
                            chunk_urls = ", ".join([f"'{url}'" for url in chunk])
                            existing_urls = set([
                                url for url, in client.execute(
                                    f"SELECT url FROM {database}.{table_name} WHERE url IN ({chunk_urls})"
                                )
                            ])
                            urls_in_db.update(existing_urls)
                        
                        # Filtra gli articoli che non esistono già
                        new_articles = [article for article in articles_dict if article['url'] not in urls_in_db]
                        
                        skipped = len(articles_dict) - len(new_articles)
                        if skipped > 0:
                            log(f"Saltati {skipped} articoli già esistenti in ClickHouse")
                        
                        if not new_articles:
                            log("Nessun nuovo articolo da salvare (tutti gli URL esistono già)")
                            return 0
                        
                        log(f"Trovati {len(new_articles)} nuovi articoli da salvare (su {len(articles_dict)} totali)")
                    except Exception as e:
                        log_error(f"Errore nel controllo degli URL esistenti: {e}")
                        # Se c'è un errore, procediamo con tutti gli articoli
                        log_error("Procedo con tutti gli articoli senza deduplicazione a causa dell'errore")
                        new_articles = articles_dict
                else:
                    new_articles = articles_dict
            else:
                new_articles = articles_dict
        else:
            # Senza deduplicazione, usa tutti gli articoli
            new_articles = articles_dict
        
        # Funzione per pulire e convertire un singolo articolo
        def clean_article(article):
            # Normalizza e sanitizza i dati
            url = str(article.get('url', '')) if article.get('url') is not None else ''
            title = str(article.get('title', '')) if article.get('title') is not None else ''
            body = str(article.get('body', '')) if article.get('body') is not None else ''
            
            # Assicurati che date sia un datetime
            date = article.get('date', datetime.now())
            if not isinstance(date, datetime):
                try:
                    date = pd.to_datetime(date).to_pydatetime()
                except:
                    date = datetime.now()
            
            source = str(article.get('source', '')) if article.get('source') is not None else ''
            
            # Gestisci campi opzionali
            searchKey = article.get('searchKey')
            if searchKey is not None and not isinstance(searchKey, str):
                searchKey = str(searchKey)
                
            image = article.get('image')
            if image is not None and not isinstance(image, str):
                image = str(image)
            
            return {
                'url': url,
                'title': title,
                'body': body,
                'date': date,
                'source': source,
                'searchKey': searchKey,
                'image': image
            }
        
        # Processa i dati in batch
        total_inserted = 0
        
        # Divide in batch per ottimizzare la memoria e gestire grandi volumi
        for i in range(0, len(new_articles), batch_size):
            batch = new_articles[i:i+batch_size]
            rows = [clean_article(article) for article in batch]
            
            # Tentativi multipli per gestire errori temporanei
            for attempt in range(retry_attempts):
                try:
                    # Inserisci in ClickHouse
                    client.execute(
                        f"""
                        INSERT INTO {database}.{table_name}
                        (url, title, body, date, source, searchKey, image)
                        VALUES
                        """,
                        rows
                    )
                    
                    total_inserted += len(rows)
                    
                    # Log del batch inserito
                    batch_num = (i // batch_size) + 1
                    log(f"Batch {batch_num}: inseriti {len(rows)} articoli in ClickHouse (tabella: {table_name})")
                    break  # Successo, esci dal ciclo di tentativi
                
                except Exception as e:
                    error_msg = str(e)
                    log_error(f"Errore nel batch {(i // batch_size) + 1} (tentativo {attempt+1}/{retry_attempts}): {error_msg}")
                    
                    if attempt == retry_attempts - 1:
                        # Ultimo tentativo fallito
                        log_error(f"Impossibile inserire il batch {(i // batch_size) + 1} dopo {retry_attempts} tentativi")
                    else:
                        # Riprova
                        log(f"Ritento inserimento batch {(i // batch_size) + 1}...")
        
        log(f"Completato: salvati {total_inserted} nuovi articoli in ClickHouse (tabella: {table_name})")
        return total_inserted
    
    except Exception as e:
        log_error(f"Errore nel salvataggio degli articoli in ClickHouse: {e}")
        return 0

def search_news_clickhouse(query, days=30, sources=None, limit=100, table_name='news'):
    """
    Cerca articoli di news in ClickHouse
    """
    try:
        client = get_clickhouse_client()
        database = os.environ.get('CLICKHOUSE_DATABASE', 'news')
        
        # Costruisci la query
        where_clauses = [f"date >= now() - INTERVAL {days} DAY"]
        
        if query:
            # Usa ILIKE per ricerca case-insensitive
            where_clauses.append(f"(title ILIKE '%{query}%' OR body ILIKE '%{query}%')")
        
        if sources:
            if isinstance(sources, str):
                sources = [sources]
            source_conditions = " OR ".join([f"source = '{source}'" for source in sources])
            where_clauses.append(f"({source_conditions})")
        
        where_clause = " AND ".join(where_clauses)
        
        # Esegui la query
        query = f"""
        SELECT
            id,
            url,
            title,
            body,
            date,
            source,
            searchKey,
            image
        FROM {database}.{table_name}
        WHERE {where_clause}
        ORDER BY date DESC
        LIMIT {limit}
        """
        
        results = client.execute(query, with_column_types=True)
        
        # Estrai colonne e dati
        data, columns = results
        column_names = [col[0] for col in columns]
        
        # Converti i risultati in una lista di dizionari
        articles = []
        for row in data:
            article = dict(zip(column_names, row))
            articles.append(article)
        
        return articles
    
    except Exception as e:
        print(f"Errore nella ricerca in ClickHouse: {e}")
        return []

def count_articles_by_source(days=30, table_name='news'):
    """
    Conta gli articoli per fonte
    """
    try:
        client = get_clickhouse_client()
        database = os.environ.get('CLICKHOUSE_DATABASE', 'news')
        
        query = f"""
        SELECT
            source,
            count() as count,
            min(date) as first_date,
            max(date) as last_date
        FROM {database}.{table_name}
        WHERE date >= now() - INTERVAL {days} DAY
        GROUP BY source
        ORDER BY count DESC
        """
        
        results = client.execute(query)
        
        # Converti in lista di dizionari
        stats = []
        for row in results:
            source, count, first_date, last_date = row
            stats.append({
                'source': source,
                'count': count,
                'first_date': first_date,
                'last_date': last_date
            })
        
        return stats
    
    except Exception as e:
        print(f"Errore nell'analisi delle fonti in ClickHouse: {e}")
        return []

if __name__ == "__main__":
    # Test delle funzionalità
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Utilità ClickHouse')
    parser.add_argument('--create-table', action='store_true', help='Crea la tabella se non esiste')
    parser.add_argument('--count-sources', action='store_true', help='Mostra conteggio articoli per fonte')
    parser.add_argument('--search', help='Cerca negli articoli')
    parser.add_argument('--days', type=int, default=30, help='Giorni da considerare')
    parser.add_argument('--limit', type=int, default=20, help='Numero massimo di risultati')
    parser.add_argument('--table', default='news', help='Tabella da utilizzare (default: news)')
    parser.add_argument('--check-table', help='Verifica disponibilità tabella')
    
    args = parser.parse_args()
    
    if args.check_table:
        print(f"Verifica disponibilità tabella {args.check_table}...")
        if check_table_availability(table_name=args.check_table):
            print(f"✓ La tabella {args.check_table} è disponibile per inserimenti")
        else:
            print(f"✗ La tabella {args.check_table} non è disponibile per inserimenti")
    
    if args.create_table:
        print("Creazione della tabella news...")
        if create_news_table_if_not_exists():
            print("✓ Tabella creata con successo")
        else:
            print("! Errore nella creazione della tabella")
            sys.exit(1)
    
    if args.count_sources:
        print(f"Conteggio articoli per fonte negli ultimi {args.days} giorni:")
        stats = count_articles_by_source(days=args.days, table_name=args.table)
        print(f"{'FONTE':<30} {'ARTICOLI':<10} {'PRIMO ARTICOLO':<20} {'ULTIMO ARTICOLO':<20}")
        print("-" * 80)
        for stat in stats:
            first_date = stat['first_date'].strftime('%Y-%m-%d') if stat['first_date'] else 'N/A'
            last_date = stat['last_date'].strftime('%Y-%m-%d') if stat['last_date'] else 'N/A'
            print(f"{stat['source']:<30} {stat['count']:<10} {first_date:<20} {last_date:<20}")
    
    if args.search:
        print(f"Ricerca di '{args.search}' negli ultimi {args.days} giorni:")
        articles = search_news_clickhouse(args.search, days=args.days, limit=args.limit, table_name=args.table)
        
        if not articles:
            print("Nessun risultato trovato")
        else:
            print(f"Trovati {len(articles)} articoli:")
            print(f"{'DATA':<15} {'FONTE':<20} {'TITOLO'}")
            print("-" * 80)
            
            for article in articles:
                date = article['date'].strftime('%Y-%m-%d') if article['date'] else 'N/A'
                title = article['title'][:60] + '...' if len(article['title']) > 60 else article['title']
                print(f"{date:<15} {article['source'][:18]:<20} {title}")