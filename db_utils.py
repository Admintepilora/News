#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Common database utilities for news scrapers
Supporta sia MongoDB che ClickHouse
"""
import os
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pandas as pd

# Importa le utilità per ClickHouse
try:
    from clickhouse_utils import save_articles_to_clickhouse
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False

def get_mongo_client(host='localhost', port=27017, username='newsadmin', password='newspassword'):
    """Create and return MongoDB client"""
    return MongoClient(host, port, username=username, password=password)

def save_articles_to_db(articles, collection_name='News', logger=None):
    """Save news articles to MongoDB with deduplication by URL"""
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    # Check if articles is empty list, None, or empty DataFrame
    if articles is None or (isinstance(articles, list) and len(articles) == 0) or \
       (isinstance(articles, pd.DataFrame) and articles.empty):
        log("No articles to save")
        return 0
    
    # Convert to DataFrame for easier processing if not already
    if not isinstance(articles, pd.DataFrame):
        try:
            df = pd.DataFrame([article for article in articles])
        except Exception as e:
            log_error(f"Error converting articles to DataFrame: {e}")
            return 0
    else:
        df = articles.copy()  # Create a copy to avoid modifying the original
    
    # Make sure required fields exist
    for field in ['url', 'title', 'body', 'date']:
        if field not in df.columns:
            log_error(f"Required field '{field}' missing from articles")
            return 0
    
    # Make sure date is datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Convert to dict for MongoDB
    articles_dict = df.to_dict(orient='records')
    
    # Create bulk operations
    bulk_operations = [
        UpdateOne(
            {'url': article['url']},  # filter criteria
            {'$set': article},        # update operation
            upsert=True
        ) 
        for article in articles_dict
    ]
    
    # Save to MongoDB
    client = get_mongo_client()
    db = client['News']
    collection = db[collection_name]
    
    # Avoid "if bulk_operations" which triggers DataFrame truth value error
    if len(bulk_operations) > 0:
        result = collection.bulk_write(bulk_operations)
        client.close()
        return result.upserted_count + result.modified_count
    else:
        client.close()
        return 0

def ensure_date_format():
    """Fix any string dates in the collection by converting to datetime"""
    client = get_mongo_client()
    db = client['News']
    collection = db['News']
    
    result = collection.update_many(
        {"date": {"$type": "string"}},  # Match documents where 'date' is a string
        [{"$set": {"date": {"$toDate": "$date"}}}]  # Use MongoDB's $toDate operator
    )
    
    client.close()
    return result.modified_count

def check_urls_exist(urls_to_check, mongodb=True, clickhouse=True, mongodb_collection='News', 
                     clickhouse_table='news', logger=None):
    """
    Controlla se specifici URL esistono già nei database
    
    Parametri:
    - urls_to_check: lista degli URL da controllare
    - mongodb: boolean, se True controlla MongoDB
    - clickhouse: boolean, se True controlla ClickHouse
    - mongodb_collection: nome collezione MongoDB
    - clickhouse_table: nome tabella ClickHouse
    - logger: oggetto logger opzionale
    
    Restituisce:
    - set di URL che esistono già
    """
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    if not urls_to_check:
        return set()
    
    existing_urls = set()
    
    # Controlla URL in MongoDB
    if mongodb:
        try:
            client = get_mongo_client()
            db = client['News']
            collection = db[mongodb_collection]
            
            # Usa $in per controllare solo gli URL specifici
            mongo_existing = collection.find(
                {"url": {"$in": urls_to_check}}, 
                {"url": 1, "_id": 0}
            )
            
            mongo_urls = set([doc["url"] for doc in mongo_existing if "url" in doc])
            existing_urls.update(mongo_urls)
            client.close()
            
            log(f"Trovati {len(mongo_urls)} URL esistenti in MongoDB")
        except Exception as e:
            log_error(f"Errore nel controllo URL MongoDB: {e}")
    
    # Controlla URL in ClickHouse
    if clickhouse and CLICKHOUSE_AVAILABLE:
        try:
            use_ch = os.environ.get('USE_CLICKHOUSE', 'true').lower() == 'true'
            if use_ch:
                from clickhouse_utils import get_clickhouse_client
                client = get_clickhouse_client()
                database = os.environ.get('CLICKHOUSE_DATABASE', 'news')
                
                # Chunk gli URL per evitare query troppo grandi
                chunk_size = 1000
                ch_urls = set()
                
                for i in range(0, len(urls_to_check), chunk_size):
                    chunk = urls_to_check[i:i+chunk_size]
                    # Escape delle quote negli URL
                    escaped_urls = [url.replace("'", "\\'") for url in chunk]
                    url_list = "', '".join(escaped_urls)
                    
                    existing_chunk = client.execute(
                        f"SELECT url FROM {database}.{clickhouse_table} WHERE url IN ('{url_list}')"
                    )
                    ch_urls.update([url for url, in existing_chunk])
                
                existing_urls.update(ch_urls)
                log(f"Trovati {len(ch_urls)} URL esistenti in ClickHouse")
        except Exception as e:
            log_error(f"Errore nel controllo URL ClickHouse: {e}")
    
    log(f"Totale: {len(existing_urls)} URL già esistenti sui {len(urls_to_check)} controllati")
    return existing_urls

def get_existing_urls(mongodb=True, clickhouse=True, mongodb_collection='News', 
                     clickhouse_table='news', max_urls=100000, logger=None):
    """
    DEPRECATA: Usa check_urls_exist per controlli più efficienti
    """
    log = logger.warning if logger else print
    log("get_existing_urls è deprecata, usa check_urls_exist per prestazioni migliori")
    return set()

# Funzione per salvare gli articoli in entrambi i database (MongoDB e ClickHouse)
def save_articles_to_all_dbs(articles, mongodb_collection='News', clickhouse_table='news', 
                           use_mongodb=True, use_clickhouse=True, logger=None,
                           check_across_dbs=True, max_urls=100000):
    """
    Salva gli articoli su tutti i database configurati con gestione degli errori
    e deduplicazione incrociata tra i database
    
    Parametri:
    - articles: articoli da salvare (DataFrame o lista di dizionari)
    - mongodb_collection: nome collezione MongoDB
    - clickhouse_table: nome tabella ClickHouse
    - use_mongodb: boolean, se True salva su MongoDB
    - use_clickhouse: boolean, se True salva su ClickHouse
    - logger: oggetto logger opzionale
    - check_across_dbs: boolean, se True controlla URL esistenti in entrambi i DB
    - max_urls: numero massimo di URL da recuperare (per evitare problemi di memoria)
    """
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    # Converti in DataFrame se non lo è già
    if not isinstance(articles, pd.DataFrame):
        try:
            df = pd.DataFrame([article for article in articles])
        except Exception as e:
            log_error(f"Errore nella conversione degli articoli in DataFrame: {e}")
            return {'mongodb': 0, 'clickhouse': 0, 'total': 0}
    else:
        df = articles.copy()  # Create a copy to avoid modifying the original
    
    # Inizializza contatori
    mongo_count = 0
    clickhouse_count = 0
    
    # Se non ci sono articoli, esci
    if df.empty:
        log("Nessun articolo da salvare")
        return {'mongodb': 0, 'clickhouse': 0, 'total': 0}
    
    # Deduplicazione incrociata tra database
    if check_across_dbs and 'url' in df.columns:
        # Controlla solo gli URL degli articoli che stiamo per inserire
        urls_to_check = df['url'].tolist()
        existing_urls = check_urls_exist(
            urls_to_check=urls_to_check,
            mongodb=use_mongodb, 
            clickhouse=use_clickhouse,
            mongodb_collection=mongodb_collection,
            clickhouse_table=clickhouse_table,
            logger=logger
        )
        
        # Filtra articoli non esistenti in entrambi i database
        if existing_urls:
            original_count = len(df)
            df = df[~df['url'].isin(existing_urls)]
            skipped = original_count - len(df)
            if skipped > 0:
                log(f"Saltati {skipped} articoli già esistenti nei database")
            
            # Se non ci sono nuovi articoli, esci
            if df.empty:
                log("Nessun nuovo articolo da salvare")
                return {'mongodb': 0, 'clickhouse': 0, 'total': 0}
    
    # Salva su MongoDB se configurato
    if use_mongodb:
        try:
            # Converti in lista di dizionari per MongoDB
            mongo_articles = df.to_dict(orient='records') if not df.empty else []
            if mongo_articles:
                mongo_count = save_articles_to_db(mongo_articles, collection_name=mongodb_collection, logger=logger)
                log(f"Salvati {mongo_count} articoli su MongoDB")
        except Exception as e:
            log_error(f"Errore nel salvataggio su MongoDB: {e}")
    
    # Salva su ClickHouse se disponibile e configurato
    if use_clickhouse and CLICKHOUSE_AVAILABLE:
        try:
            # Leggi la configurazione dalle variabili d'ambiente
            use_ch = os.environ.get('USE_CLICKHOUSE', 'true').lower() == 'true'
            if use_ch and not df.empty:
                # Non fare deduplicazione in ClickHouse se l'abbiamo già fatta
                dedup_in_clickhouse = not check_across_dbs
                clickhouse_count = save_articles_to_clickhouse(
                    df, 
                    table_name=clickhouse_table, 
                    dedup_by_url=dedup_in_clickhouse,
                    logger=logger
                )
                log(f"Salvati {clickhouse_count} articoli su ClickHouse")
        except Exception as e:
            log_error(f"Errore nel salvataggio su ClickHouse: {e}")
    
    # Restituisci riassunto delle operazioni
    return {
        'mongodb': mongo_count,
        'clickhouse': clickhouse_count,
        'total': mongo_count + clickhouse_count
    }

if __name__ == "__main__":
    # Test delle funzionalità
    import argparse
    
    parser = argparse.ArgumentParser(description='Utilities per database news')
    parser.add_argument('--fix-dates', action='store_true', help='Correggi il formato delle date in MongoDB')
    parser.add_argument('--test-clickhouse', action='store_true', help='Testa la connessione a ClickHouse')
    parser.add_argument('--check-urls', action='store_true', help='Controlla gli URL esistenti in entrambi i DB')
    
    args = parser.parse_args()
    
    if args.fix_dates:
        # Test the date format conversion
        fixed_count = ensure_date_format()
        print(f"Corretti {fixed_count} campi data nella collezione News di MongoDB")
    
    if args.test_clickhouse and CLICKHOUSE_AVAILABLE:
        from clickhouse_utils import get_clickhouse_client
        print("Test della connessione a ClickHouse...")
        try:
            client = get_clickhouse_client()
            result = client.execute("SELECT version()")
            version = result[0][0]
            print(f"✓ Connessione a ClickHouse riuscita! Versione: {version}")
        except Exception as e:
            print(f"! Errore di connessione a ClickHouse: {e}")
    
    if args.check_urls:
        # Test recupero URL
        print("Controllo URL esistenti...")
        urls = get_existing_urls(mongodb=True, clickhouse=True)
        print(f"Trovati {len(urls)} URL univoci esistenti")
        print(f"Esempi: {list(urls)[:5]}")